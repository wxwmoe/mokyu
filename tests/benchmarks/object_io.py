"""Bounded, operation-specific object IO benchmark; write verification is untimed."""
import argparse
import json
import os
import random
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from tests.benchmarks.public_reads import cpu, status
from tests.benchmarks.source_writes import percentiles, sql_changes, sql_snapshot


def memory(path):
    values = {key: int(value) for key, value in
              (line.split() for line in (path / 'memory.stat').read_text().splitlines())}
    return {key: values[key] for key in ('anon', 'file', 'file_dirty', 'file_writeback')}


def disk(path):
    return {fields[0]: {key: int(value) for key, value in (item.split('=') for item in fields[1:])}
            for fields in (line.split() for line in (path / 'io.stat').read_text().splitlines())}


def readback(client, bucket, key, expected):
    start = time.perf_counter()
    reply = client.get_object(Bucket=bucket, Key=key)
    with reply['Body'] as body:
        first = body.read(1)
        first_ms = (time.perf_counter() - start) * 1000
        assert first + body.read() == expected, 'readback body mismatch'
    assert reply['ContentLength'] == len(expected), 'readback length mismatch'
    return first_ms


def request(client, bucket, key, body, operation):
    stages = {}

    def call(name, function, **arguments):
        start = time.perf_counter()
        reply = function(**arguments)
        stages[name] = (time.perf_counter() - start) * 1000
        return reply

    start = time.perf_counter()
    if operation == 'put':
        call('put', client.put_object, Bucket=bucket, Key=key, Body=body)
    elif operation == 'multipart':
        upload = call('create', client.create_multipart_upload, Bucket=bucket, Key=key)['UploadId']
        try:
            part = call('part', client.upload_part, Bucket=bucket, Key=key,
                        UploadId=upload, PartNumber=1, Body=body)
            call('complete', client.complete_multipart_upload, Bucket=bucket, Key=key,
                 UploadId=upload, MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
        except Exception:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
            raise
    elif operation == 'get':
        stages['first_byte'] = readback(client, bucket, key, body)
    elif operation == 'head':
        reply = call('head', client.head_object, Bucket=bucket, Key=key)
        assert reply['ContentLength'] == len(body), 'HEAD length mismatch'
    else:
        raise ValueError('unknown operation')
    return (time.perf_counter() - start) * 1000, stages


def payload(base, sequence):
    if not base:
        return b''
    suffix = min(16, len(base))
    return base[:-suffix] + sequence.to_bytes(16, 'big')[-suffix:]


def drain_raw(database, bucket, seconds):
    started = time.perf_counter()
    first = None
    while True:
        current = database.query('''SELECT count(*) AS pending,
            COALESCE(sum(CASE WHEN w.last_error IS NOT NULL THEN 1 ELSE 0 END),0) AS failed
            FROM raw_processing w JOIN streams s ON s.id=w.stream_id
            JOIN buckets b ON b.id=s.bucket_id WHERE b.name=%s''', bucket)[0]
        first = first or current
        elapsed = time.perf_counter() - started
        if current['pending'] == 0 or current['failed'] or elapsed >= seconds:
            return dict(seconds=elapsed, before=first, after=current, completed=current['pending'] == 0)
        time.sleep(.05)


def measure(fixture, credentials, database=None):
    operation = fixture['operation']
    seconds = float(fixture.get('seconds', 30))
    maximum = int(fixture['max_operations'])
    concurrency = int(fixture.get('concurrency', 4))
    size = int(fixture['size'])
    assert operation in ('put', 'get', 'head', 'multipart')
    assert 1 <= seconds <= 600 and 1 <= concurrency <= 64 and 1 <= maximum <= 1000000
    assert 0 <= size <= 64 * 1024 * 1024
    assert (maximum + 1) * size <= int(fixture['max_total_bytes']), 'workload exceeds its byte budget'
    raw_drain_seconds = int(fixture.get('raw_drain_seconds', 0))
    assert 0 <= raw_drain_seconds <= 600
    raw_database = None
    if raw_drain_seconds:
        assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
        from tests.recovery.source_repair import Database
        raw_database = Database()
    base = random.Random(fixture['seed']).randbytes(size)
    prefix = fixture['prefix'].rstrip('/')
    assert prefix, 'an isolated nonempty prefix is required'
    bucket = credentials['bucket']
    clients = [boto3.client('s3', endpoint_url=fixture['endpoint'], region_name='us-east-1',
        aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
        config=Config(s3={'addressing_style': 'path'}, max_pool_connections=2, connect_timeout=5,
            read_timeout=30, retries={'max_attempts': 0}, request_checksum_calculation='when_required',
            response_checksum_validation='when_required')) for _ in range(concurrency)]
    resources = {name: Path(value['cgroup']) for name, value in fixture.get('resources', {}).items()}
    statuses = lambda: {name: status(value['status_url']) for name, value in fixture.get('resources', {}).items()
                       if value.get('status_url')}
    stop = threading.Event()
    lock = threading.Lock()
    attempted = 0
    errors = Counter()
    samples = []
    committed = []
    sampling_errors = []
    peak = {}
    sampler = None
    try:
        source = prefix + '/source'
        clients[0].put_object(Bucket=bucket, Key=source, Body=base)
        readback(clients[0], bucket, source, base)
        if raw_database:
            assert drain_raw(raw_database, bucket, raw_drain_seconds)['completed'], 'warmup raw processing did not drain'
        before_status = statuses()
        before_sql = sql_snapshot(database)
        before_cpu = {name: cpu(path) for name, path in resources.items()}
        before_disk = {name: disk(path) for name, path in resources.items()}
        peak = {name: int((path / 'memory.current').read_text()) for name, path in resources.items()}
        memory_peak = {name: memory(path) for name, path in resources.items()}

        def sample():
            try:
                while not stop.wait(.1):
                    for name, path in resources.items():
                        peak[name] = max(peak[name], int((path / 'memory.current').read_text()))
                        for field, value in memory(path).items():
                            memory_peak[name][field] = max(memory_peak[name][field], value)
            except Exception as error:
                sampling_errors.append(type(error).__name__)

        def worker(index):
            nonlocal attempted
            while time.perf_counter() < deadline:
                with lock:
                    if attempted >= maximum:
                        break
                    sequence = attempted
                    attempted += 1
                writing = operation in ('put', 'multipart')
                body = payload(base, sequence) if writing else base
                key = f'{prefix}/{sequence}' if writing else source
                try:
                    timing = request(clients[index], bucket, key, body, operation)
                    with lock:
                        samples.append(timing)
                        if writing:
                            committed.append(sequence)
                except Exception as error:
                    code = (getattr(error, 'response', None) or {}).get('Error', {}).get('Code')
                    with lock:
                        errors[code or type(error).__name__] += 1

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        started = time.perf_counter()
        client_cpu = time.process_time()
        deadline = started + seconds
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(worker, range(concurrency)))
        elapsed = time.perf_counter() - started
        client_cpu = time.process_time() - client_cpu
        stop.set()
        sampler.join(timeout=5)
        assert not sampler.is_alive(), 'sampler did not stop'
        after_cpu = {name: cpu(path) for name, path in resources.items()}
        after_disk = {name: disk(path) for name, path in resources.items()}
        after_sql = sql_snapshot(database)
        after_status = statuses()
        drained = None
        if raw_database:
            drained = drain_raw(raw_database, bucket, raw_drain_seconds)
            drained['total_seconds'] = time.perf_counter() - started
            drained['completed_operations_per_second'] = len(samples) / drained['total_seconds']
            drained['resources'] = {name: dict(
                cpu_delta={key: value - before_cpu[name][key] for key, value in cpu(path).items()},
                io_delta={device: {field: value - before_disk[name].get(device, {}).get(field, 0)
                    for field, value in values.items()} for device, values in disk(path).items()},
                memory_current=int((path / 'memory.current').read_text())) for name, path in resources.items()}
            if not drained['completed']:
                errors['raw_processing:incomplete'] += 1
        verification_started = time.perf_counter()
        verified = 0
        for sequence in committed:
            try:
                readback(clients[0], bucket, f'{prefix}/{sequence}', payload(base, sequence))
                verified += 1
            except Exception as error:
                errors['verification:' + type(error).__name__] += 1
        for error in sampling_errors:
            errors['sampling:' + error] += 1
        names = sorted({name for _, stages in samples for name in stages})
        return dict(operation=operation, attempts=attempted, acknowledged=len(samples), errors=dict(errors),
            seconds=elapsed, operations_per_second=len(samples) / elapsed,
            client_process_cpu_seconds=client_cpu,
            raw_processing=drained,
            latency=percentiles([sample[0] for sample in samples]),
            stages={name: percentiles([stage[name] for _, stage in samples if name in stage]) for name in names},
            verification=dict(objects=verified, expected=len(committed),
                seconds=time.perf_counter() - verification_started, inside_timing=operation == 'get'),
            timing='PUT ACK; GET full verified body; HEAD metadata; multipart create+part+complete',
            status_before=before_status, status_after=after_status,
            sql_delta=sql_changes(before_sql, after_sql), sql_scope='measurement window, including background work',
            resources={name: dict(cpu_delta={key: value - before_cpu[name][key] for key, value in after_cpu[name].items()},
                sampled_memory_current_max=peak[name], cpu_max=(path / 'cpu.max').read_text().strip(),
                sampled_memory_stat_max=memory_peak[name], memory_after=memory(path),
                io_delta={device: {field: value - before_disk[name].get(device, {}).get(field, 0)
                    for field, value in values.items()} for device, values in after_disk[name].items()},
                memory_max=(path / 'memory.max').read_text().strip(),
                cpuset=(path / 'cpuset.cpus.effective').read_text().strip()) for name, path in resources.items()})
    finally:
        stop.set()
        if sampler:
            sampler.join(timeout=5)
        for client in clients:
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('fixture', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    fixture = json.loads(args.fixture.read_text())
    assert fixture['build'] == 'release' and fixture['revision'] and fixture['cache_state']
    credentials = json.loads(Path(fixture['credentials_file']).read_text())
    database = None
    if fixture.get('database_file'):
        import psycopg
        database = psycopg.connect(Path(fixture['database_file']).read_text().strip(), autocommit=True)
    try:
        started_at = datetime.now(timezone.utc).isoformat()
        result = measure(fixture, credentials, database)
        report = dict(started_at=started_at,
            provenance={key: fixture[key] for key in ('revision', 'build', 'cache_state', 'seed', 'database_version')},
            size=fixture['size'], concurrency=fixture.get('concurrency', 4), result=result)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps({key: result[key] for key in ('operation', 'acknowledged', 'errors', 'seconds', 'operations_per_second', 'latency')}), flush=True)
        if result['errors']:
            raise SystemExit(1)
    finally:
        if database:
            database.close()


if __name__ == '__main__':
    main()
