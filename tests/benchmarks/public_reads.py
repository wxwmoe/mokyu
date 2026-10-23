"""Bounded release public-read benchmark. See tests/README.md for the fixture contract."""
import argparse
import ctypes
import gzip
import hashlib
import json
import math
import mmap
import os
import platform
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests


def cpu(path):
    return {key: int(value) for key, value in (line.split() for line in (path / 'cpu.stat').read_text().splitlines())}


def memory(path):
    values = {key: int(value) for key, value in
              (line.split() for line in (path / 'memory.stat').read_text().splitlines())}
    return {key: values[key] for key in ('anon', 'file', 'file_dirty', 'file_writeback')}


def disk(path):
    return {fields[0]: {key: int(value) for key, value in (item.split('=') for item in fields[1:])}
            for fields in (line.split() for line in (path / 'io.stat').read_text().splitlines())}


def evict_pages(paths):
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte)]
    for path in paths:
        with open(path, 'rb') as file:
            size = os.fstat(file.fileno()).st_size
            if not size:
                continue
            deadline = time.monotonic() + 1
            while True:
                os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_COPY) as mapped:
                    pages = (ctypes.c_ubyte * math.ceil(size / mmap.PAGESIZE))()
                    address = ctypes.addressof(ctypes.c_char.from_buffer(mapped))
                    if libc.mincore(address, size, pages) != 0:
                        raise OSError(ctypes.get_errno(), 'mincore failed')
                    if not any(page & 1 for page in pages):
                        break
                # sendfile pages may remain pinned until the preceding TCP transfer is acknowledged.
                assert time.monotonic() < deadline, 'file pages remain resident after local eviction'
                time.sleep(.01)


def status(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    value = response.json()
    fields = ['version', 'role', 'resources', 'backend_gets', 'backend_puts', 'backend_read_bytes',
              'backend_write_bytes', 'cache_hits', 'cache_hit_bytes', 'response_cache',
              'public_read_cache', 'cors_cache', 'file_delivery', 'read_checks',
              'credential_activity', 'upload_cache', 'io',
              'read_replica', 'process_memory', 'db_pool_size', 'db_pool_idle']
    return {key: value[key] for key in fields if key in value}


def sql_counts(connection):
    if connection is None:
        return None
    prefixes = {'source_pages': 'SELECT e.*,c.id', 'lease_insert': 'INSERT INTO stream_leases'}
    rows = connection.execute('SELECT query,calls FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())').fetchall()
    counts = {name: sum(calls for query, calls in rows if query.startswith(prefix)) for name, prefix in prefixes.items()}
    counts['snapshot'] = sum(calls for query, calls in rows if query.startswith('SELECT s.') and ' AS cache_offset' in query)
    return counts


def decode(data, encoding):
    if encoding == 'identity':
        return data
    if encoding == 'gzip':
        return gzip.decompress(data)
    if encoding == 'br':
        import brotli
        return brotli.decompress(data)
    if encoding == 'zstd':
        import zstandard
        with zstandard.ZstdDecompressor().stream_reader(data) as reader:
            return reader.read()
    raise AssertionError('unexpected content encoding: ' + encoding)


def measure(fixture, case, database):
    expected = Path(case['expected_file']).read_bytes()
    concurrency, count = int(fixture.get('concurrency', 4)), int(fixture.get('requests', 1000))
    warmup = int(fixture.get('warmup', 4))
    seconds = float(fixture.get('seconds', 0))
    assert 1 <= concurrency <= 64 and 1 <= count <= 1_000_000 and 0 <= warmup <= 100
    assert seconds == 0 or 1 <= seconds <= 600
    urls = case['urls']
    assert 1 <= len(urls) <= 4096
    headers = {'Accept-Encoding': case['encoding'], **case.get('headers', {})}
    method = case.get('method', 'GET')
    assert method in ('GET', 'HEAD', 'OPTIONS')
    cold_files = case.get('evict_files', [])
    assert not cold_files or concurrency == 1, 'cold page-cache reads require one worker'

    def read(session, url):
        if cold_files:
            evict_pages(cold_files)
        start = time.perf_counter()
        with session.request(method, url, headers=headers, timeout=(5, 30), stream=True) as response:
            response.raise_for_status()
            assert response.status_code == case.get('status', 200), 'unexpected success status'
            first = response.raw.read(1, decode_content=False)
            ttfb = time.perf_counter() - start
            wire = first + response.raw.read(decode_content=False)
            elapsed = time.perf_counter() - start
            encoding = response.headers.get('Content-Encoding', 'identity')
            assert encoding == case['encoding'], f'expected {case["encoding"]}, got {encoding}'
            if method != 'HEAD' and 'Content-Length' in response.headers:
                assert int(response.headers['Content-Length']) == len(wire), 'wire length mismatch'
            if method == 'GET':
                assert decode(wire, encoding) == expected, 'body mismatch'
            else:
                assert wire == b'', 'unexpected response body'
            for name, value in case.get('response_headers', {}).items():
                assert response.headers.get(name) == value, 'header mismatch: ' + name
        return elapsed * 1000, ttfb * 1000, len(wire)

    # Finish warmup before starting any worker; a failure cannot strand a barrier.
    with requests.Session() as session:
        session.trust_env = False
        for _ in range(warmup):
            for url in urls:
                read(session, url)
    paths = {name: Path(resource['cgroup']) for name, resource in fixture['resources'].items()}
    memory_samples = {name: [int((path / 'memory.current').read_text())] for name, path in paths.items()}
    memory_before = {name: memory(path) for name, path in paths.items()}
    memory_peak = {name: dict(values) for name, values in memory_before.items()}
    io_before = {name: disk(path) for name, path in paths.items()}
    before_status = {name: status(resource['status_url']) for name, resource in fixture['resources'].items() if resource.get('status_url')}
    before_sql = sql_counts(database)
    stop = threading.Event()
    sampling_errors = []

    def sample():
        while not stop.wait(.25):
            try:
                for name, path in paths.items():
                    memory_samples[name].append(int((path / 'memory.current').read_text()))
                    for field, value in memory(path).items():
                        memory_peak[name][field] = max(memory_peak[name][field], value)
            except OSError as error:
                sampling_errors.append(type(error).__name__)
                return

    def worker(index):
        timings, errors, wire_bytes = [], Counter(), 0
        with requests.Session() as session:
            session.trust_env = False
            for request in range(index, count, concurrency):
                if seconds and time.perf_counter() >= deadline:
                    break
                try:
                    latency, first_byte, size = read(session, urls[request % len(urls)])
                    timings.append((latency, first_byte))
                    wire_bytes += size
                except requests.HTTPError as error:
                    errors['HTTP ' + str(error.response.status_code)] += 1
                except (requests.RequestException, AssertionError, OSError, ValueError) as error:
                    errors[type(error).__name__ + ': ' + str(error)[:160]] += 1
        return timings, errors, wire_bytes

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    before_cpu = {name: cpu(path) for name, path in paths.items()}
    start = time.perf_counter()
    deadline = start + seconds
    process_cpu = time.process_time()
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(worker, range(concurrency)))
    finally:
        elapsed = time.perf_counter() - start
        process_cpu = time.process_time() - process_cpu
        stop.set()
        sampler.join(timeout=5)
    after_cpu = {name: cpu(path) for name, path in paths.items()}
    after_sql = sql_counts(database)
    after_status = {name: status(resource['status_url']) for name, resource in fixture['resources'].items() if resource.get('status_url')}
    errors = sum((result[1] for result in results), Counter())
    for error in sampling_errors:
        errors['resource sampling: ' + error] += 1
    latencies = sorted(latency for result in results for latency, _ in result[0])
    first_bytes = sorted(first for result in results for _, first in result[0])

    def percentiles(values):
        return {f'p{percent}_ms': values[max(0, math.ceil(len(values) * percent / 100) - 1)] if values else None for percent in [50, 95, 99]}

    return {'name': case['name'], 'encoding': case['encoding'], 'decoded_size': len(expected),
            'file_pages_evicted_per_request': bool(cold_files),
            'sha256': hashlib.sha256(expected).hexdigest(), 'attempts': len(latencies) + sum(result[1].total() for result in results), 'successes': len(latencies),
            'seconds': elapsed, 'rps': len(latencies) / elapsed, 'latency': percentiles(latencies),
            'ttfb': percentiles(first_bytes), 'errors': dict(errors),
            'successful_wire_body_bytes': sum(result[2] for result in results),
            'client_process_cpu_seconds': process_cpu,
            'resources': {name: {'cpu_delta': {key: after_cpu[name][key] - before_cpu[name][key] for key in before_cpu[name]},
                                  'sampled_memory_current_max': max(memory_samples[name]), 'memory_samples': len(memory_samples[name]),
                                  'memory_after': int((path / 'memory.current').read_text()),
                                  'sampled_memory_stat_max': memory_peak[name], 'memory_stat_before': memory_before[name], 'memory_stat_after': memory(path),
                                  'io_delta': {device: {field: value - io_before[name].get(device, {}).get(field, 0)
                                      for field, value in values.items()} for device, values in disk(path).items()},
                                  'cpu_max': (path / 'cpu.max').read_text().strip(),
                                  'cpuset': (path / 'cpuset.cpus.effective').read_text().strip(),
                                  'memory_max': (path / 'memory.max').read_text().strip()}
                          for name, path in paths.items()},
            'status_before': before_status, 'status_after': after_status,
            'sql_delta': {key: after_sql[key] - before_sql[key] for key in before_sql} if before_sql else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('fixture', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    fixture = json.loads(args.fixture.read_text())
    assert fixture['build'] == 'release' and fixture['revision'] and fixture['cache_state']
    assert fixture['database_version'] and fixture['seed'] is not None and fixture['resources']
    database = None
    if fixture.get('database_file'):
        import psycopg
        database = psycopg.connect(Path(fixture['database_file']).read_text().strip(), autocommit=True)
    report = {'started_at': datetime.now(timezone.utc).isoformat(),
              'provenance': {key: fixture[key] for key in ['label', 'revision', 'build', 'cache_state', 'database_version', 'seed']},
              'client': {'python': platform.python_version(), 'requests': requests.__version__, 'platform': platform.platform()},
              'concurrency': fixture.get('concurrency', 4), 'warmup_per_url': fixture.get('warmup', 4), 'cases': []}
    try:
        for case in fixture['cases']:
            try:
                result = measure(fixture, case, database)
            except Exception as error:
                result = {'name': case['name'], 'errors': {type(error).__name__ + ': ' + str(error)[:160]: 1}}
            report['cases'].append(result)
            args.output.write_text(json.dumps(report, indent=2) + '\n')
            print(json.dumps(result), flush=True)
            if result['errors']:
                raise SystemExit(1)
    finally:
        if database is not None:
            database.close()


if __name__ == '__main__':
    main()
