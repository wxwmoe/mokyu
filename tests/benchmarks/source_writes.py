"""Bounded release write benchmark with full readback and resource/SQL evidence."""
import argparse
import json
import math
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
from botocore.exceptions import BotoCoreError, ClientError
from tests.benchmarks.public_reads import cpu, status


def percentiles(values):
    values = sorted(values)
    return {f'p{percent}_ms': values[math.ceil(len(values)*percent/100)-1] if values else None
            for percent in (50,95,99)}


def operation(client, bucket, key, source, body, mode):
    if mode == 'multipart':
        upload = client.create_multipart_upload(Bucket=bucket,Key=key)['UploadId']
        try:
            part = client.upload_part(Bucket=bucket,Key=key,UploadId=upload,PartNumber=1,Body=body)
            start = time.perf_counter()
            client.complete_multipart_upload(Bucket=bucket,Key=key,UploadId=upload,
                MultipartUpload={'Parts':[{'PartNumber':1,'ETag':part['ETag']}]})
        except Exception:
            client.abort_multipart_upload(Bucket=bucket,Key=key,UploadId=upload)
            raise
    else:
        start = time.perf_counter()
        if mode in ('copy','many'):
            client.copy_object(Bucket=bucket,Key=key,CopySource={'Bucket':bucket,'Key':source})
        else:
            client.put_object(Bucket=bucket,Key=key,Body=body)
    write_ms = (time.perf_counter()-start)*1000
    result = client.get_object(Bucket=bucket,Key=key)
    with result['Body'] as stream:
        assert stream.read() == body, 'readback body mismatch'
    assert result['ContentLength'] == len(body), 'readback length mismatch'
    return write_ms


SQL_ID = ('userid','dbid','queryid','toplevel')
SQL_METRICS = ('calls','total_exec_time','rows','plans','total_plan_time',
               'shared_blks_hit','shared_blks_read','shared_blks_dirtied','shared_blks_written',
               'temp_blks_read','temp_blks_written','wal_records','wal_fpi','wal_bytes')
BLOCKING_SQL = ("SELECT a.pid,b.pid,a.query_id,b.query_id,a.application_name,b.application_name,b.state "
    "FROM pg_stat_activity a CROSS JOIN LATERAL unnest(pg_blocking_pids(a.pid)) p(pid) "
    "JOIN pg_stat_activity b ON b.pid=p.pid WHERE a.datname=current_database() LIMIT 128")


def sql_snapshot(connection):
    if connection is None:
        return None
    columns = SQL_ID+SQL_METRICS+('stats_since','query')
    rows = connection.execute('SELECT '+','.join(columns)+" FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())").fetchall()
    result = {}
    for values in rows:
        row = dict(zip(columns,values))
        row['wal_bytes'] = int(row['wal_bytes'])
        result[tuple(row[key] for key in SQL_ID)] = row
    return result


def sql_changes(before, after):
    if before is None:
        return None
    result = []
    assert before.keys() <= after.keys(), 'SQL statistics evicted during measurement'
    for key,row in after.items():
        old = before.get(key,{})
        assert not old or row['stats_since']==old['stats_since'], 'SQL statistics reset during measurement'
        metrics = {name:row[name]-old.get(name,0) for name in SQL_METRICS}
        assert all(value >= 0 for value in metrics.values()), 'SQL statistics reset during measurement'
        if metrics['calls']:
            result.append(dict(**{name:row[name] for name in SQL_ID},query=row['query'],
                               **metrics,milliseconds=metrics['total_exec_time']))
    return sorted(result,key=lambda row:row['milliseconds'],reverse=True)


def queue_snapshot(connection):
    if connection is None:
        return None
    row = connection.execute("SELECT (SELECT COUNT(*) FROM pending_uploads),"
        "(SELECT COUNT(*) FROM pack_inputs),(SELECT COUNT(*) FROM chunk_encodings WHERE state='ready'),"
        "(SELECT COUNT(*) FROM tasks WHERE task_type IN ('upload','cache_flush') AND state IN ('queued','running'))").fetchone()
    return dict(zip(('pending','inputs','ready_encodings','upload_tasks'),row))


def drain(connection, seconds, sample=None):
    start = time.perf_counter()
    history = []
    while True:
        if sample:
            sample()
        queue = queue_snapshot(connection)
        elapsed = time.perf_counter()-start
        history.append(dict(seconds=elapsed,**queue))
        empty = queue['pending']==0 and queue['inputs']==0
        if empty or elapsed >= seconds:
            return dict(seconds=elapsed,complete=empty,samples=history)
        time.sleep(.5)


def measure(fixture, mode, credentials, database):
    seconds = float(fixture.get('seconds',60))
    concurrency = int(fixture.get('concurrency',4))
    maximum = int(fixture.get('max_operations',100000))
    slots = int(fixture.get('slots_per_worker',4))
    size = int(fixture.get('size',32768))
    drain_seconds = float(fixture.get('drain_seconds',0))
    unique_keys = fixture.get('unique_keys',False)
    assert 1 <= seconds <= 600 and 1 <= concurrency <= 32 and 1 <= maximum <= 1000000
    assert 1 <= slots <= 64 and 32 <= size <= 16*1024*1024
    assert 0 <= drain_seconds <= 600 and isinstance(unique_keys,bool)
    assert not drain_seconds or database is not None, 'drain measurement requires PostgreSQL'
    assert not unique_keys or maximum*size <= int(fixture['max_total_bytes']), 'unique keys require a bounded byte budget'
    assert mode in ('new','dedup','copy','multipart','many','mixed')
    endpoints = fixture['endpoints']
    assert 1 <= len(endpoints) <= 8
    payload = random.Random(fixture['seed']).randbytes(size)
    source = fixture.get('source_key','benchmark-source')
    if mode == 'many':
        source = fixture['many_source']['key']
        payload = Path(fixture['many_source']['expected_file']).read_bytes()
        assert len(payload) <= 64*1024*1024
    repairs = list(fixture.get('repair_payloads',[])) if mode == 'mixed' else []
    assert len(repairs) <= 1024
    if mode == 'mixed':
        assert repairs, 'mixed requires pre-quarantined payloads; an empty list is a healthy-only workload'
    lock = threading.Lock()
    next_request = 0
    repair_count = 0
    clients = [boto3.client('s3',endpoint_url=endpoints[i%len(endpoints)],region_name='us-east-1',
        aws_access_key_id=credentials['access_key'],aws_secret_access_key=credentials['secret_key'],
        config=Config(s3={'addressing_style':'path'},max_pool_connections=2,connect_timeout=5,read_timeout=30,
                      retries={'max_attempts':0},request_checksum_calculation='when_required',
                      response_checksum_validation='when_required')) for i in range(concurrency)]
    bucket = credentials['bucket']
    prefix = fixture['prefix'].rstrip('/')+'/'+mode
    if mode != 'many':
        clients[0].put_object(Bucket=bucket,Key=source,Body=payload)
    for index, client in enumerate(clients):
        operation(client,bucket,f'{prefix}/{index}/0',source,payload,'copy' if mode=='many' else 'dedup')

    resources = {name:Path(value['cgroup']) for name,value in fixture['resources'].items()}
    peak = {name:int((path/'memory.current').read_text()) for name,path in resources.items()}
    statuses = lambda: {name:status(value['status_url']) for name,value in fixture['resources'].items() if value.get('status_url')}
    warmup_drain = drain(database,drain_seconds) if drain_seconds else None
    before_queue = queue_snapshot(database)
    if drain_seconds:
        assert before_queue['pending']==0 and before_queue['inputs']==0, 'drain warmup before timing'
    before_status, before_sql = statuses(),sql_snapshot(database)
    before_cpu = {name:cpu(path) for name,path in resources.items()}
    waits, blocking, sampling_errors, queues, blocking_examples = Counter(),Counter(),[],[],[]
    stop = threading.Event()
    def sample():
        monitor = None
        try:
            if fixture.get('database_file'):
                import psycopg
                monitor = psycopg.connect(Path(fixture['database_file']).read_text().strip(),autocommit=True)
            tick = 0
            while not stop.wait(.1):
                for name,path in resources.items():
                    peak[name] = max(peak[name],int((path/'memory.current').read_text()))
                if monitor:
                    for application,event,detail,count in monitor.execute("SELECT application_name,COALESCE(wait_event_type,'Running'),COALESCE(wait_event,''),COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND pid<>pg_backend_pid() GROUP BY application_name,wait_event_type,wait_event"):
                        waits[application+':'+event+':'+detail] += count
                    for pid,blocker,query,blocking_query,application,blocking_application,state in monitor.execute(BLOCKING_SQL):
                        blocking[f'{application}/{query} <- {blocking_application}/{blocking_query}/{state}'] += 1
                        if len(blocking_examples)<128:
                            blocking_examples.append(dict(seconds=time.perf_counter()-sample_start,pid=pid,blocker=blocker,
                                queryid=query,blocking_queryid=blocking_query,blocking_state=state))
                    if tick%10==0:
                        queues.append(dict(seconds=time.perf_counter()-sample_start,**queue_snapshot(monitor)))
                    tick += 1
                    waits['samples'] += 1
        except Exception as error:
            sampling_errors.append(type(error).__name__)
        finally:
            if monitor:
                monitor.close()
    def worker(index):
        nonlocal next_request,repair_count
        timings,errors,repairs_ms,healthy_ms = [],Counter(),[],[]
        while time.perf_counter() < deadline:
            with lock:
                request = next_request
                if request >= maximum:
                    break
                next_request += 1
                repair = repairs.pop() if mode=='mixed' and request%32==0 and repairs else None
                repair_count += int(repair is not None)
            body = payload
            if mode == 'new':
                body = payload[:-16]+(fixture['round']*maximum+request).to_bytes(16,'big')
            elif repair:
                body = Path(repair).read_bytes()
            key = f'{prefix}/{index}/{request if unique_keys else request%slots}'
            try:
                elapsed = operation(clients[index],bucket,key,source,body,mode)
                timings.append(elapsed)
                if repair:
                    repairs_ms.append(elapsed)
                else:
                    healthy_ms.append(elapsed)
            except ClientError as error:
                errors[str(error.response['Error']['Code'])] += 1
            except (BotoCoreError,AssertionError,OSError,ValueError) as error:
                errors[type(error).__name__] += 1
        return timings,errors,repairs_ms,healthy_ms
    sampler = threading.Thread(target=sample,daemon=True)
    sample_start = time.perf_counter()
    sampler.start()
    start, process_start = time.perf_counter(),time.process_time()
    deadline = start+seconds
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(worker,range(concurrency)))
    finally:
        elapsed = time.perf_counter()-start
        process_cpu = time.process_time()-process_start
        stop.set()
        sampler.join(timeout=10)
        if sampler.is_alive():
            sampling_errors.append('sampler did not stop')
        for client in clients:
            client.close()
    after_sql,after_status = sql_snapshot(database),statuses()
    after_cpu = {name:cpu(path) for name,path in resources.items()}
    after_queue = queue_snapshot(database)
    request_peak = dict(peak)
    def sample_memory():
        for name,path in resources.items():
            peak[name] = max(peak[name],int((path/'memory.current').read_text()))
    drained = drain(database,drain_seconds,sample_memory) if drain_seconds else None
    drained_sql = sql_snapshot(database) if drained else None
    drained_status = statuses() if drained else None
    drained_cpu = {name:cpu(path) for name,path in resources.items()} if drained else None
    latencies = [value for result in results for value in result[0]]
    repair_latencies = [value for result in results for value in result[2]]
    healthy_latencies = [value for result in results for value in result[3]]
    errors = sum((result[1] for result in results),Counter())
    errors.update('sampling:'+error for error in sampling_errors)
    if drained and not drained['complete']:
        errors['drain timeout'] += 1
    return dict(mode=mode,attempts=next_request,successes=len(latencies),seconds=elapsed,
        cycles_per_second=len(latencies)/elapsed,write_latency=percentiles(latencies),errors=dict(errors),
        repair_attempts=repair_count,repair_write_latency=percentiles(repair_latencies),
        healthy_successes=len(healthy_latencies),healthy_write_latency=percentiles(healthy_latencies),
        readback='full body and length checked after every successful write; cycles include readback',
        multipart_latency='complete only; cycles also include create and part upload',
        client_process_cpu_seconds=process_cpu,status_before=before_status,status_after=after_status,
        database_active_samples=dict(waits),database_blocking_samples=dict(blocking),database_blocking_examples=blocking_examples,database_pool_wait_seconds=None,
        sql_delta=sql_changes(before_sql,after_sql),
        warmup_drain=warmup_drain,queue_before=before_queue,queue_after=after_queue,queue_samples=queues,drain=drained,
        sql_through_drain=sql_changes(before_sql,drained_sql) if drained else None,status_after_drain=drained_status,
        resources={name:dict(cpu_delta={key:value-before_cpu[name][key] for key,value in after_cpu[name].items()},
            cpu_through_drain={key:value-before_cpu[name][key] for key,value in drained_cpu[name].items()} if drained else None,
            sampled_request_memory_current_max=request_peak[name],
            sampled_memory_current_max=peak[name],cpu_max=(path/'cpu.max').read_text().strip(),
            cpuset=(path/'cpuset.cpus.effective').read_text().strip(),memory_max=(path/'memory.max').read_text().strip())
            for name,path in resources.items()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('fixture',type=Path)
    parser.add_argument('output',type=Path)
    args = parser.parse_args()
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES')=='isolated-only'
    fixture = json.loads(args.fixture.read_text())
    assert fixture['build']=='release' and fixture['revision'] and fixture['database_version'] and fixture['cache_state']
    assert isinstance(fixture['round'],int) and 0 <= fixture['round'] < 2**64
    credentials = json.loads(Path(fixture['credentials_file']).read_text())
    database = None
    if fixture.get('database_file'):
        import psycopg
        database = psycopg.connect(Path(fixture['database_file']).read_text().strip(),autocommit=True)
    report = dict(started_at=datetime.now(timezone.utc).isoformat(),
        provenance={key:fixture[key] for key in ('label','revision','build','database_version','cache_state','seed','round')},
        concurrency=fixture.get('concurrency',4),cases=[])
    if database:
        report['sql_settings'] = {key:database.execute('SHOW '+key).fetchone()[0] for key in
            ('pg_stat_statements.track','pg_stat_statements.track_planning','track_io_timing','fsync','synchronous_commit')}
    try:
        for mode in fixture['cases']:
            try:
                result = measure(fixture,mode,credentials,database)
            except Exception as error:
                result = dict(mode=mode,errors={type(error).__name__:1})
            report['cases'].append(result)
            args.output.write_text(json.dumps(report,indent=2)+'\n')
            print(json.dumps({key:result[key] for key in ('mode','errors','successes','seconds','cycles_per_second','write_latency') if key in result}),flush=True)
            if result['errors']:
                raise SystemExit(1)
    finally:
        if database:
            database.close()


if __name__ == '__main__':
    main()
