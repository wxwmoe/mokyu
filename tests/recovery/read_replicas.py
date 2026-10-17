"""Public paging on a real isolated PostgreSQL standby, including replay and query failures.

Requires the response_cache environment plus MOKYU_TEST_REPLICA_DATABASE_FILE
(standby admin DSN), MOKYU_TEST_REPLICA_USER and a fault-injection data node.
Both database DSNs must belong to the disposable deployment under test.
"""
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config
from psycopg import sql

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credentials = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credentials['bucket']
endpoint = os.environ['MOKYU_TEST_ENDPOINT']
status_url = os.environ['MOKYU_TEST_STATUS_URL']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
replica = psycopg.connect(Path(os.environ['MOKYU_TEST_REPLICA_DATABASE_FILE']).read_text(), autocommit=True)
assert replica.execute('SELECT pg_is_in_recovery()').fetchone()[0]
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
    aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
    config=Config(retries={'max_attempts': 0}, s3={'addressing_style': 'path'}, request_checksum_calculation='when_required', response_checksum_validation='when_required'))
key = 'read-replicas-' + uuid.uuid4().hex
raw = os.urandom(193 * 1024)
s3.put_object(Bucket=bucket, Key=key, Body=raw, ACL='public-read')
stream, chunk = db.execute('SELECT o.stream_id,e.chunk_id FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id WHERE b.name=%s AND o.object_key=%s', (bucket,key)).fetchone()
assert db.execute('SELECT count(*) FROM extents WHERE stream_id=%s', (stream,)).fetchone()[0] == 1
with db.transaction():
    db.execute('DELETE FROM extents WHERE stream_id=%s',(stream,))
    for offset in range(0,len(raw),1024):
        db.execute('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(%s,%s,1024,%s,%s)',(stream,offset,chunk,offset))

def status():
    response = requests.get(status_url, timeout=5)
    response.raise_for_status()
    return response.json()['read_replica']

def read(expected=raw, **headers):
    response = requests.get(f'{endpoint}/{bucket}/{key}', headers={'Accept-Encoding':'identity','Cache-Control':'no-store',**headers}, timeout=15)
    assert response.status_code in (200,206) and response.content == expected
    return response

def caught_up():
    lsn = db.execute('SELECT pg_current_wal_insert_lsn()').fetchone()[0]
    for _ in range(1000):
        if replica.execute('SELECT pg_last_wal_replay_lsn()>=%s::pg_lsn',(lsn,)).fetchone()[0]: return
        time.sleep(.01)
    raise AssertionError('standby did not catch up')

def recover():
    before = status()['pages']
    for _ in range(5):
        time.sleep(status().get('retry_after_ms',0)/1000 + .1)
        read()
        if status()['pages'] > before: return
    raise AssertionError(('replica paging did not recover', status()))

def calls():
    return sum(count for query,count in db.execute('SELECT query,calls FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())') if query.startswith('SELECT e.*,c.id,'))

faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
reader = os.environ['MOKYU_TEST_REPLICA_USER']
try:
    caught_up()
    recover()
    before, pages = calls(), status()['pages']
    read()
    assert calls() - before == 1, 'only the first public page should use the primary'
    assert status()['pages'] - pages == 3
    read(raw[17:-13], Range=f'bytes=17-{len(raw)-14}')
    before = status()['attempts']
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    assert status()['attempts'] == before, 'signed reads must not use the replica'
    print('PASS first page on primary, later public pages on standby, Range and signed exclusions', flush=True)

    replica.execute('SELECT pg_wal_replay_pause()')
    before, pages = status()['lagging'], status()['pages']
    read()
    assert status()['lagging'] > before and status()['pages'] == pages
    s3.put_object_acl(Bucket=bucket, Key=key, ACL='private')
    response = requests.get(f'{endpoint}/{bucket}/{key}',timeout=5)
    assert response.status_code == 403
    s3.put_object_acl(Bucket=bucket, Key=key, ACL='public-read')
    replica.execute('SELECT pg_wal_replay_resume()')
    caught_up()
    recover()
    print('PASS invisible new lease falls back; stale public ACL never authorizes a request', flush=True)

    saved = db.execute('SELECT deployment_key_fingerprint FROM mokyu_meta WHERE singleton').fetchone()[0]
    try:
        db.execute('UPDATE mokyu_meta SET deployment_key_fingerprint=%s WHERE singleton',(bytes([7])*32,))
        caught_up()
        before = status()['errors']
        read()
        assert status()['errors'] > before
    finally:
        db.execute('UPDATE mokyu_meta SET deployment_key_fingerprint=%s WHERE singleton',(saved,))
    caught_up()
    recover()
    db.execute(sql.SQL('REVOKE SELECT ON extents FROM {}').format(sql.Identifier(reader)))
    try:
        caught_up()
        before = status()['errors']
        read()
        assert status()['errors'] > before
    finally:
        db.execute(sql.SQL('GRANT SELECT ON extents TO {}').format(sql.Identifier(reader)))
    caught_up()
    recover()
    print('PASS wrong deployment identity and revoked SELECT permission fall back and recover', flush=True)

    # A replayed primary DDL lock blocks a real standby query until its short timeout.
    fault = faults / 'read-replica-page'
    fault.with_suffix('.hit').unlink(missing_ok=True)
    fault.touch()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(read)
        try:
            for _ in range(500):
                if fault.with_suffix('.hit').exists(): break
                time.sleep(.01)
            assert fault.with_suffix('.hit').exists()
            db.execute('BEGIN')
            db.execute('LOCK TABLE extents IN ACCESS EXCLUSIVE MODE')
            caught_up()
            before = status()['errors']
            fault.unlink()
            for _ in range(500):
                if status()['errors'] > before: break
                time.sleep(.01)
            assert status()['errors'] > before
        finally:
            db.execute('ROLLBACK')
            fault.unlink(missing_ok=True)
            fault.with_suffix('.hit').unlink(missing_ok=True)
        pending.result(timeout=15)
    caught_up()
    recover()
    print('PASS real replayed DDL lock cancels the standby page and resumes the unsent page on primary', flush=True)

    fault = faults / 'read-replica-query'
    fault.touch()
    try:
        before = status()
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: read(), range(4)))
        after = status()
        assert after['errors'] > before['errors'] and after['fallbacks'] > before['fallbacks']
        assert after['pool_size'] <= after['connections'] and after['busy'] > before['busy']
        assert after['attempts'] - before['attempts'] <= after['connections']
        assert time.monotonic() - started < 8
    finally:
        fault.unlink(missing_ok=True)
        fault.with_suffix('.hit').unlink(missing_ok=True)
    recover()
    print('PASS bounded connection/query attempts, saturation fallback and single recovery probe', flush=True)

    fault = faults / 'read-replica-page'
    fault.with_suffix('.hit').unlink(missing_ok=True)
    fault.touch()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(requests.get, f'{endpoint}/{bucket}/{key}', headers={'Accept-Encoding':'identity','Cache-Control':'no-store'}, timeout=35)
        try:
            for _ in range(500):
                if fault.with_suffix('.hit').exists(): break
                time.sleep(.01)
            assert fault.with_suffix('.hit').exists()
            caught_up()
            replica.execute('SELECT pg_wal_replay_pause()')
            db.execute("UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE stream_id=%s AND lease_type='read'",(stream,))
            assert replica.execute("SELECT count(*) FROM stream_leases WHERE stream_id=%s AND lease_type='read' AND expires_at>now()",(stream,)).fetchone()[0] >= 1
            pages = status()['pages']
            time.sleep(21)
        finally:
            fault.unlink(missing_ok=True)
            fault.with_suffix('.hit').unlink(missing_ok=True)
        try:
            pending.result(timeout=10)
        except requests.RequestException:
            pass
        else:
            raise AssertionError('stale replica lease revived an expired primary lease')
        assert status()['pages'] == pages
    replica.execute('SELECT pg_wal_replay_resume()')
    caught_up()
    recover()
    print('PASS expired primary lease terminates the stream even while the standby retains its old lease', flush=True)

    # Hold an existing response after its first page, then remove the object on the primary.
    fault = faults / 'read-replica-page'
    fault.with_suffix('.hit').unlink(missing_ok=True)
    fault.touch()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(read)
        try:
            for _ in range(500):
                if fault.with_suffix('.hit').exists(): break
                time.sleep(.01)
            assert fault.with_suffix('.hit').exists()
            caught_up()
            replica.execute('SELECT pg_wal_replay_pause()')
            s3.put_object(Bucket=bucket, Key=key, Body=raw[::-1], ACL='public-read')
            response = requests.get(f'{endpoint}/{bucket}/{key}', headers={'Accept-Encoding':'identity'}, timeout=5)
            assert response.status_code == 200 and response.content == raw[::-1]
            s3.delete_object(Bucket=bucket, Key=key)
            assert db.execute("SELECT count(*) FROM stream_leases WHERE stream_id=%s AND lease_type='read' AND expires_at>now()",(stream,)).fetchone()[0] == 1
            assert requests.get(f'{endpoint}/{bucket}/{key}',timeout=5).status_code == 404
        finally:
            fault.unlink(missing_ok=True)
            fault.with_suffix('.hit').unlink(missing_ok=True)
        pending.result(timeout=15)
    replica.execute('SELECT pg_wal_replay_resume()')
    print('PASS in-flight response stays on its leased stream after overwrite and deletion; new requests see the replacement then 404', flush=True)
finally:
    replica.execute('SELECT pg_wal_replay_resume()')
    for name in ['read-replica-query','read-replica-page']:
        (faults/name).unlink(missing_ok=True)
        (faults/name).with_suffix('.hit').unlink(missing_ok=True)
    s3.delete_object(Bucket=bucket, Key=key)
    replica.close()
    db.close()
