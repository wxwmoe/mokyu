"""Time-based read leases on two isolated data nodes with a 30s read idle timeout.

Uses state_checks credentials/DSN, MOKYU_TEST_DATA_ENDPOINTS (JSON array),
and MOKYU_TEST_FAULT_DIR for the first data node. Requires pg_stat_statements.
"""
import json
import math
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
endpoints = json.loads(os.environ['MOKYU_TEST_DATA_ENDPOINTS'])
assert len(endpoints) >= 2
bucket = credential['bucket']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
assert db.execute('SELECT deployment_mode FROM mokyu_meta WHERE singleton').fetchone()[0] == 'cluster'
s3 = boto3.client('s3', endpoint_url=endpoints[1], region_name='us-east-1',
    aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
    config=Config(s3={'addressing_style': 'path'}, request_checksum_calculation='when_required'))
key = 'read-leases-' + uuid.uuid4().hex
raw = os.urandom(128 * 1024 * 1024)
etag = s3.put_object(Bucket=bucket, Key=key, Body=raw, ACL='public-read')['ETag']
stream = db.execute('SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id '
                    'WHERE b.name=%s AND o.object_key=%s', (bucket, key)).fetchone()[0]
assert db.execute('SELECT count(*) FROM extents WHERE stream_id=%s', (stream,)).fetchone()[0] > 64
fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'read-body'
assert fault.parent.is_dir()


def wait(predicate):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if value := predicate():
            return value
        time.sleep(.02)
    raise AssertionError('lease condition timed out')


def calls(prefix):
    return sum(n for q, n in db.execute('SELECT query,calls FROM pg_stat_statements '
        'WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())')
        if q.startswith(prefix))


def renewals():
    return sum(n for q, n in db.execute('SELECT query,calls FROM pg_stat_statements '
        'WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())')
        if q.startswith('UPDATE stream_leases SET') and 'WHERE id=' in q)


def leases():
    return db.execute("SELECT id FROM stream_leases WHERE stream_id=%s AND lease_type='read'",
                      (stream,)).fetchall()


def get(endpoint):
    response = requests.get(f'{endpoint}/{bucket}/{key}', headers={'Accept-Encoding': 'identity'}, timeout=35)
    response.raise_for_status()
    assert response.content == raw


try:
    before, started = renewals(), time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(get, endpoints[:2]))
    elapsed = time.monotonic() - started
    assert renewals() - before <= 2 * math.ceil(elapsed / 20)
    wait(lambda: not leases())
    print('PASS two-node reads renew by elapsed time, not extent count', flush=True)

    for expire in (False, True):
        fault.with_suffix('.hit').unlink(missing_ok=True)
        fault.touch()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(get, endpoints[0])
            try:
                wait(fault.with_suffix('.hit').exists)
                lease, = leases()
                before = calls('INSERT INTO stream_leases')
                for endpoint in endpoints[:2]:
                    url = f'{endpoint}/{bucket}/{key}'
                    response = requests.head(url, timeout=3)
                    assert response.status_code == 200 and not response.content
                    response = requests.get(url, headers={'If-None-Match': etag}, timeout=3)
                    assert response.status_code == 304 and not response.content
                assert calls('INSERT INTO stream_leases') == before
                assert leases() == [lease]
                if expire:
                    db.execute("UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE id=%s", lease)
                before = renewals()
                time.sleep(21)
            finally:
                fault.unlink(missing_ok=True)
            if expire:
                try:
                    pending.result(timeout=15)
                except requests.RequestException:
                    pass
                else:
                    raise AssertionError('expired lease allowed a source read')
            else:
                pending.result(timeout=15)
            assert renewals() - before == 1
        wait(lambda: not leases())
    print('PASS HEAD/304 create no leases; delayed reads renew once and expired leases fail closed', flush=True)
finally:
    fault.unlink(missing_ok=True)
    fault.with_suffix('.hit').unlink(missing_ok=True)
    s3.delete_object(Bucket=bucket, Key=key)
    db.close()
