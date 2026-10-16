"""Direct single-process inventory retains snapshots on bounded query failure."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config
from tests.integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
dsn = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text()
db = psycopg.connect(dsn, autocommit=True)
assert db.execute("SELECT deployment_mode='single' FROM mokyu_meta").fetchone()[0]
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
second = json.loads(Path(os.environ['MOKYU_TEST_SECOND_CREDENTIALS']).read_text())
other = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                    aws_access_key_id=second['access_key'], aws_secret_access_key=second['secret_key'],
                    config=Config(s3={'addressing_style': 'path'}, request_checksum_calculation='when_required',
                                  response_checksum_validation='when_required'))
ids = [str(db.execute('SELECT storage_id FROM buckets WHERE name=%s', (name,)).fetchone()[0])
       for name in (bucket, second['bucket'])]
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'inventory-storage'
keys = ['inventory-' + uuid.uuid4().hex for _ in range(3)]
payloads = [os.urandom(128 * 1024), os.urandom(192 * 1024 + 1), os.urandom(64 * 1024)]


def cli(*args, timeout=10):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    end = time.monotonic() + 30
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('inventory refresh did not finish')


def refreshed(expected):
    value = cli('status')['storage']
    if value.get('last_error') is not None or set(value.get('storages', {})) != set(ids):
        return None
    matches = all(int(value['storages'][storage]['logical_bytes']) == size for storage, size in zip(ids, expected))
    return value if matches else None


assert cli('settings', 'get', 'metrics')['query_timeout'] == '2s'
assert db.execute("SELECT count(*) FROM tasks WHERE task_type='usage'").fetchone()[0] == 0
try:
    s3.put_object(Bucket=bucket, Key=keys[0], Body=payloads[0])
    other.put_object(Bucket=second['bucket'], Key=keys[1], Body=payloads[1])
    baseline = wait(lambda: refreshed([len(payloads[0]), len(payloads[1])]))
    assert all(baseline['storages'][storage]['objects'] == 1 for storage in ids)
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    wait(marker.with_suffix('.hit').exists)
    before = cli('status')['storage']
    s3.put_object(Bucket=bucket, Key=keys[2], Body=payloads[2])
    with psycopg.connect(dsn) as blocker:
        blocker.execute("SET lock_timeout='1s'")
        blocker.execute('LOCK chunks IN ACCESS EXCLUSIVE MODE')
        marker.unlink()
        failed = wait(lambda: (value if (value := cli('status')['storage'])['last_error'] else None))
        assert failed['last_error'] == 'refresh_failed' and failed['stale']
        assert failed['snapshot'] == before['snapshot'] and failed['storages'] == before['storages']
        # The two-connection application pool must still serve control requests.
        status = cli('status', timeout=5)
        assert status['role'] == 'single' and status['db_pool_size'] <= 2
        blocker.rollback()
    fresh = wait(lambda: refreshed([len(payloads[0]) + len(payloads[2]), len(payloads[1])]))
    assert fresh['snapshot']['as_of'] != before['snapshot']['as_of']
    assert not fresh['stale'] and fresh['storages'][ids[0]]['objects'] == 2
    web = os.environ['MOKYU_TEST_WEB']
    session = requests.Session()
    session.post(web + '/api/v1/auth/login', headers={'Origin': web},
                 json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=10).raise_for_status()
    for name in (bucket, second['bucket']):
        ident = str(db.execute('SELECT id FROM buckets WHERE name=%s', (name,)).fetchone()[0])
        def scope_ready():
            reply = session.get(web + '/api/v1/usage/summary', params={'bucket': ident}, timeout=10)
            reply.raise_for_status()
            value = reply.json()
            return value if value['usage'] is not None and not value['stale'] else None
        assert wait(scope_ready)['scope'] == 'bucket'
    assert db.execute('SELECT count(*) FROM usage_history').fetchone()[0] > 0
    assert db.execute("SELECT count(*) FROM tasks WHERE task_type='usage'").fetchone()[0] == 0
    print('PASS two-store sampling, bounded timeout, retained snapshots, two-connection pool, scope refresh and history without usage tasks', flush=True)
finally:
    marker.unlink(missing_ok=True)
    for client, name, key in ((s3, bucket, keys[0]), (other, second['bucket'], keys[1]), (s3, bucket, keys[2])):
        client.delete_object(Bucket=name, Key=key)
    db.close()
