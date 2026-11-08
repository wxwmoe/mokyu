"""Per-storage pack limits and Range selection in an isolated two-store fixture."""
import json
import os
import subprocess
import time
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from tests.integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
second = json.loads(Path(os.environ['MOKYU_TEST_SECOND_CREDENTIALS']).read_text())
other = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                    aws_access_key_id=second['access_key'], aws_secret_access_key=second['secret_key'],
                    config=Config(s3={'addressing_style': 'path'},
                                  request_checksum_calculation='when_required',
                                  response_checksum_validation='when_required'))
stores = [os.environ['MOKYU_TEST_STORAGE'], os.environ['MOKYU_TEST_SECOND_STORAGE']]
ids = [db.execute('SELECT id FROM storages WHERE name=%s', (name,)).fetchone()[0] for name in stores]


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.1)
    raise AssertionError('pack profile operation timed out')


def task(policy='pack'):
    job = cli('pack', 'run', '--mode', policy)['task_id']
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
    state = cli('task', 'show', job)
    assert state['state'] == 'completed', state
    assert db.execute('SELECT count(*) FROM tasks WHERE parent_task_id=%s', (job,)).fetchone()[0] == 0
    assert state['detail']['storage_index'] == len(state['detail']['storages'])
    assert 'role' not in state['detail'] and 'children' not in state['detail']
    return state['detail']


def profile(index, **changes):
    value = cli('storage', 'profile', 'show', stores[index])
    value.update(changes)
    path = Path(os.environ['MOKYU_TEST_DATA']) / 'profile-test.json'
    path.write_text(json.dumps(value))
    try:
        cli('storage', 'profile', 'set', stores[index], str(path))
    finally:
        path.unlink(missing_ok=True)


profile(0, pack_max_size='4MiB')
profile(1, pack_enabled=False)
raw = os.urandom(15 * 1024 * 1024 + 17)
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'upload-before-work'
marker.with_suffix('.hit').unlink(missing_ok=True)
marker.touch()
try:
    s3.put_object(Bucket=bucket, Key='profile-source', Body=raw)
    wait(marker.with_suffix('.hit').exists)
finally:
    marker.unlink(missing_ok=True)
wait(lambda: db.execute('SELECT count(*)=0 FROM pending_uploads').fetchone()[0])
packs = db.execute("SELECT raw_size FROM packs WHERE storage_id=%s AND state='ready'", (ids[0],)).fetchall()
assert len(packs) > 1 and all(size <= 4 * 1024 * 1024 for (size,) in packs), packs
assert s3.get_object(Bucket=bucket, Key='profile-source')['Body'].read() == raw
assert s3.get_object(Bucket=bucket, Key='profile-source', Range='bytes=4000000-5000000')['Body'].read() == raw[4000000:5000001]
print('PASS upload-cache packs obey the 4MiB storage limit and preserve full/Range reads', flush=True)

other.put_object(Bucket=second['bucket'], Key='other-source', Body=raw)
detail = task()
assert {row['storage_id'] for row in detail['storages']} == {str(ids[0])}, detail
assert db.execute("SELECT count(*) FROM packs WHERE storage_id=%s AND state='ready'", (ids[1],)).fetchone()[0] == 0
profile(1, pack_enabled=True)
detail = task()
assert {row['storage_id'] for row in detail['storages']} == {str(value) for value in ids}, detail
other_pack = db.execute("SELECT id FROM packs WHERE storage_id=%s AND state='ready'", (ids[1],)).fetchone()[0]
assert other.get_object(Bucket=second['bucket'], Key='other-source')['Body'].read() == raw
print('PASS profiles isolate the pack size and enablement of both backends', flush=True)
