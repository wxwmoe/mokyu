"""One durable pack task spans storage profiles, pause/resume and bounded snapshots."""
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
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
assert db.execute("SELECT deployment_mode='single' FROM mokyu_meta").fetchone()[0]
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
second = json.loads(Path(os.environ['MOKYU_TEST_SECOND_CREDENTIALS']).read_text())
other = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                    aws_access_key_id=second['access_key'], aws_secret_access_key=second['secret_key'],
                    config=Config(s3={'addressing_style': 'path'}, request_checksum_calculation='when_required',
                                  response_checksum_validation='when_required'))
stores = [os.environ['MOKYU_TEST_STORAGE'], os.environ['MOKYU_TEST_SECOND_STORAGE']]
ids = [str(db.execute('SELECT id FROM storages WHERE name=%s', (name,)).fetchone()[0]) for name in stores]
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'pack-storage-advanced'
keys = ['single-pack-' + uuid.uuid4().hex for _ in range(3)]
raw = [os.urandom(9 * 1024 * 1024 + i) for i in range(3)]


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.1)
    raise AssertionError('single pack operation timed out')


def profile(index, **changes):
    value = cli('storage', 'profile', 'show', stores[index])
    value.update(changes)
    path = Path(os.environ['MOKYU_TEST_DATA']) / 'single-pack-profile.json'
    path.write_text(json.dumps(value))
    try:
        cli('storage', 'profile', 'set', stores[index], str(path))
    finally:
        path.unlink(missing_ok=True)


def completed(job):
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
    state = cli('task', 'show', job)
    assert state['state'] == 'completed', state
    assert state['detail']['storage_index'] == len(state['detail']['storages']), state
    assert 'role' not in state['detail'] and 'children' not in state['detail']
    assert db.execute('SELECT count(*) FROM tasks WHERE parent_task_id=%s', (job,)).fetchone()[0] == 0
    return state


original = [cli('storage', 'profile', 'show', name) for name in stores]
try:
    for i in range(2):
        profile(i, pack_enabled=False, upload_cache=False)
    empty = completed(cli('pack', 'run')['task_id'])
    assert empty['detail']['storages'] == [] and empty['processed'] == 0
    s3.put_object(Bucket=bucket, Key=keys[0], Body=raw[0])
    other.put_object(Bucket=second['bucket'], Key=keys[1], Body=raw[1])
    for i in range(2):
        profile(i, pack_enabled=True, pack_max_size='32MiB')
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    job = cli('pack', 'run')['task_id']
    wait(marker.with_suffix('.hit').exists)
    state = cli('task', 'show', job)
    assert state['detail']['storage_index'] == 1 and state['detail']['storage_id'] == ids[1], state
    assert state['cursor'] is None and state['processed'] > 0, state
    before = state['processed']
    snapshot = state['detail']['upper_time']
    cli('task', 'pause', job)
    profile(1, pack_max_size='16MiB')
    marker.unlink()
    # The next storage must also exclude streams newer than the original snapshot.
    other.put_object(Bucket=second['bucket'], Key=keys[2], Body=raw[2])
    cli('task', 'resume', job)
    state = completed(job)
    assert state['processed'] > before and state['detail']['upper_time'] == snapshot
    web = os.environ['MOKYU_TEST_WEB']
    session = requests.Session()
    login = session.post(web + '/api/v1/auth/login', headers={'Origin': web},
                         json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=20)
    login.raise_for_status()
    for storage in ids:
        page = session.get(web + '/api/v1/tasks', params={'storage': storage, 'policy': 'pack'}, timeout=20)
        page.raise_for_status()
        assert job in {item['id'] for item in page.json()['items']}
    assert db.execute("SELECT bool_and(c.pack_id IS NULL) FROM objects o JOIN extents e ON e.stream_id=o.stream_id "
                      'JOIN chunks c ON c.id=e.chunk_id WHERE o.object_key=%s', (keys[2],)).fetchone()[0]
    packs = db.execute('SELECT DISTINCT c.pack_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id '
                       'JOIN chunks c ON c.id=e.chunk_id WHERE o.object_key=%s', (keys[1],)).fetchall()
    assert packs and all(pack for (pack,) in packs)
    for (pack,) in packs:
        state = completed(cli('pack', 'unpack', str(pack), '--execute')['task_id'])
        assert len(state['detail']['storages']) == 1
        assert db.execute('SELECT count(*) FROM chunks WHERE pack_id=%s', (pack,)).fetchone()[0] == 0
    assert s3.get_object(Bucket=bucket, Key=keys[0])['Body'].read() == raw[0]
    assert other.get_object(Bucket=second['bucket'], Key=keys[1])['Body'].read() == raw[1]
    assert other.get_object(Bucket=second['bucket'], Key=keys[2])['Body'].read() == raw[2]
    assert other.get_object(Bucket=second['bucket'], Key=keys[1], Range='bytes=31-4097')['Body'].read() == raw[1][31:4098]
    print('PASS empty/single/multiple storage tasks, cumulative progress, profile resume, bounded snapshot and unpack bytes', flush=True)
finally:
    marker.unlink(missing_ok=True)
    for i, value in enumerate(original):
        value.pop('revision', None)
        profile(i, **value)
    for client, name, key in ((s3, bucket, keys[0]), (other, second['bucket'], keys[1]), (other, second['bucket'], keys[2])):
        client.delete_object(Bucket=name, Key=key)
