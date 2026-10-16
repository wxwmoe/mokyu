"""A single integrity task retains storage snapshots and its report across restart."""
import fcntl
import json
import os
import signal
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
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
second = json.loads(Path(os.environ['MOKYU_TEST_SECOND_CREDENTIALS']).read_text())
other = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                    aws_access_key_id=second['access_key'], aws_secret_access_key=second['secret_key'],
                    config=Config(s3={'addressing_style': 'path'}, request_checksum_calculation='when_required',
                                  response_checksum_validation='when_required'))
stores = [os.environ['MOKYU_TEST_STORAGE'], os.environ['MOKYU_TEST_SECOND_STORAGE']]
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
points = ['integrity-before-commit', 'integrity-storage-advanced']
pid = int(os.environ['MOKYU_TEST_GATEWAY_PID'])
process = None
keys = ['single-integrity-' + uuid.uuid4().hex for _ in range(2)]
late = 'zzzz-late-' + uuid.uuid4().hex
raw = os.urandom(128 * 1024)
saved = []


def cli(*args):
    result = subprocess.run(command + ['cli', *args], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('single integrity operation timed out')


def complete(job):
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
    state = cli('task', 'show', job)
    assert state['state'] == 'completed', state
    assert db.execute('SELECT count(*) FROM tasks WHERE parent_task_id=%s', (job,)).fetchone()[0] == 0
    assert state['detail']['storage_index'] == 2 and 'role' not in state['detail']
    return state


def released():
    with (Path(os.environ['MOKYU_TEST_DATA']) / 'gateway.lock').open('rb') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False


def ready():
    assert process.poll() is None, 'replacement gateway exited; inspect single-integrity-recovery.log'
    result = subprocess.run(command + ['cli', 'status'], capture_output=True, text=True, timeout=20)
    return result.returncode == 0


profiles = [cli('storage', 'profile', 'show', name) for name in stores]
profile_file = Path(os.environ['MOKYU_TEST_DATA']) / 'single-integrity-profile.json'
try:
    for name, profile in zip(stores, profiles):
        profile_file.write_text(json.dumps({**profile, 'upload_cache': False, 'pack_enabled': False}))
        cli('storage', 'profile', 'set', name, str(profile_file))
    s3.put_object(Bucket=bucket, Key=keys[0], Body=raw)
    other.put_object(Bucket=second['bucket'], Key=keys[1], Body=raw)
    saved = db.execute('SELECT o.stream_id,s.size FROM objects o JOIN streams s ON s.id=o.stream_id '
                       'WHERE o.object_key=ANY(%s)', (keys,)).fetchall()
    assert len(saved) == 2
    for stream, size in saved:
        db.execute('UPDATE streams SET size=%s WHERE id=%s', (size + 1, stream))
    count = db.execute('SELECT count(*) FROM objects').fetchone()[0]
    marker = faults / 'integrity-storage-advanced'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    response = cli('integrity', 'check', '--mode', 'metadata')
    assert 'children' not in response
    job = response['task_id']
    wait(marker.with_suffix('.hit').exists)
    state = cli('task', 'show', job)
    assert state['detail']['storage_index'] == 1 and state['detail']['phase'] == 'metadata'
    assert state['detail']['issues'] == 1 and state['detail']['finished_at'] is None
    before = state['processed']
    snapshot = state['detail']['storages']
    cli('task', 'pause', job)
    marker.unlink()
    other.put_object(Bucket=second['bucket'], Key=late, Body=raw + b'late')
    cli('task', 'resume', job)
    state = complete(job)
    assert state['processed'] > before and state['detail']['objects_checked'] == count
    assert state['detail']['issues'] == 2 and state['detail']['storages'] == snapshot
    print('PASS one integrity report, storage snapshots and cumulative progress after pause/resume', flush=True)

    for point in points:
        marker = faults / point
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        job = cli('integrity', 'check', '--mode', 'metadata')['task_id']
        wait(marker.with_suffix('.hit').exists)
        os.kill(pid, signal.SIGKILL)
        if process:
            process.wait(timeout=10)
        wait(released)
        marker.unlink()
        with (Path(os.environ['MOKYU_TEST_RESULTS']) / 'single-integrity-recovery.log').open('a') as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        pid = process.pid
        Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE']).write_text(str(pid))
        wait(ready)
        state = complete(job)
        assert state['detail']['objects_checked'] == count + 1
        assert state['detail']['issues'] == 2
        issues = db.execute('SELECT code,stream_id FROM integrity_issues WHERE task_id=%s', (job,)).fetchall()
        assert len(issues) == 2 and {code for code, _ in issues} == {'object_length'}
        assert {stream for _, stream in issues} == {stream for stream, _ in saved}
        print('PASS integrity crash/restart, stable counters and deduplicated issues:', point, flush=True)

    web = os.environ['MOKYU_TEST_WEB']
    session = requests.Session()
    session.post(web + '/api/v1/auth/login', headers={'Origin': web},
                 json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=20).raise_for_status()
    report = session.get(web + f'/api/v1/tasks/{job}/report', timeout=20)
    report.raise_for_status()
    records = [json.loads(line) for line in report.text.splitlines()]
    assert len(records) == 4 and records[-1] == {'type': 'end', 'issues': 2}
    for stream, size in saved:
        db.execute('UPDATE streams SET size=%s WHERE id=%s', (size, stream))
    for mode in ('head', 'full'):
        state = complete(cli('integrity', 'check', '--mode', mode)['task_id'])
        assert state['detail']['issues'] == 0 and state['detail']['objects_checked'] == count + 1
    assert s3.get_object(Bucket=bucket, Key=keys[0])['Body'].read() == raw
    assert other.get_object(Bucket=second['bucket'], Key=keys[1])['Body'].read() == raw
    print('PASS combined JSONL report and head/full checks across both stores', flush=True)
finally:
    for point in points:
        (faults / point).unlink(missing_ok=True)
    for stream, size in saved:
        db.execute('UPDATE streams SET size=%s WHERE id=%s', (size, stream))
    for name, profile in zip(stores, profiles):
        current = cli('storage', 'profile', 'show', name)
        profile_file.write_text(json.dumps({**profile, 'revision': current['revision']}))
        cli('storage', 'profile', 'set', name, str(profile_file))
    profile_file.unlink(missing_ok=True)
    for client, name, key in ((s3, bucket, keys[0]), (other, second['bucket'], keys[1]), (other, second['bucket'], late)):
        client.delete_object(Bucket=name, Key=key)
    if process:
        process.terminate()
        process.wait(timeout=40)
