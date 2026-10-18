"""Single-container crash recovery through public APIs on every database backend."""
import json
import fcntl
import os
import signal
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
import boto3
from tests.integration import s3, bucket, error, config, endpoint
from tests.recovery.source_repair import Database

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
data = Path(os.environ['MOKYU_TEST_DATA'])
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
pidfile = Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE'])
storage = os.environ['MOKYU_TEST_STORAGE']
web = os.environ['MOKYU_TEST_WEB']
points = ('upload-cache-written', 'upload-cache-durable', 'upload-cache-before-commit',
          'upload-cache-committed', 'object-published', 'pack-stored',
          'pack-before-publish', 'upload-before-release')
prefix = 'restart-' + uuid.uuid4().hex
profile_file = data / (prefix + '.json')
process = None


def cli(*args):
    result = subprocess.run(command + ['cli', *args], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('single restart condition timed out')


def alive():
    stat = Path('/proc') / pidfile.read_text().strip() / 'stat'
    return stat.exists() and stat.read_text().rsplit(') ', 1)[1].split()[0] != 'Z'


def kill():
    pid = int(pidfile.read_text())
    argv = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
    assert os.fsencode(command[0]) in argv and os.fsencode(command[2]) in argv
    os.kill(pid, signal.SIGKILL)
    if process:
        process.wait(timeout=15)
    wait(lambda: not alive())


def start(maintenance=False):
    global process
    def unlocked():
        with (data / 'gateway.lock').open('rb') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(unlocked)
    with (data / 'restart-test.log').open('a') as log:
        process = subprocess.Popen(command + (['--maintenance'] if maintenance else []),
                                   stdout=log, stderr=log, start_new_session=True)
    pidfile.write_text(str(process.pid))
    def ready():
        assert process.poll() is None, 'runtime exited during recovery'
        return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=15).returncode == 0
    wait(ready)


def complete(response):
    job = response['task_id']
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
    row = cli('task', 'show', job)
    assert row['state'] == 'completed', row
    assert 'parent_task_id' not in row and 'children' not in row['detail']


def profile(**changes):
    value = cli('storage', 'profile', 'show', storage)
    profile_file.write_text(json.dumps({**value, **changes}))
    cli('storage', 'profile', 'set', storage, str(profile_file))


def read(key, raw):
    with s3.get_object(Bucket=bucket, Key=key)['Body'] as body:
        assert body.read() == raw
    with s3.get_object(Bucket=bucket, Key=key, Range='bytes=17-4097')['Body'] as body:
        assert body.read() == raw[17:4098]


original = cli('storage', 'profile', 'show', storage)
try:
    profile(upload_cache=True, pack_enabled=True, pack_max_size='32MiB')
    complete(cli('cache', 'flush'))
    for index, point in enumerate(points):
        key = f'{prefix}-{index}'
        raw = os.urandom(6 * 1024 * 1024 + index)
        marker = faults / point
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        if index < 5:
            (faults / 'upload-before-work').touch()
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(s3.put_object, Bucket=bucket, Key=key, Body=raw)
            if index >= 5:
                request.result(timeout=60)
            wait(marker.with_suffix('.hit').exists)
            kill()
            try:
                request.result(timeout=20)
            except Exception:
                assert index < 5
        marker.unlink()
        (faults / 'upload-before-work').unlink(missing_ok=True)
        start()
        if index >= 4:
            read(key, raw)
        else:
            error('404', s3.head_object, Bucket=bucket, Key=key)
        complete(cli('cache', 'flush'))
        assert cli('cache', 'status')['reserved_bytes'] == 0
        if index >= 4:
            read(key, raw)
        s3.delete_object(Bucket=bucket, Key=key)
        print('PASS single publication crash boundary:', point, flush=True)

    database = Database()
    for point in ('upload-prepare-before-commit', 'upload-before-work'):
        gate = faults / 'upload-before-prepare'
        gate.with_suffix('.hit').unlink(missing_ok=True)
        gate.touch()
        key = prefix + '-' + point
        raw = os.urandom(6 * 1024 * 1024 + 13)
        s3.put_object(Bucket=bucket, Key=key, Body=raw)
        wait(gate.with_suffix('.hit').exists)
        marker = faults / point
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        gate.unlink()
        wait(marker.with_suffix('.hit').exists)
        pins = database.query('SELECT COUNT(*) AS n FROM pack_inputs')[0]['n']
        assert (pins == 0) if point == 'upload-prepare-before-commit' else (pins > 0), pins
        kill()
        marker.unlink()
        start(maintenance=True)
        assert database.query('SELECT COUNT(*) AS n FROM pack_inputs')[0]['n'] == 0
        read(key, raw)
        complete(cli('cache', 'flush'))
        assert database.query('SELECT COUNT(*) AS n FROM pending_uploads')[0]['n'] == 0
        kill()
        start(maintenance=True)
        read(key, raw)
        cli('maintenance', 'disable')
        s3.delete_object(Bucket=bucket, Key=key)
        print('PASS acknowledged upload survives preparation crash and cold read:', point, flush=True)

    marker = faults / 'upload-before-work'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    key = prefix + '-rotation'
    raw = os.urandom(7 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    wait(marker.with_suffix('.hit').exists)
    running = [task['id'] for task in cli('task', 'list')
               if task['task_type'] == 'upload' and task['state'] == 'running']
    assert running
    kill()
    marker.unlink()
    start(maintenance=True)
    assert cli('status')['maintenance']
    assert all(cli('task', 'show', task)['state'] == 'paused' for task in running)
    assert cli('cache', 'status')['reserved_bytes'] > 0
    before = cli('status')['backend_puts']
    time.sleep(.5)
    assert cli('status')['backend_puts'] == before
    name = prefix + '-key'
    rotated = cli('key', 'create', name, 'block', '-a', 'aes-256-gcm')
    cli('storage', 'key', 'set', storage, name)
    read(key, raw)
    complete(cli('cache', 'flush'))
    read(key, raw)

    session = requests.Session()
    reply = session.post(web + '/api/v1/auth/login', headers={'Origin': web},
                         json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=15)
    reply.raise_for_status()
    def get(path, **params):
        reply = session.get(web + '/api/v1/' + path, params=params, timeout=20)
        reply.raise_for_status()
        return reply.json()
    bucket_id = next(row['id'] for row in get('buckets') if row['name'] == bucket)
    obj, = get(f'buckets/{bucket_id}/objects', q=key, mode='exact', search_in='path', recursive='true')['items']
    rows = get(f'buckets/{bucket_id}/object/chunks', key=key, version=obj['id'], limit=200)['items']
    assert rows
    packs = {row['pack_id'] for row in rows if row['source'] == 'pack'}
    for pack in packs:
        assert get('packs/' + pack)['pack']['block_key_id'] == rotated['id']
    assert all(row['block_key_id'] == rotated['id'] for row in rows if row['source'] != 'pack')
    cli('maintenance', 'disable')
    assert not (data / 'node.json').exists()
    s3.delete_object(Bucket=bucket, Key=key)
    print('PASS pending data survives restart and flush uses the rotated write key', flush=True)

    local = prefix + '-local'
    record = cli('storage', 'create', local, 'local', '--init', '--key',
                 os.environ['MOKYU_TEST_BLOCK_KEY'], '--path', str(data / local))
    local_profile = cli('storage', 'profile', 'show', local)
    profile_file.write_text(json.dumps({**local_profile, 'upload_cache': False, 'pack_enabled': False}))
    cli('storage', 'profile', 'set', local, str(profile_file))
    cli('bucket', 'create', local, '--storage', record['id'])
    credentials = cli('cred', 'create', local)
    client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1', config=config,
                          aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'])
    client.put_object(Bucket=local, Key='persistent', Body=raw)
    kill()
    start()
    with client.get_object(Bucket=local, Key='persistent')['Body'] as body:
        assert body.read() == raw
    with client.get_object(Bucket=local, Key='persistent', Range='bytes=31-8192')['Body'] as body:
        assert body.read() == raw[31:8193]
    client.delete_object(Bucket=local, Key='persistent')
    print('PASS encrypted local storage, managed keys and credentials survive restart', flush=True)
finally:
    for point in (*points, 'upload-before-work', 'upload-before-prepare', 'upload-prepare-before-commit'):
        (faults / point).unlink(missing_ok=True)
    if not alive():
        start()
    cli('maintenance', 'disable')
    cli('storage', 'key', 'set', storage, os.environ['MOKYU_TEST_BLOCK_KEY'])
    original.pop('revision', None)
    profile(**original)
    profile_file.unlink(missing_ok=True)
