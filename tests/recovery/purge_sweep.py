"""Purge pause boundaries and confirmed sweep retention in an isolated runtime."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
import psycopg
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
url = os.environ['MOKYU_TEST_WEB']
name = 'purge-' + uuid.uuid4().hex[:10]
directory = Path(os.environ['MOKYU_TEST_DATA']) / name


def cli(*args, success=True):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=30)
    assert (result.returncode == 0) == success, result.stderr
    return json.loads(result.stdout) if success else None


def wait(predicate):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('purge or sweep timed out')


def done(task):
    def ready():
        state = cli('task', 'show', str(task))
        assert state['state'] not in ('failed', 'paused'), state
        return state if state['state'] == 'completed' else None
    return wait(ready)


admin = requests.Session()
response = admin.post(url + '/api/v1/auth/login', headers={'Origin': url},
                      json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=20)
assert response.status_code == 200
admin.headers.update({'Origin': url, 'X-CSRF-Token': response.json()['csrf_token']})
assert admin.post(url + '/api/v1/account/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204


def post(path, body=None, status=200):
    response = admin.post(url + path, json=body, timeout=30)
    assert response.status_code == status, (response.status_code, response.text)
    return response.json() if status != 204 else None


storage = cli('storage', 'create', name, 'local', '--init', '--path', str(directory))['id']


def create(suffix):
    bucket = name + '-' + suffix
    record = cli('bucket', 'create', bucket, '--storage', storage)
    credentials = cli('cred', 'create', bucket)
    client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                          aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'])
    return bucket, record['id'], client


fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'purge-before-batch'
fault.with_suffix('.hit').unlink(missing_ok=True)
fault.touch()
tasks = []
try:
    for scenario in ('objects', 'uploads', 'empty'):
        bucket, bucket_id, client = create(scenario)
        if scenario == 'objects':
            client.put_object(Bucket=bucket, Key='keep', Body=b'keep until resumed')
        elif scenario == 'uploads':
            upload = client.create_multipart_upload(Bucket=bucket, Key='pending')['UploadId']
            client.upload_part(Bucket=bucket, Key='pending', UploadId=upload, PartNumber=1, Body=b'pending')
        preview = post(f'/api/v1/buckets/{bucket_id}/purge/preview')
        post(f'/api/v1/buckets/{bucket_id}/purge', {'confirm_name': 'wrong', 'confirmation': preview['confirmation']}, 412)
        post(f'/api/v1/buckets/{bucket_id}/purge', {'confirm_name': bucket, 'confirmation': 'invalid'}, 412)
        task = post(f'/api/v1/buckets/{bucket_id}/purge', {'confirm_name': bucket, 'confirmation': preview['confirmation']})['task_id']
        tasks.append((task, bucket_id))
    wait(fault.with_suffix('.hit').exists)
    cli('maintenance', 'enable')
finally:
    fault.unlink(missing_ok=True)
for task, bucket_id in tasks:
    wait(lambda: cli('task', 'show', task)['state'] == 'paused')
    assert db.execute('SELECT count(*) FROM buckets WHERE id=%s', (bucket_id,)).fetchone()[0] == 1
    cli('task', 'resume', task, success=False)
assert db.execute('SELECT count(*) FROM objects WHERE bucket_id=%s AND stream_id IS NOT NULL', (tasks[0][1],)).fetchone()[0] == 1
assert db.execute("SELECT count(*) FROM uploads WHERE bucket_id=%s AND state='active'", (tasks[1][1],)).fetchone()[0] == 1
cli('maintenance', 'disable')
for task, bucket_id in tasks:
    assert cli('task', 'show', task)['state'] == 'paused'
    cli('task', 'resume', task)
for task, bucket_id in tasks:
    done(task)
    assert db.execute('SELECT count(*) FROM buckets WHERE id=%s', (bucket_id,)).fetchone()[0] == 0
print('PASS confirmed purge, maintenance pause in object/upload/empty phases and explicit resume', flush=True)

bucket, bucket_id, client = create('retained')
raw = os.urandom(1024 * 1024)
client.put_object(Bucket=bucket, Key='retained', Body=raw)
orphans = []
for prefix in ('packs', '', 'chunks'):
    location = uuid.uuid4().hex
    path = directory / prefix / location[:2] / location
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'orphan')
    os.utime(path, (time.time() - 10, time.time() - 10))
    orphans.append(path)
retained = {path for path in directory.rglob('*') if path.is_file() and path not in orphans}


def retention(value):
    profile = cli('storage', 'profile', 'show', name)
    profile['min_storage_duration'] = value
    profile['maintenance_concurrency'] = 1
    path = Path(os.environ['MOKYU_TEST_DATA']) / (name + '.json')
    path.write_text(json.dumps(profile))
    try:
        cli('storage', 'profile', 'set', name, str(path))
    finally:
        path.unlink(missing_ok=True)


cli('maintenance', 'enable')
try:
    retention('30d')
    scan = done(post('/api/v1/maintenance/sweep', {'older_than': '1s', 'storage_id': storage})['task_id'])
    assert scan['detail']['candidates'] == 0 and all(path.exists() for path in orphans)
    retention('0s')
    scan = done(post('/api/v1/maintenance/sweep', {'older_than': '1s', 'storage_id': storage})['task_id'])
    assert scan['detail']['candidates'] == len(orphans) and all(path.exists() for path in orphans)
    assert len(set(scan['detail']['samples'])) == len(orphans)
    preview = post('/api/v1/maintenance/sweep/preview', {'task_id': scan['id']})
    post('/api/v1/maintenance/sweep/execute', {'preview_id': preview['id'], 'confirmation': 'invalid'}, 412)
    request = {'preview_id': preview['id'], 'confirmation': preview['confirmation']}
    task = post('/api/v1/maintenance/sweep/execute', request)['task_id']
    assert done(task)['detail']['candidates'] == len(orphans)
    assert post('/api/v1/maintenance/sweep/execute', request)['task_id'] == task
    assert not any(path.exists() for path in orphans) and all(path.exists() for path in retained)
    assert client.get_object(Bucket=bucket, Key='retained')['Body'].read() == raw
    print('PASS sweep minimum age, preview-only preservation, confirmation replay and indexed source retention', flush=True)
finally:
    cli('maintenance', 'disable')
