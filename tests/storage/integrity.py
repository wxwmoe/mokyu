"""Read-only integrity checks and reports on an explicitly disposable deployment."""
import json
import os
import subprocess
import time
import tomllib
import uuid
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
configuration = tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']


def cli(*args):
    return json.loads(subprocess.check_output([*command, *args], text=True))


bucket = 'integrity-' + uuid.uuid4().hex[:10]
storage_id = cli('storage', 'show', os.environ['MOKYU_TEST_STORAGE'])['id']
cli('bucket', 'create', bucket, '--storage', storage_id)
credential = cli('cred', 'create', bucket)
client_config = Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
    request_checksum_calculation='when_required', response_checksum_validation='when_required')
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
    aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'], config=client_config)
backend_file = os.environ.get('MOKYU_TEST_BACKEND')
backend_config = json.loads(Path(backend_file).read_text()) if backend_file else configuration['backend']
def secret(name):
    return backend_config.get(name) or (Path(os.environ['MOKYU_TEST_CONFIG']).parent / backend_config[name + '_file']).read_text().strip()
backend = boto3.client('s3', endpoint_url=backend_config['endpoint'], region_name=backend_config['region'],
    aws_access_key_id=secret('access_key'), aws_secret_access_key=secret('secret_key'), config=client_config)
session = requests.Session()
login = session.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
login.raise_for_status()
headers = {'Origin': url, 'X-CSRF-Token': login.json()['csrf_token']}
bucket_id = str(db.execute('SELECT id FROM buckets WHERE name=%s', (bucket,)).fetchone()[0])


def get(path, **params):
    r = session.get(url + path, params=params, timeout=20)
    r.raise_for_status()
    assert r.headers.get('x-request-id') and r.headers['cache-control'] == 'private, no-store'
    return r.json()


def wait(task):
    for _ in range(300):
        value = get('/api/v1/tasks/' + task)
        if value['status'] in ('completed', 'failed'):
            assert value['status'] == 'completed', value
            if value['detail'].get('role') == 'parent':
                children = db.execute('SELECT id FROM tasks WHERE parent_task_id=%s', (task,)).fetchall()
                assert len(children) == 1, children
                value = get('/api/v1/tasks/' + str(children[0][0]))
                assert value['status'] == 'completed', value
            return value
        time.sleep(.05)
    raise AssertionError('integrity task timed out')


def check(mode='metadata', key=None):
    r = session.post(url + '/api/v1/integrity', headers=headers, json={'mode': mode, 'bucket': bucket, 'key': key})
    assert r.status_code == 200, r.text
    value = wait(r.json()['task_id'])
    issues = get('/api/v1/tasks/' + value['id'] + '/issues')['items']
    return value, issues


def codes(mode, expected, key='a'):
    task, issues = check(mode, key)
    assert {i['code'] for i in issues} == set(expected), (task, issues)
    assert task['detail']['issues'] == len(issues)
    return task, issues


data = os.urandom(128 * 1024)
for key, body in [('a', data), ('b', data), ('empty', b'')]:
    s3.put_object(Bucket=bucket, Key=key, Body=body)
assert s3.get_object(Bucket=bucket, Key='a')['Body'].read() == data
cache = Path(os.environ['MOKYU_TEST_DATA']) / 'chunks'
cache_before = {str(p): p.read_bytes() for p in cache.rglob('*') if p.is_file()}
def backend_gets():
    return int(next(row['value'] for row in get('/api/v1/system/status')['counters'] if row['key'] == 'backend_gets'))

gets = backend_gets()
task, issues = check()
assert not issues and task['detail']['objects_checked'] == 3 and task['detail']['chunks_checked'] == 1, task
assert backend_gets() == gets
task, _ = check('head')
assert backend_gets() == gets
task, _ = check('full')
assert task['detail']['chunks_checked'] == 1 and backend_gets() == gets + 1
assert {str(p): p.read_bytes() for p in cache.rglob('*') if p.is_file()} == cache_before
assert wait(cli('integrity', 'check', '--bucket', bucket, '--mode', 'metadata')['task_id'])['detail']['issues'] == 0
print('PASS all modes, empty objects, shared chunks checked once, CLI, zero cache changes', flush=True)

row = db.execute("SELECT c.id,l.location_id,l.standalone_size,l.encryption_algorithm,l.block_key_id,l.nonce,l.standalone_compressed,s.id FROM chunks c JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.stream_id=s.id WHERE o.bucket_id=%s AND o.object_key='a'", (bucket_id,)).fetchone()
chunk, storage, stored, algorithm, key_id, nonce, compressed, stream = row
storage = storage.hex
path = '/'.join(filter(None, [backend_config.get('prefix', '').rstrip('/'), storage[:2], storage]))
remote = {'Bucket': backend_config['bucket'], 'Key': path}
encoded = backend.get_object(**remote)['Body'].read()
try:
    backend.delete_object(**remote)
    assert s3.get_object(Bucket=bucket, Key='a')['Body'].read() == data
    codes('metadata', [])
    _, issues = codes('head', ['remote_missing'])
    task, _ = codes('full', ['remote_missing'])
    related = get(f"/api/v1/tasks/{task['id']}/issues/{get('/api/v1/tasks/' + task['id'] + '/issues')['items'][0]['id']}/objects")
    assert {o['key'] for o in related['items']} == {'a', 'b'}
    backend.put_object(**remote, Body=bytes([encoded[0] ^ 1]) + encoded[1:])
    codes('head', [])
    codes('full', ['authentication_failed'])
    backend.put_object(**remote, Body=encoded[:-1])
    codes('head', ['length_mismatch'])
    codes('full', ['length_mismatch'])
    backend.put_object(**remote, Body=encoded)
    # Simulate corrupt key references only inside this isolated fixture transaction.
    with db.transaction():
        db.execute("SET LOCAL session_replication_role='replica'")
        db.execute('UPDATE chunks SET block_key_id=%s WHERE id=%s', (uuid.uuid4(), chunk))
        db.execute('UPDATE chunk_encodings SET block_key_id=%s WHERE chunk_id=%s', (uuid.uuid4(), chunk))
    codes('metadata', ['missing_key'])
    db.execute("UPDATE chunks SET encryption_algorithm='none',block_key_id=NULL,nonce=NULL,standalone_compressed=false,standalone_size=raw_size WHERE id=%s", (chunk,))
    db.execute("UPDATE chunk_encodings SET encryption_algorithm='none',block_key_id=NULL,nonce=NULL,standalone_compressed=false,standalone_size=%s WHERE chunk_id=%s", (len(data), chunk))
    backend.put_object(**remote, Body=b'!' + data[1:])
    codes('full', ['hash_mismatch'])
    broken = b'not a zstd frame'
    db.execute('UPDATE chunks SET standalone_compressed=true,standalone_size=%s WHERE id=%s', (len(broken), chunk))
    db.execute('UPDATE chunk_encodings SET standalone_compressed=true,standalone_size=%s WHERE chunk_id=%s', (len(broken), chunk))
    backend.put_object(**remote, Body=broken)
    codes('full', ['decompression_failed'])
finally:
    db.execute('UPDATE chunks SET standalone_size=%s,encryption_algorithm=%s,block_key_id=%s,nonce=%s,standalone_compressed=%s WHERE id=%s', (stored, algorithm, key_id, nonce, compressed, chunk))
    db.execute('UPDATE chunk_encodings SET standalone_size=%s,encryption_algorithm=%s,block_key_id=%s,nonce=%s,standalone_compressed=%s WHERE chunk_id=%s', (stored, algorithm, key_id, nonce, compressed, chunk))
    backend.put_object(**remote, Body=encoded)
codes('full', [])
print('PASS remote corruption hidden by cache, length/authentication/hash/decompression/key classifications', flush=True)

saved = db.execute('SELECT offset_bytes,length,chunk_id,source_offset FROM extents WHERE stream_id=%s', (stream,)).fetchall()
try:
    db.execute('UPDATE extents SET source_offset=1 WHERE stream_id=%s', (stream,))
    codes('metadata', ['mapping_source'])
    db.execute('DELETE FROM extents WHERE stream_id=%s', (stream,))
    # More than three mapping pages and two findings pages, without a large file in memory.
    with db.cursor() as cur:
        cur.executemany('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES(%s,%s,1,%s)', [(stream, i * 2, chunk) for i in range(210)])
    task, first = check(key='a')
    assert task['detail']['objects_checked'] == 1 and task['detail']['issues'] == 210, task
    path_api = '/api/v1/tasks/' + task['id']
    all_issues, after = [], 0
    while True:
        page = get(path_api + '/issues', after=after)
        assert len(page['items']) <= 100
        all_issues.extend(page['items'])
        if page['next_cursor'] is None: break
        assert isinstance(page['next_cursor'], str)
        after = page['next_cursor']
    assert len(all_issues) == 210 and len({i['id'] for i in all_issues}) == 210
    assert all(isinstance(i['id'], str) for i in all_issues)
    assert len(cli('integrity', 'issues', task['id'], '--limit', '1')['issues']) == 1
    exported = session.get(url + path_api + '/report')
    exported.raise_for_status()
    report = [json.loads(line) for line in exported.text.splitlines()]
    assert report[0]['type'] == 'task' and report[-1] == {'type': 'end', 'issues': 210} and len(report) == 212
    assert 'attachment;' in exported.headers['content-disposition']
    for suffix in ['/issues', '/report', '/issues/' + first[0]['id'] + '/objects']:
        assert requests.get(url + path_api + suffix).status_code == 403
    for params in [{'after': -1}, {'limit': 201}, {'limit': 0}]:
        assert session.get(url + path_api + '/issues', params=params).status_code == 400
    assert session.get(url + path_api + '/issues/' + str(2**63-1) + '/objects').status_code == 404
finally:
    db.execute('DELETE FROM extents WHERE stream_id=%s', (stream,))
    with db.cursor() as cur:
        cur.executemany('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(%s,%s,%s,%s,%s)', [(stream, *r) for r in saved])
codes('metadata', [])
print('PASS paged mappings/findings, exact-key scope, JSONL export, string IDs and access control', flush=True)

profile = cli('storage', 'profile', 'show', os.environ['MOKYU_TEST_STORAGE'])
profile_path = Path(os.environ['MOKYU_TEST_DATA']) / 'integrity-profile.json'
try:
    limited = {**profile, 'integrity_rps': 2, 'integrity_bandwidth': '64KiB', 'integrity_byte_budget': '64KiB'}
    profile_path.write_text(json.dumps(limited))
    cli('storage', 'profile', 'set', os.environ['MOKYU_TEST_STORAGE'], str(profile_path))
    checked, issues = check('full', 'a')
    assert checked['detail']['chunks_checked'] == 1 and not issues
finally:
    profile['revision'] = cli('storage', 'profile', 'show', os.environ['MOKYU_TEST_STORAGE'])['revision']
    profile_path.write_text(json.dumps(profile))
    cli('storage', 'profile', 'set', os.environ['MOKYU_TEST_STORAGE'], str(profile_path))
    profile_path.unlink()
print('PASS rate and byte budgets allow a chunk larger than one second of bandwidth', flush=True)

for body, status in [({'mode': 'invalid'}, 422), ({'unknown': 1}, 422), ({'key': 'a'}, 400),
                     ({'bucket': bucket, 'key': ''}, 400), ({'bucket': bucket, 'key': 'x' * 1025}, 400),
                     ({'bucket': bucket, 'key': 'missing'}, 404), ({'bucket': 'missing-bucket'}, 404)]:
    assert session.post(url + '/api/v1/integrity', headers=headers, json=body).status_code == status
assert requests.post(url + '/api/v1/integrity', headers=headers, json={}).status_code == 403
assert session.post(url + '/api/v1/integrity', headers={'Origin': url}, json={}).status_code == 403
assert session.post(url + '/api/v1/integrity', headers={**headers, 'Origin': 'https://invalid.example'}, json={}).status_code == 403
assert not list(cache.rglob('*.tmp'))
db.close()
print('PASS input validation, Origin and CSRF; all fixture corruption restored', flush=True)
