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

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
configuration = tomllib.loads(Path(os.environ['MGW_TEST_CONFIG']).read_text())
url = os.environ['MGW_TEST_WEB']
db = psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MGW_TEST_BINARY'], '--config', os.environ['MGW_TEST_CONFIG'], 'cli']


def cli(*args):
    return json.loads(subprocess.check_output([*command, *args], text=True))


bucket = 'integrity-' + uuid.uuid4().hex[:10]
cli('bucket', 'create', bucket)
credential = cli('credential', 'create', bucket)
client_config = Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
    request_checksum_calculation='when_required', response_checksum_validation='when_required')
s3 = boto3.client('s3', endpoint_url=os.environ['MGW_TEST_ENDPOINT'], region_name='us-east-1',
    aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'], config=client_config)
backend_config = configuration['backend']
def secret(name):
    return backend_config.get(name) or (Path(os.environ['MGW_TEST_CONFIG']).parent / backend_config[name + '_file']).read_text().strip()
backend = boto3.client('s3', endpoint_url=backend_config['endpoint'], region_name=backend_config['region'],
    aws_access_key_id=secret('access_key'), aws_secret_access_key=secret('secret_key'), config=client_config)
session = requests.Session()
login = session.post(url + '/api/login', headers={'Origin': url}, json={'username': 'tester', 'password': os.environ['MGW_TEST_PASSWORD']})
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
        value = get('/api/tasks/' + task)
        if value['state'] in ('completed', 'failed'):
            assert value['state'] == 'completed', value
            return value
        time.sleep(.05)
    raise AssertionError('integrity task timed out')


def check(mode='metadata', key=None):
    r = session.post(url + '/api/integrity', headers=headers, json={'mode': mode, 'bucket': bucket, 'key': key})
    assert r.status_code == 200, r.text
    value = wait(r.json()['task_id'])
    issues = get('/api/tasks/' + value['id'] + '/issues')['issues']
    return value, issues


def codes(mode, expected, key='a'):
    task, issues = check(mode, key)
    assert {i['code'] for i in issues} == set(expected), (task, issues)
    assert task['detail']['issues'] == len(issues)
    return task, issues


data = os.urandom(128 * 1024)
for key, body in [('a', data), ('b', data), ('empty', b'')]:
    s3.put_object(Bucket=bucket, Key=key, Body=body)
cache = Path(os.environ['MGW_TEST_DATA']) / 'chunks'
cache_before = {str(p): p.read_bytes() for p in cache.rglob('*') if p.is_file()}
gets = get('/api/status')['backend_gets']
task, issues = check()
assert not issues and task['detail']['objects_checked'] == 3 and task['detail']['chunks_checked'] == 1, task
assert get('/api/status')['backend_gets'] == gets
task, _ = check('head')
assert get('/api/status')['backend_gets'] == gets
task, _ = check('full')
assert task['detail']['chunks_checked'] == 1 and get('/api/status')['backend_gets'] == gets + 1
assert {str(p): p.read_bytes() for p in cache.rglob('*') if p.is_file()} == cache_before
assert wait(cli('integrity', 'check', '--bucket', bucket, '--mode', 'metadata')['task_id'])['detail']['issues'] == 0
print('PASS all modes, empty objects, shared chunks checked once, CLI, zero cache changes', flush=True)

row = db.execute("SELECT c.id,c.storage_id,c.stored_size,c.algorithm,c.key_id,c.nonce,c.compressed,s.id FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.stream_id=s.id WHERE o.bucket_id=%s AND o.key='a'", (bucket_id,)).fetchone()
chunk, storage, stored, algorithm, key_id, nonce, compressed, stream = row
storage = storage.hex
path = '/'.join(filter(None, [backend_config.get('prefix', '').rstrip('/'), 'chunks', storage[:2], storage]))
remote = {'Bucket': backend_config['bucket'], 'Key': path}
encoded = backend.get_object(**remote)['Body'].read()
try:
    backend.delete_object(**remote)
    assert s3.get_object(Bucket=bucket, Key='a')['Body'].read() == data
    codes('metadata', [])
    _, issues = codes('head', ['remote_missing'])
    task, _ = codes('full', ['remote_missing'])
    related = get(f"/api/tasks/{task['id']}/issues/{get('/api/tasks/' + task['id'] + '/issues')['issues'][0]['id']}/objects")
    assert {o['key'] for o in related['objects']} == {'a', 'b'}
    backend.put_object(**remote, Body=bytes([encoded[0] ^ 1]) + encoded[1:])
    codes('head', [])
    codes('full', ['authentication_failed'])
    backend.put_object(**remote, Body=encoded[:-1])
    codes('head', ['length_mismatch'])
    codes('full', ['length_mismatch'])
    backend.put_object(**remote, Body=encoded)
    db.execute('UPDATE chunks SET key_id=%s WHERE id=%s', ('missing-inspection-key', chunk))
    codes('metadata', ['missing_key'])
    db.execute("UPDATE chunks SET algorithm='none',key_id='',nonce=NULL,compressed=false,stored_size=raw_size WHERE id=%s", (chunk,))
    db.execute('UPDATE chunk_locations SET nonce=NULL,compressed=false,stored_size=%s WHERE chunk_id=%s', (len(data), chunk))
    backend.put_object(**remote, Body=b'!' + data[1:])
    codes('full', ['hash_mismatch'])
    broken = b'not a zstd frame'
    db.execute('UPDATE chunks SET compressed=true,stored_size=%s WHERE id=%s', (len(broken), chunk))
    db.execute('UPDATE chunk_locations SET compressed=true,stored_size=%s WHERE chunk_id=%s', (len(broken), chunk))
    backend.put_object(**remote, Body=broken)
    codes('full', ['decompression_failed'])
finally:
    db.execute('UPDATE chunks SET stored_size=%s,algorithm=%s,key_id=%s,nonce=%s,compressed=%s WHERE id=%s', (stored, algorithm, key_id, nonce, compressed, chunk))
    db.execute('UPDATE chunk_locations SET stored_size=%s,nonce=%s,compressed=%s WHERE chunk_id=%s', (stored, nonce, compressed, chunk))
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
    path_api = '/api/tasks/' + task['id']
    all_issues, after = [], 0
    while True:
        page = get(path_api + '/issues', after=after)
        assert len(page['issues']) <= 100
        all_issues.extend(page['issues'])
        if page['next_after'] is None: break
        assert isinstance(page['next_after'], str)
        after = page['next_after']
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

for body, status in [({'mode': 'invalid'}, 422), ({'unknown': 1}, 422), ({'key': 'a'}, 400),
                     ({'bucket': bucket, 'key': ''}, 400), ({'bucket': bucket, 'key': 'x' * 1025}, 400),
                     ({'bucket': bucket, 'key': 'missing'}, 404), ({'bucket': 'missing-bucket'}, 404)]:
    assert session.post(url + '/api/integrity', headers=headers, json=body).status_code == status
assert requests.post(url + '/api/integrity', headers=headers, json={}).status_code == 403
assert session.post(url + '/api/integrity', headers={'Origin': url}, json={}).status_code == 403
assert session.post(url + '/api/integrity', headers={**headers, 'Origin': 'https://invalid.example'}, json={}).status_code == 403
assert not list(cache.rglob('*.tmp'))
db.close()
print('PASS input validation, Origin and CSRF; all fixture corruption restored', flush=True)
