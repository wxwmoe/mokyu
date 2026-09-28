"""Management mutation, cursor and version checks on a disposable deployment."""
import json
import os
import socket
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
    aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
    config=Config(s3={'addressing_style': 'path'}))
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
session = requests.Session()
login = session.post(url + '/api/login', headers={'Origin': url}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
login.raise_for_status()
headers = {'Origin': url, 'X-CSRF-Token': login.json()['csrf_token']}
bucket_id = next(b['id'] for b in session.get(url + '/api/buckets').json() if b['name'] == bucket)


def get(path, **params):
    response = session.get(url + path, params=params)
    response.raise_for_status()
    assert response.headers['cache-control'] == 'private, no-store'
    return response.json()


def selected(key):
    obj = get('/api/object', bucket=bucket_id, key=key)['object']
    return {'key': key, 'version': obj['id']}


def change(action, objects, **extra):
    response = session.post(url + '/api/objects/actions', headers=headers,
        json={'bucket': bucket_id, 'action': action, 'objects': objects, **extra})
    assert response.status_code == 200, response.text
    assert response.headers.get('x-request-id')
    return [r['status'] for r in response.json()['results']]


try:
    prefix = 'management/'
    for key in ['a', 'b', 'nested/c', 'odd+%? #中', 'a//b']:
        s3.put_object(Bucket=bucket, Key=prefix + key, Body=b'shared management data', ContentType='text/plain')
    a, b = selected(prefix + 'a'), selected(prefix + 'b')
    s3.put_object(Bucket=bucket, Key=b['key'], Body=b'replacement')
    failed_before = get('/api/status')['runtime']['http']['manage']['failed']
    assert change('public-read', [a, b]) == [200, 412]
    assert get('/api/status')['runtime']['http']['manage']['failed'] > failed_before
    assert get('/api/object', bucket=bucket_id, key=a['key'])['object']['public_read']
    assert not get('/api/object', bucket=bucket_id, key=b['key'])['object']['public_read']
    assert change('delete', [b]) == [412]
    assert s3.get_object(Bucket=bucket, Key=b['key'])['Body'].read() == b'replacement'
    assert change('private', [a]) == [200]
    # ACL changes share the S3 path and must remain visible in both interfaces.
    s3.put_object_acl(Bucket=bucket, Key=a['key'], ACL='public-read')
    assert get('/api/object', bucket=bucket_id, key=a['key'])['object']['public_read']
    assert change('delete', [a]) == [200]
    assert change('delete', [a]) == [412]
    assert s3.get_object(Bucket=bucket, Key=prefix + 'nested/c')['Body'].read() == b'shared management data'
    print('PASS stale versions, partial results, S3 ACL reuse and shared data survival', flush=True)

    body = {'bucket': bucket_id, 'action': 'private', 'objects': [selected(b['key'])]}
    assert requests.post(url + '/api/objects/actions', json=body, headers=headers).status_code == 403
    assert session.post(url + '/api/objects/actions', json=body, headers={'Origin': url}).status_code == 403
    assert session.post(url + '/api/objects/actions', json=body, headers={**headers, 'Origin': 'https://invalid.example'}).status_code == 403
    for objects in [[], [b, b], [{'key': '', 'version': b['version']}], [{'key': 'x' * 1025, 'version': b['version']}],
                    [{'key': str(i), 'version': b['version']} for i in range(1001)]]:
        assert session.post(url + '/api/objects/actions', headers=headers, json={**body, 'objects': objects}).status_code == 400
    assert session.post(url + '/api/objects/actions', headers=headers, json={**body, 'unknown': True}).status_code == 422
    assert session.post(url + '/api/objects/actions', headers=headers, json={**body, 'action': 'purge'}).status_code == 422
    assert session.post(url + '/api/objects/actions', headers={**headers, 'Content-Type': 'application/json'}, data=b'x' * (2 * 1024 * 1024 + 1)).status_code == 413
    assert change('private', [{'key': prefix + f'missing-{i:04}', 'version': str(uuid.uuid4())} for i in range(1000)]) == [412] * 1000
    target = urlsplit(url)
    def stalled(authenticated):
        connection = socket.create_connection((target.hostname, target.port), timeout=75)
        auth = (f"Cookie: mokyu_session={session.cookies['mokyu_session']}\r\nOrigin: {url}\r\nX-CSRF-Token: {headers['X-CSRF-Token']}\r\n" if authenticated else '')
        connection.sendall((f'POST /api/objects/actions HTTP/1.1\r\nHost: {target.netloc}\r\n{auth}Content-Type: application/json\r\nContent-Length: 100000\r\n\r\n{{').encode())
        return connection
    unauthenticated = stalled(False)
    unauthenticated.settimeout(5)
    assert b'403' in unauthenticated.recv(4096).split(b'\r\n')[0]
    unauthenticated.close()
    slow = [stalled(True), stalled(True)]
    try:
        for _ in range(30):
            response = session.post(url + '/api/objects/actions', headers=headers, json=body)
            if response.status_code == 503: break
            time.sleep(.1)
        assert response.status_code == 503
        assert session.get(url + '/api/object', params={'bucket': bucket_id, 'key': b['key']}).status_code == 200
        assert requests.post(url + '/api/objects/actions', json=body).status_code == 403
        print('PASS admission rejects unauthenticated incomplete bodies; waiting for bounded slow-body timeout', flush=True)
        for connection in slow:
            assert b'408' in connection.recv(4096).split(b'\r\n')[0]
    finally:
        for connection in slow: connection.close()
    assert change('private', body['objects']) == [200]
    # Set maintenance through the running service, not only its database flag.
    import subprocess
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
    subprocess.run(command + ['maintenance', 'enable'], check=True, capture_output=True)
    assert change('private', body['objects']) == [503]
    subprocess.run(command + ['maintenance', 'disable'], check=True, capture_output=True)
    db.execute("UPDATE buckets SET state='purging' WHERE id=%s", (bucket_id,))
    assert change('private', body['objects']) == [409]
    db.execute("UPDATE buckets SET state='active' WHERE id=%s", (bucket_id,))
    print('PASS authentication, CSRF, input bounds, 1000-item request, maintenance and sealed buckets', flush=True)

    listing = get('/api/objects', bucket=bucket_id, prefix=prefix, limit=1, recursive='true')
    keys = [o['object_key'] for o in listing['objects']]
    token = listing['next_token']
    assert token
    assert session.get(url + '/api/objects', params={'bucket': bucket_id, 'prefix': prefix, 'token': token}).status_code == 400
    while token:
        listing = get('/api/objects', bucket=bucket_id, prefix=prefix, limit=1, recursive='true', token=token)
        keys.extend(o['object_key'] for o in listing['objects']); token = listing['next_token']
    assert keys == sorted(set(keys)) and prefix + 'nested/c' in keys

    db.execute("SELECT setval(pg_get_serial_sequence('chunks','id'),9007199254740992)")
    content = b'chunk-page-fixture-' * 1_000_000
    key = prefix + 'chunks'
    s3.put_object(Bucket=bucket, Key=key, Body=content)
    version = selected(key)['version']
    before = get('/api/status')['backend_gets']
    chunk_page = get('/api/object/chunks', bucket=bucket_id, key=key, version=version, limit=1)
    chunks = chunk_page['chunks']
    assert isinstance(chunks[0]['id'], str) and int(chunks[0]['id']) > 2**53
    while chunk_page['next_offset'] is not None:
        chunk_page = get('/api/object/chunks', bucket=bucket_id, key=key, version=version, limit=1, after=chunk_page['next_offset'])
        chunks.extend(chunk_page['chunks'])
    assert sum(c['length'] for c in chunks) == len(content)
    assert len({c['offset_bytes'] for c in chunks}) == len(chunks)
    assert all(c['compression'] == 'zstd' and c['stored_size'] - c['payload_size'] == 16 for c in chunks)
    assert get('/api/status')['backend_gets'] == before
    s3.put_object(Bucket=bucket, Key=key, Body=b'new')
    assert session.get(url + '/api/object/chunks', params={'bucket': bucket_id, 'key': key, 'version': version}).status_code == 412
    print('PASS prefix cursors, chunk pagination, bigint precision, stored compression metadata and no backend reads', flush=True)

    ids = [uuid.uuid4() for _ in range(225)]
    with db.transaction():
        for i, task_id in enumerate(ids):
            db.execute("INSERT INTO tasks(id,kind,state,created_at,error) VALUES(%s,'sweep',%s,'2026-01-01',%s)",
                       (task_id, 'paused' if i % 2 else 'failed', '<script>unsafe task error</script>'))
    found = []
    page = get('/api/tasks', state='paused', limit=17)
    token = page['next_token']
    assert session.get(url + '/api/tasks', params={'state': 'failed', 'token': token}).status_code == 400
    assert session.get(url + '/api/tasks', params={'state': 'unknown'}).status_code == 400
    assert session.get(url + '/api/tasks', params={'limit': 1000}).status_code == 400
    while True:
        found.extend(t['id'] for t in page['tasks'])
        if not page['next_token']: break
        page = get('/api/tasks', state='paused', limit=17, token=page['next_token'])
    assert len(found) == 112 == len(set(found))
    task_id = str(ids[0])
    assert get('/api/tasks/' + task_id)['error'].startswith('<script>')
    assert session.post(url + '/api/tasks/' + task_id + '/actions', json={'action': 'resume'}).status_code == 403
    assert session.post(url + '/api/tasks/' + task_id + '/actions', headers=headers, json={'action': 'pause'}).status_code == 409
    db.execute("UPDATE tasks SET state='running' WHERE id=%s", (task_id,))
    assert session.post(url + '/api/tasks/' + task_id + '/actions', headers=headers, json={'action': 'pause'}).status_code == 200
    db.execute("UPDATE tasks SET detail=%s WHERE id=%s", (json.dumps({'dry_run': False}), task_id))
    assert session.post(url + '/api/tasks/' + task_id + '/actions', headers=headers, json={'action': 'resume'}).status_code == 409
    # Resume an authorized non-destructive sweep from its saved scope.
    config = __import__('tomllib').loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())
    backend_prefix = config['backend'].get('prefix', '').rstrip('/')
    detail = {'dry_run': True, 'prefix': (backend_prefix + '/' if backend_prefix else ''), 'older_than_seconds': 172800,
              'cutoff': '2000-01-01T00:00:00Z', 'candidates': 0, 'bytes': 0, 'unrecognized': 0, 'samples': []}
    db.execute('UPDATE tasks SET detail=%s WHERE id=%s', (json.dumps(detail), task_id))
    subprocess.run(command + ['maintenance', 'enable'], check=True, capture_output=True)
    response = session.post(url + '/api/tasks/' + task_id + '/actions', headers=headers, json={'action': 'resume'})
    assert response.status_code == 200, response.text
    for _ in range(100):
        task = get('/api/tasks/' + task_id)
        if task['state'] in ('completed', 'failed'): break
        time.sleep(.1)
    assert task['state'] == 'completed', task
    subprocess.run(command + ['maintenance', 'disable'], check=True, capture_output=True)
    print('PASS task pagination, tied timestamps, state filters, CSRF and resumable worker', flush=True)
finally:
    db.close()
