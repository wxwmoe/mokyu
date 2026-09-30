"""Browser multipart shares storage admission, checksums, generation guards and live project permissions."""
import base64
import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
prefix = 'web-upload-' + uuid.uuid4().hex[:10]
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']


def cli(*args):
    return json.loads(subprocess.run(command + list(args), capture_output=True, text=True, check=True, timeout=20).stdout)


def login(name, password):
    client = requests.Session()
    reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': name, 'password': password}, timeout=10)
    assert reply.status_code == 200, reply.status_code
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
bucket = cli('bucket', 'create', prefix)
created, user, token_id = [], None, None


def start(key, size, overwrite=False, client=None):
    body = {'client_id': str(uuid.uuid4()), 'key': key, 'file_name': key.split('/')[-1], 'size': str(size),
            'modified_at': 1234567890, 'content_type': 'application/octet-stream', 'overwrite': overwrite}
    reply = (client or admin).post(url + '/api/buckets/' + bucket['id'] + '/uploads', json=body, timeout=15)
    assert reply.status_code == 201, (reply.status_code, reply.text)
    value = reply.json()
    created.append(value['id'])
    assert value['can_resume'] and value['source'] == 'web' and value['received_bytes'] == '0'
    assert value['expires_at'] and value['part_size'] == str(16 * 1024 * 1024)
    return value, body


def part(id, number, data, client=None):
    checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
    reply = (client or admin).put(url + f'/api/uploads/{id}/parts/{number}', headers={'X-Content-SHA256': checksum}, data=data, timeout=60)
    assert reply.status_code == 200, (reply.status_code, reply.text)
    value = reply.json()
    assert value['sha256'] == checksum and value['number'] == number
    return value


def complete(id, receipts, status=200):
    reply = admin.post(url + '/api/uploads/' + id + '/complete', json={'parts': receipts}, timeout=60)
    assert reply.status_code == status, (reply.status_code, reply.text)
    return reply


try:
    first = b'part-one-' * (2 * 1024 * 1024)
    first = first[:16 * 1024 * 1024]
    tail = b'end'
    upload, body = start('folder/a +%.bin', len(first) + len(tail))
    repeat = admin.post(url + '/api/buckets/' + bucket['id'] + '/uploads', json=body)
    assert repeat.status_code == 201 and repeat.json()['id'] == upload['id']
    changed = admin.post(url + '/api/buckets/' + bucket['id'] + '/uploads', json={**body, 'size': '1'})
    assert changed.status_code == 400
    wrong = admin.put(url + '/api/uploads/' + upload['id'] + '/parts/2', headers={'X-Content-SHA256': base64.b64encode(bytes(32)).decode()}, data=tail)
    assert wrong.status_code == 400 and wrong.json()['code'] == 'BadDigest', wrong.text
    last = part(upload['id'], 2, tail)
    head = part(upload['id'], 1, first)
    listing = admin.get(url + '/api/uploads/' + upload['id'] + '/parts').json()
    assert [p['number'] for p in listing['parts']] == [1, 2]
    assert [p['sha256'] for p in listing['parts']] == [head['sha256'], last['sha256']]
    assert admin.get(url + '/api/uploads/' + upload['id']).json()['received_bytes'] == str(len(first) + len(tail))
    complete(upload['id'], [head], 400)
    reply = complete(upload['id'], [head, last])
    assert reply.json()['state'] == 'completed' and reply.json()['remote_state'] == 'stored', reply.json()
    assert complete(upload['id'], [head, last]).json()['state'] == 'completed'
    downloaded = admin.get(url + '/api/download', params={'bucket': bucket['id'], 'key': body['key']}, timeout=30)
    assert downloaded.content == first + tail
    event = db.execute('SELECT action,outcome FROM audit_events WHERE request_id=%s', (reply.headers['X-Request-ID'],)).fetchone()
    assert event == ('upload.complete', 'succeeded'), event
    print('PASS idempotent creation, out-of-order checksummed parts, complete retry, actual content and transactional audit', flush=True)

    empty, _ = start('empty', 0)
    assert complete(empty['id'], [part(empty['id'], 1, b'')]).json()['state'] == 'completed'
    conflict = admin.post(url + '/api/buckets/' + bucket['id'] + '/uploads', json={**body, 'client_id': str(uuid.uuid4())})
    assert conflict.status_code == 409 and conflict.json()['code'] == 'ObjectAlreadyExists'
    key = cli('credential', 'create', prefix)
    s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                      aws_access_key_id=key['access_key'], aws_secret_access_key=key['secret_key'],
                      config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}, request_checksum_calculation='when_required'))
    s3.put_object(Bucket=prefix, Key='replace', Body=b'abc')
    replacement, _ = start('replace', 3, True)
    receipt = part(replacement['id'], 1, b'def')
    s3.put_object(Bucket=prefix, Key='replace', Body=b'abc')
    denied = complete(replacement['id'], [receipt], 412)
    assert denied.json()['code'] == 'PreconditionFailed'
    assert admin.get(url + '/api/download', params={'bucket': bucket['id'], 'key': 'replace'}).content == b'abc'
    assert admin.delete(url + '/api/uploads/' + replacement['id']).status_code == 204
    cli('credential', 'disable', key['access_key'])
    print('PASS empty object, explicit overwrite and generation protection despite identical old ETag', flush=True)

    waiting, _ = start('quota', 1)
    limit = admin.put(url + '/api/quotas/bucket/' + bucket['id'], json={'byte_limit': '0'})
    assert limit.status_code == 200
    denied = admin.put(url + '/api/uploads/' + waiting['id'] + '/parts/1', data=b'x', headers={'X-Content-SHA256': base64.b64encode(hashlib.sha256(b'x').digest()).decode()})
    assert denied.status_code == 403 and denied.json()['code'] == 'QuotaExceeded', denied.text
    assert admin.put(url + '/api/quotas/bucket/' + bucket['id'], json={'byte_limit': None}).status_code == 200
    secret = 'Upload-test-' + uuid.uuid4().hex
    member_reply = admin.post(url + '/api/users', json={'username': prefix, 'password': secret, 'role': 'member', 'must_change_password': False})
    assert member_reply.status_code == 201, member_reply.text
    user = member_reply.json()['id']
    project = next(b['project_id'] for b in admin.get(url + '/api/buckets').json() if b['id'] == bucket['id'])
    membership = url + f'/api/projects/{project}/members/{user}'
    grants = [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'object.write']}]
    assert admin.put(membership, json={'role': 'writer', 'scope': 'selected', 'grants': grants}).status_code == 204
    member = login(prefix, secret)
    assert member.get(url + '/api/uploads/' + waiting['id']).status_code == 404
    own, _ = start('member', 1, client=member)
    assert member.get(url + '/api/uploads', params={'bucket': bucket['id']}).json()['uploads'][0]['id'] == own['id']
    assert admin.put(membership, json={'role': 'reader', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read']}]}).status_code == 204
    revoked = member.put(url + '/api/uploads/' + own['id'] + '/parts/1', data=b'x', headers={'X-Content-SHA256': base64.b64encode(hashlib.sha256(b'x').digest()).decode()})
    assert revoked.status_code in (403, 404)
    assert admin.put(membership, json={'role': 'maintainer', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'object.write', 'bucket.settings']}]}).status_code == 204
    assert member.get(url + '/api/uploads/' + waiting['id']).json()['can_resume'] is False
    denied = member.put(url + '/api/uploads/' + waiting['id'] + '/parts/1', data=b'x', headers={'X-Content-SHA256': base64.b64encode(hashlib.sha256(b'x').digest()).decode()})
    assert denied.status_code == 403
    assert member.delete(url + '/api/uploads/' + waiting['id']).status_code == 204
    print('PASS shared quota, owner-only resume, live write revocation and maintainer cancellation', flush=True)
    issued = admin.post(url + '/api/tokens', json={'label': prefix, 'grants': [{'bucket_id': bucket['id'], 'actions': ['object.write']}]}).json()
    token_id = issued['token']['id']
    bearer = requests.Session()
    bearer.headers['Authorization'] = 'Bearer ' + issued['secret']
    scoped, _ = start('token', 3, client=bearer)
    receipt = part(scoped['id'], 1, b'pat', client=bearer)
    reply = bearer.post(url + '/api/uploads/' + scoped['id'] + '/complete', json={'parts': [receipt]})
    assert reply.status_code == 200 and reply.json()['state'] == 'completed', reply.text
    rows = bearer.get(url + '/api/uploads?state=all').json()['uploads']
    assert rows and all(item['bucket_id'] == bucket['id'] for item in rows)
    assert admin.put(url + '/api/tokens/' + token_id, json={'label': prefix, 'grants': []}).status_code == 200
    assert bearer.get(url + '/api/uploads/' + scoped['id']).status_code == 404
    assert bearer.get(url + '/api/uploads?state=all').json()['uploads'] == []
    print('PASS scoped management token upload and immediate scope removal', flush=True)
finally:
    for id in created:
        admin.delete(url + '/api/uploads/' + id)
    if user:
        admin.delete(url + '/api/users/' + user)
    if token_id:
        admin.delete(url + '/api/tokens/' + token_id)
    db.close()
