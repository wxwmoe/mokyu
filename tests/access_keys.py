"""Application key rotation and management-token isolation, using disposable Docker data."""
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
prefix = 'keys-' + uuid.uuid4().hex[:10]
password = 'Access-key-test-' + uuid.uuid4().hex
users, credentials, tokens = [], [], []
writer = None


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, check=True, timeout=20)
    return json.loads(result.stdout)


def login(name, secret):
    client = requests.Session()
    reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': name, 'password': secret}, timeout=10)
    assert reply.status_code == 200, reply.status_code
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    assert client.post(url + '/api/me/reauth', json={'password': secret}).status_code == 204
    return client


def mint(client, **input):
    reply = client.post(url + '/api/tokens', json={'label': prefix, **input})
    assert reply.status_code == 201, reply.status_code
    item = reply.json()
    tokens.append(item['token']['id'])
    bearer = requests.Session()
    bearer.headers['Authorization'] = 'Bearer ' + item['secret']
    return bearer, item


def s3(row):
    return boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                       aws_access_key_id=row['access_key'], aws_secret_access_key=row['secret_key'],
                       config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                                     request_checksum_calculation='when_required', response_checksum_validation='when_required'))


def denied(call, **kwargs):
    try:
        call(**kwargs)
    except ClientError as error:
        assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403
    else:
        raise AssertionError('revoked S3 key succeeded')


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
bucket = next(row for row in admin.get(url + '/api/buckets').json() if row['name'] == 'media')
grant = {'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'storage.inspect']}
key_input = {'label': prefix, 'project_id': bucket['project_id'], 'grants': [grant]}
try:
    # Secrets are creation-only; rotation creates a distinct access ID with an overlap.
    reply = admin.post(url + '/api/credentials', json=key_input)
    assert reply.status_code == 201, reply.status_code
    original = reply.json()
    credentials.append(original['access_key'])
    client = s3(original)
    assert [row['Name'] for row in client.list_buckets()['Buckets']] == ['media']
    denied(client.put_object, Bucket='media', Key=prefix, Body=b'not permitted')
    rows = admin.get(url + '/api/credentials', params={'project': bucket['project_id']}).text
    assert original['secret_key'] not in rows and 'secret_encrypted' not in rows
    assert db.execute('SELECT last_used_at IS NOT NULL FROM credentials WHERE access_key=%s', (original['access_key'],)).fetchone()[0]
    rotation = admin.post(url + '/api/credentials/' + original['access_key'] + '/rotate', json={'overlap': '1h'}).json()
    credentials.append(rotation['access_key'])
    assert rotation['access_key'] != original['access_key']
    client.list_buckets()
    replacement = s3(rotation)
    replacement.list_buckets()
    db.execute("UPDATE credentials SET expires_at=now()-interval '1 second' WHERE access_key=%s", (original['access_key'],))
    denied(client.list_buckets)
    replacement.list_buckets()
    cli('credential', 'disable', rotation['access_key'])
    denied(replacement.list_buckets)
    cli('credential', 'enable', rotation['access_key'])
    replacement.list_buckets()
    wrong = {**key_input, 'grants': [{'bucket_id': str(uuid.uuid4()), 'actions': ['object.read']}]}
    assert admin.post(url + '/api/credentials', json=wrong).status_code == 400
    assert cli('credential', 'show', rotation['access_key'])['grants'] == [grant]
    print('PASS S3 key secrecy, scoped permissions, expiration, overlapping rotation and CLI enable/disable', flush=True)

    # Admin-owned restricted tokens do not inherit instance-wide access.
    bearer, issued = mint(admin, grants=[grant])
    row = issued['token']
    assert row['active'] and row['expires_at'] and not row['system']
    assert [item['id'] for item in bearer.get(url + '/api/buckets').json()] == [bucket['id']]
    assert [item['id'] for item in bearer.get(url + '/api/projects').json()] == [bucket['project_id']]
    for path in ['/api/users', '/api/status', '/api/tasks', '/api/tokens', '/api/me', '/api/me/sessions', '/api/session', '/api/credentials']:
        assert bearer.get(url + path).status_code == 403, path
    assert bearer.post(url + '/api/tokens', json={'label': 'recursive'}).status_code == 403
    assert bearer.get(url + '/api/buckets', headers={'Cookie': 'unrelated=1'}).status_code == 403
    assert requests.get(url + '/api/buckets', params={'token': issued['secret']}).status_code == 403
    stored = db.execute('SELECT token_hash,prefix FROM api_tokens WHERE id=%s', (row['id'],)).fetchone()
    assert len(stored[0]) == 32 and stored[1] != issued['secret']
    listing = admin.get(url + '/api/tokens').text
    assert issued['secret'] not in listing and 'token_hash' not in listing
    assert db.execute('SELECT last_used_at IS NOT NULL FROM api_tokens WHERE id=%s', (row['id'],)).fetchone()[0]
    full, full_issued = mint(admin, system=True, expires_in=None)
    assert full.get(url + '/api/status').status_code == 200
    assert full.get(url + '/api/tokens').status_code == 403
    created = full.post(url + '/api/projects', json={'name': prefix + '-automation'})
    assert created.status_code == 201
    assert full.delete(url + '/api/projects/' + created.json()['id']).status_code == 204
    assert admin.delete(url + '/api/tokens/' + full_issued['token']['id']).status_code == 204
    assert full.get(url + '/api/status').status_code == 403
    assert admin.put(url + '/api/tokens/' + row['id'], json={'label': prefix, 'expires_in': None, 'grants': []}).status_code == 200
    assert bearer.get(url + '/api/buckets').json() == []
    assert bearer.get(url + '/api/projects').json() == []
    assert admin.put(url + '/api/tokens/' + row['id'], json={'label': prefix, 'expires_in': '1h', 'grants': [grant]}).status_code == 200
    db.execute("UPDATE api_tokens SET expires_at=now()-interval '1 second' WHERE id=%s", (row['id'],))
    assert bearer.get(url + '/api/buckets').status_code == 403
    print('PASS PAT hashing, explicit system scope, list isolation, cookie ambiguity, scope edits and revocation', flush=True)

    # Revocation wins while an admitted mutation is waiting before its final transaction.
    if os.environ.get('MOKYU_TEST_FAULT_DIR'):
        key = cli('credential', 'create', 'media', '--label', prefix + '-write')
        credentials.append(key['access_key'])
        writer = s3(key)
        writer.put_object(Bucket='media', Key=prefix, Body=b'private key regression')
        version = str(db.execute('SELECT stream_id FROM objects WHERE bucket_id=%s AND key=%s', (bucket['id'], prefix)).fetchone()[0])
        mutation, mutating = mint(admin, grants=[{**grant, 'actions': ['object.acl']}])
        gate = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'management-before-change'
        hit = gate.with_suffix('.hit')
        hit.unlink(missing_ok=True)
        gate.write_text('wait')
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(mutation.post, url + '/api/objects/actions', json={'bucket': bucket['id'], 'action': 'public-read', 'objects': [{'key': prefix, 'version': version}]}, timeout=20)
            try:
                deadline = time.monotonic() + 10
                while not hit.exists():
                    assert time.monotonic() < deadline, 'mutation did not reach the admitted boundary'
                    time.sleep(.02)
                cli('token', 'revoke', mutating['token']['id'])
            finally:
                gate.unlink(missing_ok=True)
            result = pending.result().json()
        assert result['results'][0]['status'] == 403
        assert not db.execute('SELECT public_read FROM streams WHERE id=%s', (version,)).fetchone()[0]
        denied(client.get_object, Bucket='media', Key=prefix)
        assert replacement.get_object(Bucket='media', Key=prefix)['Body'].read() == b'private key regression'
        print('PASS token revoked during an admitted mutation cannot commit; expired S3 key cannot download', flush=True)

    user = admin.post(url + '/api/users', json={'username': prefix, 'password': password, 'role': 'member', 'must_change_password': False}).json()
    users.append(user['id'])
    membership = f"/api/projects/{bucket['project_id']}/members/{user['id']}"
    assert admin.put(url + membership, json={'role': 'reader', 'scope': 'selected', 'grants': [grant]}).status_code == 204
    member = login(prefix, password)
    member_token, member_issued = mint(member, grants=[grant])
    assert member.post(url + '/api/tokens', json={'label': prefix, 'system': True}).status_code == 403
    assert member.post(url + '/api/tokens', json={'label': prefix, 'grants': [{**grant, 'actions': ['object.write']}]}).status_code == 403
    assert member.post(url + '/api/credentials', json=key_input).status_code == 403
    assert member.get(url + '/api/tokens', params={'user': admin.get(url + '/api/me').json()['id']}).status_code == 403
    assert member.delete(url + '/api/tokens/' + row['id']).status_code == 403
    assert admin.delete(url + membership).status_code == 204
    assert member_token.get(url + '/api/buckets').json() == []
    assert admin.put(url + membership, json={'role': 'reader', 'scope': 'selected', 'grants': [grant]}).status_code == 204
    assert member_token.get(url + '/api/buckets').json()[0]['id'] == bucket['id']
    assert admin.post(url + '/api/users/' + user['id'] + '/reset-password', json={'password': password + '-new', 'must_change_password': False}).status_code == 204
    assert member_token.get(url + '/api/buckets').status_code == 403
    replacement.list_buckets()
    # The key remains after deleting its recorded creator; tokens are removed with their owner.
    db.execute('UPDATE credentials SET created_by=%s WHERE access_key=%s', (user['id'], rotation['access_key']))
    assert admin.delete(url + '/api/users/' + user['id']).status_code == 204
    replacement.list_buckets()
    assert db.execute('SELECT created_by FROM credentials WHERE access_key=%s', (rotation['access_key'],)).fetchone()[0] is None
    print('PASS member permission intersection, reset revocation and project-owned key independence', flush=True)
finally:
    if writer:
        writer.delete_object(Bucket='media', Key=prefix)
    for key in credentials:
        admin.delete(url + '/api/credentials/' + key)
    for token in tokens:
        admin.delete(url + '/api/tokens/' + token)
    for user in users:
        admin.delete(url + '/api/users/' + user)
    db.close()
