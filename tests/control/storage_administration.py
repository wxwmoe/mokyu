"""Storage administration preserves revision races, rollback and encrypted reads."""
import json
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = 'storage-' + uuid.uuid4().hex[:10]
directory = Path(os.environ['MOKYU_TEST_DATA']) / name
document = directory.with_suffix('.json')
admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url},
                   json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=15)
assert reply.status_code == 200
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
assert admin.post(url + '/api/v1/account/reauth',
                  json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204


def cli(*args, success=True):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=30)
    assert (result.returncode == 0) == success, result.stderr
    return json.loads(result.stdout) if success else None


def call(method, path, body=None, status=200):
    result = admin.request(method, url + '/api/v1/' + path, json=body, timeout=30)
    assert result.status_code == status, (path, result.status_code, result.text)
    return result.json() if status != 204 else None


def profile():
    return call('GET', 'storages/' + name + '/profile')


def snapshot():
    return db.execute('SELECT s.path,s.block_key_id,s.enabled,p.revision,p.read_concurrency '
                      'FROM storages s JOIN storage_profiles p ON p.storage_id=s.id WHERE s.name=%s',
                      (name,)).fetchone()


try:
    initial = call('POST', 'storages', {'name': name, 'type': 'local', 'path': str(directory),
                                       'initialize': True, 'read_concurrency': 2}, 201)
    assert initial['profile_revision'] == 1 and initial['read_concurrency'] == 2
    assert not {'secret_key', 'access_key', 'identity_marker'} & initial.keys()
    plain = profile()
    assert not any(plain[field] for field in ['cache_enabled', 'upload_cache', 'pack_enabled', 'pack_range_enabled'])
    payload = {**plain, 'read_concurrency': 3}
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(lambda _: admin.put(url + '/api/v1/storages/' + name + '/profile',
                                                    json=payload, timeout=30), range(2)))
    assert sorted(r.status_code for r in replies) == [200, 409]
    assert next(r for r in replies if r.status_code == 409).json()['code'] == 'StorageProfileConflict'
    assert profile()['revision'] == 2 and profile()['read_concurrency'] == 3
    before = snapshot()
    rejected = call('PUT', 'storages/' + name + '/profile', {**profile(), 'cache_enabled': True}, 400)
    assert rejected['code'] == 'LocalStorageFeatures' and snapshot() == before
    document.write_text(json.dumps({**profile(), 'read_concurrency': 4}))
    updated = cli('storage', 'profile', 'set', name, str(document))
    assert updated['revision'] == 3 and updated['read_concurrency'] == 4
    cli('storage', 'profile', 'set', name, str(document), success=False)
    audit = db.execute("SELECT detail->>'revision' FROM audit_events WHERE action='storage.profile.update' "
                       'AND target=%s ORDER BY id DESC LIMIT 1', (name,)).fetchone()
    assert audit[0] == '3'
    shown = cli('storage', 'show', name)
    assert 'created_at' not in shown and shown == cli('storage', 'check', name)
    assert shown == next(row for row in cli('storage', 'list') if row['id'] == initial['id'])
    assert call('POST', 'storages/' + name + '/check')['id'] == initial['id']
    print('PASS storage profile races, validation, audit and Web/CLI parity', flush=True)

    second = call('POST', 'storages', {'name': name + '-other', 'type': 'local',
                                      'path': str(directory) + '-other', 'initialize': True,
                                      'profile': {**profile(), 'read_concurrency': 5}}, 201)
    assert second['profile_revision'] == 2 and second['read_concurrency'] == 5
    before = snapshot()
    failed = call('PUT', 'storages/' + name, {'path': str(directory) + '-other', 'read_concurrency': 6}, 409)
    assert failed['code'] == 'StorageVerificationFailed' and snapshot() == before
    stale = call('PUT', 'storages/' + name, {'profile': payload}, 409)
    assert stale['code'] == 'StorageProfileConflict' and snapshot() == before
    duplicate = admin.post(url + '/api/v1/storages', json={'name': name + '-bad', 'type': 'local',
                                                         'path': str(directory), 'initialize': True}, timeout=30)
    assert duplicate.status_code >= 400
    assert db.execute('SELECT count(*) FROM storages WHERE name=%s', (name + '-bad',)).fetchone()[0] == 0
    changed = call('PUT', 'storages/' + name, {'read_concurrency': None})
    assert changed['read_concurrency'] is None and changed['profile_revision'] == before[3] + 1
    print('PASS storage identity failures roll back rows and revisions; explicit profile and nullable patches', flush=True)

    key = call('POST', 'keys', {'name': name, 'purpose': 'block', 'algorithm': 'aes-256-gcm'}, 201)
    selected = cli('storage', 'key', 'set', name, name)
    assert selected['block_key_id'] == key['id']
    assert call('DELETE', 'keys/' + key['id'], status=400)['code'] == 'InvalidRequest'
    cleared = cli('storage', 'key', 'clear', name)
    assert cleared['block_key_id'] is None and cleared['profile_revision'] == selected['profile_revision'] + 1
    call('PUT', 'storages/' + name + '/key', {'key': key['id']})
    bucket = cli('bucket', 'create', name, '--storage', initial['id'])
    host = name + '.example.test'
    cli('domain', 'set', host, bucket['name'])
    try:
        assert {'host': host, 'bucket': bucket['name']} in cli('domain', 'list')
    finally:
        cli('domain', 'delete', host)
    listed = cli('user', 'list')
    tester = next(user for user in listed if user['username'] == 'tester')
    assert not {'password_hash', 'session_revision'} & tester.keys()
    member = call('POST', 'users', {'username': name, 'password': uuid.uuid4().hex,
                                    'role': 'member', 'must_change_password': False}, 201)
    document.write_text(json.dumps({'label': name, 'expires_in': '1h'}))
    token = cli('token', 'create', name, str(document))
    try:
        assert token['token']['user_id'] == member['id']
        tokens = cli('token', 'list', name)
        assert token['token']['id'] in {row['id'] for row in tokens}
        assert token['secret'] not in json.dumps(tokens)
        body = {'label': name + '-updated', 'keep_expiry': True, 'expires_in': None}
        call('PUT', 'api-tokens/' + token['token']['id'], body, 403)
        document.write_text(json.dumps(body))
        updated = cli('token', 'update', token['token']['id'], str(document))
        assert updated['label'] == name + '-updated'
    finally:
        cli('token', 'revoke', token['token']['id'])
        call('DELETE', 'users/' + member['id'], status=204)
    cli('token', 'list', name + '-missing', success=False)
    cli('token', 'update', str(uuid.uuid4()), str(document), success=False)
    print('PASS CLI user, token and domain metadata with safe fields and missing-row failures', flush=True)
    credential = cli('cred', 'create', bucket['name'])
    client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                          aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                          config=Config(s3={'addressing_style': 'path'}, request_checksum_calculation='when_required'))
    body = os.urandom(128 * 1024)
    client.put_object(Bucket=bucket['name'], Key='encrypted', Body=body)
    call('PUT', 'storages/' + name + '/key', {'key': None})
    assert client.get_object(Bucket=bucket['name'], Key='encrypted')['Body'].read() == body
    client.put_object(Bucket=bucket['name'], Key='plain', Body=b'storage refresh')
    assert client.get_object(Bucket=bucket['name'], Key='plain')['Body'].read() == b'storage refresh'
    assert cli('storage', 'rewrite', name, 'preview')['preview']['storage_id'] == initial['id']
    assert cli('storage', 'rewrite', initial['id'], 'preview')['preview']['storage_id'] == initial['id']
    print('PASS storage key selection/clearing, encrypted reads after refresh and storage name/ID lookup', flush=True)
finally:
    document.unlink(missing_ok=True)
    db.close()
