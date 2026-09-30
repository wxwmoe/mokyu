"""Account lifecycle, memberships, forced rotation and concurrent last-admin protection."""
import json
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
prefix = 'users-' + uuid.uuid4().hex[:10]
password = 'User-test-' + uuid.uuid4().hex
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
created = []
other_admins = [row[0] for row in db.execute("SELECT id FROM web_users WHERE enabled AND role='admin' AND username<>'tester'")]
isolated_admins = False


def cli(*args, secret=None, success=True):
    reply = subprocess.run(command + list(args), input=secret, capture_output=True, text=True, timeout=20)
    assert (reply.returncode == 0) == success, reply.stderr
    return json.loads(reply.stdout) if success else reply.stderr


def login(name, secret):
    client = requests.Session()
    reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': name, 'password': secret}, timeout=10)
    assert reply.status_code == 200, reply.status_code
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
try:
    for suffix in ['a', 'b']:
        reply = admin.post(url + '/api/users', json={'username': prefix + suffix, 'password': password, 'role': 'member'})
        assert reply.status_code == 201, reply.text
        created.append(reply.json())
        assert reply.json()['must_change_password']
        assert not any(key in reply.json() for key in ['password_hash', 'password', 'avatar_email'])
    user = created[0]
    assert admin.post(url + '/api/users', json={'username': user['username'], 'password': password, 'role': 'member'}).status_code == 409
    for values in [{'role': 'owner'}, {'username': ' leading'}, {'password': 'short'}, {'unknown': True}]:
        assert admin.post(url + '/api/users', json={'username': prefix + '-invalid', 'password': password, 'role': 'member', **values}).status_code in (400, 422)
    page = admin.get(url + '/api/users', params={'q': prefix, 'limit': 1}).json()
    assert len(page['users']) == 1 and page['next'] == user['username']
    following = admin.get(url + '/api/users', params={'q': prefix, 'limit': 1, 'after': page['next']}).json()
    assert following['users'][0]['id'] == created[1]['id'] and following['next'] is None
    member = login(user['username'], password)
    assert member.get(url + '/api/me').json()['must_change_password']
    denied = member.get(url + '/api/buckets')
    assert denied.status_code == 403 and denied.json()['code'] == 'PasswordChangeRequired'
    replacement = password + '-rotated'
    assert member.post(url + '/api/me/password', json={'current_password': password, 'new_password': replacement}).status_code == 204
    assert not member.get(url + '/api/me').json()['must_change_password']
    assert member.get(url + '/api/buckets').json() == []
    assert member.patch(url + '/api/users/' + user['id'], json={'role': 'admin'}).status_code == 403
    print('PASS safe user records, validation, directory pagination and required password rotation', flush=True)

    bucket = next(item for item in admin.get(url + '/api/buckets').json() if item['name'] == 'media')
    project = bucket['project_id']
    target = f"/api/projects/{project}/members/{user['id']}"
    base = {'role': 'reader', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'storage.inspect']}]}
    assert admin.put(url + target, json=base).status_code == 204
    assert member.get(url + '/api/buckets').json()[0]['id'] == bucket['id']
    assert member.get(url + f'/api/projects/{project}/members').status_code == 403
    invalid = {**base, 'grants': [{'bucket_id': bucket['id'], 'actions': ['object.write']}]}
    assert admin.put(url + target, json=invalid).status_code == 400
    invalid = {**base, 'grants': base['grants'] * 2}
    assert admin.put(url + target, json=invalid).status_code == 400
    invalid = {**base, 'grants': [{'bucket_id': str(uuid.uuid4()), 'actions': ['bucket.list']}]}
    assert admin.put(url + target, json=invalid).status_code == 400
    assert admin.put(url + target, json={'role': 'maintainer', 'scope': 'all', 'grants': []}).status_code == 204
    assert 'bucket.settings' in member.get(url + '/api/buckets').json()[0]['actions']
    assert admin.delete(url + target).status_code == 204
    assert member.get(url + '/api/buckets').json() == []
    assert admin.put(url + target, json=base).status_code == 204
    assert admin.patch(url + '/api/users/' + user['id'], json={'role': 'admin'}).status_code == 200
    assert member.get(url + '/api/session').status_code == 403
    assert not any(item['user_id'] == user['id'] for item in admin.get(url + f'/api/projects/{project}/members').json())
    assert admin.patch(url + '/api/users/' + user['id'], json={'role': 'member'}).status_code == 200
    member = login(user['username'], replacement)
    assert member.get(url + '/api/buckets').json() == []
    assert admin.post(url + '/api/users/' + user['id'] + '/reset-password', json={'password': password}).status_code == 204
    assert member.get(url + '/api/session').status_code == 403
    member = login(user['username'], password)
    assert member.get(url + '/api/session').json()['must_change_password']
    assert admin.patch(url + '/api/users/' + user['id'], json={'enabled': False}).status_code == 200
    assert member.get(url + '/api/me').status_code == 403
    assert requests.post(url + '/api/login', headers={'Origin': url}, json={'username': user['username'], 'password': password}).status_code == 403
    cli('user', 'enable', user['username'])
    assert login(user['username'], password).get(url + '/api/me').status_code == 200
    print('PASS membership caps, scope replacement, promotion cleanup, reset and account revocation', flush=True)

    # The two remaining administrators cannot both be disabled, even through local CLI.
    backup = prefix + '-admin'
    record = cli('user', 'create', backup, '--password-stdin', secret=password + '\n')
    created.append(record)
    db.execute('UPDATE web_users SET enabled=false WHERE id=ANY(%s)', (other_admins,))
    isolated_admins = True
    assert db.execute("SELECT count(*) FROM web_users WHERE enabled AND role='admin'").fetchone()[0] == 2
    with ThreadPoolExecutor(max_workers=2) as pool:
        attempts = [pool.submit(subprocess.run, command + ['user', 'disable', name], capture_output=True, text=True, timeout=15) for name in ['tester', backup]]
        replies = [attempt.result() for attempt in attempts]
    assert sorted(reply.returncode == 0 for reply in replies) == [False, True]
    assert db.execute("SELECT count(*) FROM web_users WHERE enabled AND role='admin'").fetchone()[0] == 1
    cli('user', 'enable', 'tester')
    cli('user', 'disable', backup)
    admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
    own = admin.get(url + '/api/me').json()['id']
    for method, body in [('delete', None), ('patch', {'role': 'member'}), ('patch', {'enabled': False})]:
        reply = getattr(admin, method)(url + '/api/users/' + own, **({'json': body} if body else {}))
        assert reply.status_code == 409 and reply.json()['code'] == 'LastAdministrator', reply.text
    assert admin.get(url + '/api/session').status_code == 200
    print('PASS concurrent last-administrator invariant across Web and CLI', flush=True)
finally:
    cli('user', 'enable', 'tester')
    if isolated_admins:
        db.execute('UPDATE web_users SET enabled=true WHERE id=ANY(%s)', (other_admins,))
    for user in created:
        cli('user', 'delete', user['username'])
    db.close()
