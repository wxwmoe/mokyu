"""Account preferences, session revocation and in-flight password changes."""
import hashlib
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
password = 'Account-test-' + uuid.uuid4().hex
username = 'account-' + uuid.uuid4().hex[:12]


def cli(*args, secret=None):
    result = subprocess.run(command + list(args), input=secret, text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def login(secret=password):
    session = requests.Session()
    response = session.post(url + '/api/login', headers={'Origin': url}, json={'username': username, 'password': secret}, timeout=10)
    assert response.status_code == 200, response.status_code
    session.headers.update({'Origin': url, 'X-CSRF-Token': response.json()['csrf_token']})
    return session


db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
user = cli('user', 'create', username, '--password-stdin', secret=password + '\n')['id']
try:
    first, second = login(), login()
    assert first.get(url + '/api/bootstrap').json() == {'setup_required': False}
    assert requests.get(url + '/api/me').status_code == 403
    profile = first.get(url + '/api/me').json()
    assert profile['locale'] is None and not profile['avatar_enabled'] and profile['avatar_url'] is None
    preferences = dict(display_name='Mochi keeper', locale='ja', theme='dark', avatar_email=' AVATAR@example.test ', avatar_enabled=True)
    response = first.put(url + '/api/me', json=preferences)
    assert response.status_code == 200, response.text
    saved = second.get(url + '/api/session').json()
    expected = hashlib.sha256(b'avatar@example.test').hexdigest()
    assert saved['display_name'] == 'Mochi keeper' and saved['locale'] == 'ja' and saved['theme'] == 'dark'
    assert saved['avatar_email'] == 'avatar@example.test' and saved['avatar_url'].endswith('/' + expected + '?s=128&r=g&d=404')
    for invalid in [dict(locale='fr'), dict(theme='system'), dict(display_name='x' * 241), dict(display_name='a\nb'), dict(avatar_email='invalid'), dict(avatar_email='')]:
        assert first.put(url + '/api/me', json={**preferences, **invalid}).status_code == 400
    assert first.put(url + '/api/me', json={**preferences, 'unknown': True}).status_code == 422
    assert first.put(url + '/api/me', headers={'X-CSRF-Token': 'wrong'}, json=preferences).status_code == 403
    assert first.put(url + '/api/me', json={**preferences, 'locale': None, 'theme': None, 'avatar_enabled': False}).json()['avatar_url'] is None
    sessions = first.get(url + '/api/me/sessions').json()['sessions']
    assert len(sessions) == 2 and sum(s['current'] for s in sessions) == 1
    assert all('token_hash' not in s and 'csrf_hash' not in s for s in sessions)
    other = next(s['id'] for s in sessions if not s['current'])
    foreign = db.execute('SELECT id FROM sessions WHERE user_id<>%s LIMIT 1', (user,)).fetchone()[0]
    assert first.delete(url + '/api/me/sessions/' + str(foreign)).status_code == 204
    assert db.execute('SELECT count(*) FROM sessions WHERE id=%s', (foreign,)).fetchone()[0] == 1
    assert first.delete(url + '/api/me/sessions/' + other).status_code == 204
    assert second.get(url + '/api/session').status_code == 403
    second = login()
    assert first.post(url + '/api/me/reauth', json={'password': 'incorrect'}).status_code == 403
    assert first.post(url + '/api/me/reauth', json={'password': password}).status_code == 204
    new = 'New-account-' + uuid.uuid4().hex
    assert first.post(url + '/api/me/password', json={'current_password': password, 'new_password': new}).status_code == 204
    assert first.get(url + '/api/session').status_code == 200
    assert second.get(url + '/api/session').status_code == 403
    second = login(new)
    assert first.delete(url + '/api/me/sessions').status_code == 204
    assert second.get(url + '/api/session').status_code == 403
    assert first.get(url + '/api/session').status_code == 200
    print('PASS nullable preferences, avatar hashing, validation, CSRF, own sessions and password rotation', flush=True)

    # Password verification occurs outside the row lock. A concurrent reset must win
    # over a verification result obtained before the reset committed.
    with ThreadPoolExecutor(max_workers=1) as pool:
        with db.transaction():
            db.execute('SELECT id FROM web_users WHERE id=%s FOR UPDATE', (user,))
            pending = pool.submit(first.post, url + '/api/me/password', json={'current_password': new, 'new_password': password}, timeout=15)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                db.execute('SELECT pg_stat_clear_snapshot()')
                assert not pending.done(), pending.result().status_code if pending.done() else ''
                waiting = db.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT password_hash,auth_revision,enabled FROM web_users%')").fetchone()[0]
                if waiting:
                    break
                time.sleep(.02)
            assert waiting, 'password update did not reach its transaction guard'
            db.execute('UPDATE web_users SET auth_revision=auth_revision+1 WHERE id=%s', (user,))
            db.execute('DELETE FROM sessions WHERE user_id=%s', (user,))
        assert pending.result().status_code == 403
    assert login(new).get(url + '/api/me').status_code == 200
    print('PASS a password reset invalidates an already verified in-flight change', flush=True)
finally:
    cli('user', 'delete', username)
    db.close()
