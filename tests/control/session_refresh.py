"""Refresh rotation, concurrent replies, replay fencing and login/reset ordering."""
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
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
username = 'refresh-' + uuid.uuid4().hex[:12]
password = 'Refresh-test-' + uuid.uuid4().hex
markers = []


def cli(*args, password=None):
    result = subprocess.run(command + list(args), input=password, text=True, capture_output=True, timeout=20, check=True)
    return json.loads(result.stdout)


def login(secret=password):
    session = requests.Session()
    response = session.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={'username': username, 'password': secret}, timeout=20)
    if response.status_code == 200:
        session.headers.update({'Origin': url, 'X-CSRF-Token': response.json()['csrf_token']})
    return session, response


def refresh(secret):
    return requests.post(url + '/api/v1/auth/refresh', headers={'Origin': url}, json={'refresh_token': secret}, timeout=20)


def wait(check):
    deadline = time.monotonic() + 10
    while not check():
        assert time.monotonic() < deadline, 'authentication boundary timed out'
        time.sleep(.02)


user = cli('user', 'create', username, '--stdin', password=password + '\n')['id']
try:
    session, response = login()
    assert response.status_code == 200
    secret = response.json()['refresh_token']
    session_id = session.get(url + '/api/v1/auth/session').json()['session_id']
    db.execute("UPDATE sessions SET expires_at=now()-interval '1 second',reauthenticated_at=now()-interval '6 minutes' WHERE id=%s", (session_id,))
    db.execute("UPDATE session_refresh_tokens SET expires_at=now()+interval '6 days' WHERE session_id=%s", (session_id,))
    assert session.get(url + '/api/v1/auth/session').status_code == 403
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(refresh, [secret, secret]))
    assert all(reply.status_code == 200 for reply in replies)
    rotated = replies[0].json()['refresh_token']
    assert rotated != secret and all(reply.json()['refresh_token'] == rotated for reply in replies)
    assert replies[0].json()['csrf_token'] == replies[1].json()['csrf_token']
    assert replies[0].cookies.get('mokyu_session') == replies[1].cookies.get('mokyu_session')
    assert all(reply.json()['session_id'] == session_id for reply in replies)
    assert db.execute('SELECT count(*),count(*) FILTER (WHERE revoked_at IS NULL) FROM session_refresh_tokens WHERE session_id=%s', (session_id,)).fetchone() == (2, 1)
    assert db.execute("SELECT reauthenticated_at<now()-interval '5 minutes' FROM sessions WHERE id=%s", (session_id,)).fetchone()[0]
    assert refresh(rotated).status_code == 200
    db.execute("UPDATE session_refresh_tokens SET revoked_at=now()-interval '31 seconds' WHERE session_id=%s AND revoked_at IS NOT NULL", (session_id,))
    assert refresh(secret).status_code == 403
    assert refresh(rotated).status_code == 403
    assert db.execute('SELECT count(*) FROM sessions WHERE id=%s', (session_id,)).fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM audit_events WHERE actor_id=%s AND action='session.refresh_reuse'", (user,)).fetchone()[0] == 1
    print('PASS expired access recovery, concurrent rotation, bounded replay and preserved reauthentication', flush=True)

    session, response = login()
    assert response.status_code == 200
    old = response.json()['refresh_token']
    response = session.post(url + '/api/v1/account/reauth', json={'password': password})
    assert response.status_code == 204
    replacement = response.headers['X-Mokyu-Refresh-Token']
    assert refresh(old).status_code == 403
    assert session.get(url + '/api/v1/auth/session').status_code == 200
    assert refresh(replacement).status_code == 200
    print('PASS reauthentication retires the old family without revoking its replacement', flush=True)

    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    with ThreadPoolExecutor(max_workers=2) as pool:
        for action in ('password', 'disable'):
            marker = faults / 'login-verified'
            markers.append(marker)
            marker.with_suffix('.hit').unlink(missing_ok=True)
            marker.touch()
            pending = pool.submit(login)
            try:
                wait(lambda: marker.with_suffix('.hit').exists())
                cli('user', action, username, *(['--stdin'] if action == 'password' else []), password=password + '\n' if action == 'password' else None)
            finally:
                marker.unlink(missing_ok=True)
            assert pending.result()[1].status_code == 403
            assert db.execute('SELECT count(*) FROM sessions WHERE user_id=%s', (user,)).fetchone()[0] == 0
            if action == 'disable':
                cli('user', 'enable', username)

        marker = faults / 'login-before-session'
        markers.append(marker)
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        pending = pool.submit(login)
        try:
            wait(lambda: marker.with_suffix('.hit').exists())
            resetting = pool.submit(cli, 'user', 'password', username, '--stdin', password=password + '\n')
            wait(lambda: db.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'UPDATE web_users SET password_hash%')").fetchone()[0])
        finally:
            marker.unlink(missing_ok=True)
        session, response = pending.result()
        assert response.status_code == 200
        resetting.result()
        assert session.get(url + '/api/v1/auth/session').status_code == 403
    print('PASS reset/disable after verification and reset after session lock both revoke access', flush=True)
finally:
    for marker in markers:
        marker.unlink(missing_ok=True)
    cli('user', 'delete', username)
    db.close()
