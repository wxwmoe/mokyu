"""First-admin setup on a separate disposable database and backend prefix."""
import json
import os
import signal
import subprocess
import time
import tomllib
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from psycopg import sql
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
source = Path(os.environ['MOKYU_TEST_CONFIG'])
config = tomllib.loads(source.read_text())
suffix = uuid.uuid4().hex[:12]
database = 'setup_' + suffix
root = source.parent / ('setup-' + suffix)
root.mkdir(mode=0o700)
config['database']['name'] = database
config['instance'].update(listen_s3='127.0.0.1:19200', listen_web='127.0.0.1:19201', listen_control='127.0.0.1:19202', data_path=str(root / 'data'), name='setup-' + suffix)
url = 'http://127.0.0.1:19202'
origin = 'http://localhost:9002'
config_path = source.parent / ('setup-' + suffix + '.toml')
config_path.touch(mode=0o600)
config_path.write_text('\n'.join('[' + section + ']\n' + '\n'.join(key + ' = ' + json.dumps(value) for key, value in values.items()) for section, values in config.items()))
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
db.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
process = None
try:
    with (root / 'service.log').open('wb') as log:
        process = subprocess.Popen([os.environ['MOKYU_TEST_BINARY'], '--config', str(config_path)], stdout=log, stderr=log)
    deadline = time.monotonic() + 30
    response = None
    while time.monotonic() < deadline:
        assert process.poll() is None, 'isolated setup service exited'
        try:
            response = requests.get(url + '/api/v1/auth/bootstrap', timeout=1)
            if response.status_code == 200:
                break
        except (requests.ConnectionError, requests.Timeout):
            pass
        time.sleep(.05)
    assert response is not None and response.json() == {'setup_required': True}
    member = subprocess.run([os.environ['MOKYU_TEST_BINARY'], '--config', str(config_path), 'cli', 'user', 'create', 'first-member', '--role', 'member', '--stdin'],
                            input='Temporary-member-password\n', capture_output=True, text=True)
    assert member.returncode != 0 and member.stderr.strip() == 'Error: Conflict'
    assert requests.get(url + '/api/v1/auth/bootstrap').json() == {'setup_required': True}
    issued = subprocess.run([os.environ['MOKYU_TEST_BINARY'], '--config', str(config_path), 'cli', 'setup', 'token'], capture_output=True, text=True, check=True, timeout=15)
    secret = json.loads(issued.stdout)['token']
    assert len(secret) == 64
    body = {'token': secret, 'username': 'first-admin', 'password': 'First-account-' + uuid.uuid4().hex}
    assert requests.post(url + '/api/v1/auth/setup', json=body).status_code == 403
    assert requests.post(url + '/api/v1/auth/setup', headers={'Origin': origin}, json={**body, 'token': '0' * 64}).status_code == 403
    # Expiry must be checked after waiting for the initialization lock.
    with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), dbname=database, autocommit=True) as guard:
        guard.execute('SELECT pg_advisory_lock(%s)', (734922709851002,))
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(requests.post, url + '/api/v1/auth/setup', headers={'Origin': origin}, json=body, timeout=15)
            try:
                deadline = time.monotonic() + 10
                while not guard.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory')").fetchone()[0]:
                    assert not pending.done(), 'setup did not wait for initialization lock'
                    assert time.monotonic() < deadline, 'setup lock wait was not observed'
                    time.sleep(.02)
                guard.execute("UPDATE setup_tokens SET expires_at=clock_timestamp()-interval '1 microsecond'")
            finally:
                guard.execute('SELECT pg_advisory_unlock(%s)', (734922709851002,))
            assert pending.result().status_code == 403, 'expired token was accepted after waiting for initialization lock'
    assert requests.get(url + '/api/v1/auth/bootstrap').json() == {'setup_required': True}
    issued = subprocess.run([os.environ['MOKYU_TEST_BINARY'], '--config', str(config_path), 'cli', 'setup', 'token'], capture_output=True, text=True, check=True, timeout=15)
    body['token'] = json.loads(issued.stdout)['token']
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(lambda _: requests.post(url + '/api/v1/auth/setup', headers={'Origin': origin}, json=body, timeout=10), range(2)))
    assert sorted(r.status_code for r in replies) == [201, 403]
    assert requests.get(url + '/api/v1/auth/bootstrap').json() == {'setup_required': False}
    retired = subprocess.run([os.environ['MOKYU_TEST_BINARY'], '--config', str(config_path), 'cli', 'setup', 'token'], capture_output=True, text=True, timeout=15)
    assert retired.returncode != 0
    assert requests.post(url + '/api/v1/auth/setup', headers={'Origin': origin}, json=body).status_code == 403
    assert requests.post(url + '/api/v1/auth/login', headers={'Origin': origin}, json={k: body[k] for k in ('username', 'password')}).status_code == 200
    assert all(token not in (root / 'service.log').read_text() for token in (secret, body['token']))
    print('PASS private one-time setup, expiry after lock wait, fresh migration chain, origin, concurrent claims and token retirement', flush=True)
finally:
    if process and process.poll() is None:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=15)
    db.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
    db.close()
