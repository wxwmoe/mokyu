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
config['listen'].update(s3='127.0.0.1:19200', web='127.0.0.1:19201', manage='127.0.0.1:19202', admin_socket=str(root / 'admin.sock'))
config['storage']['data'] = str(root / 'data')
config['backend']['prefix'] = 'setup-' + suffix
url = 'http://127.0.0.1:19202'
config['manage'].update(origin=url, secure_cookie=False)
config_path = source.parent / ('setup-' + suffix + '.toml')
config_path.touch(mode=0o600)
config_path.write_text('\n'.join('[' + section + ']\n' + '\n'.join(key + ' = ' + json.dumps(value) for key, value in values.items()) for section, values in config.items()))
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
db.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
process = None
try:
    with (root / 'service.log').open('wb') as log:
        process = subprocess.Popen([os.environ['MOKYU_TEST_BINARY'], '--config', str(config_path), 'serve'], stdout=log, stderr=log)
    deadline = time.monotonic() + 30
    response = None
    while time.monotonic() < deadline:
        assert process.poll() is None, (root / 'service.log').read_text()[-3000:]
        try:
            response = requests.get(url + '/api/bootstrap', timeout=1)
            if response.status_code == 200:
                break
        except requests.ConnectionError:
            pass
        time.sleep(.05)
    assert response is not None and response.json() == {'setup_required': True}
    secret = (root / 'setup-token').read_text()
    assert len(secret) == 64 and (root / 'setup-token').stat().st_mode & 0o077 == 0
    body = {'token': secret, 'username': 'first-admin', 'password': 'First-account-' + uuid.uuid4().hex}
    assert requests.post(url + '/api/setup', json=body).status_code == 403
    assert requests.post(url + '/api/setup', headers={'Origin': url}, json={**body, 'token': '0' * 64}).status_code == 403
    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(lambda _: requests.post(url + '/api/setup', headers={'Origin': url}, json=body, timeout=10), range(2)))
    assert sorted(r.status_code for r in replies) == [201, 403]
    assert requests.get(url + '/api/bootstrap').json() == {'setup_required': False}
    assert not (root / 'setup-token').exists()
    assert requests.post(url + '/api/setup', headers={'Origin': url}, json=body).status_code == 403
    assert requests.post(url + '/api/login', headers={'Origin': url}, json={k: body[k] for k in ('username', 'password')}).status_code == 200
    assert secret not in (root / 'service.log').read_text()
    print('PASS private one-time setup, fresh migration chain, origin, concurrent claims and token retirement', flush=True)
finally:
    if process and process.poll() is None:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=15)
    db.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
    db.close()
