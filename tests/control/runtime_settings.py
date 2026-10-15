"""Revisioned settings and encrypted key mutations in an isolated PostgreSQL fixture."""
import json
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from psycopg import sql
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
prefix = 'settings-' + uuid.uuid4().hex[:10]
trigger = '_settings_test_' + uuid.uuid4().hex[:10]
document = Path('/config/' + prefix + '.json')
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


def current():
    result = admin.get(url + '/api/v1/runtime-settings/maintenance')
    assert result.status_code == 200
    return result.json()


def drop_trigger():
    db.execute(sql.SQL('DROP TRIGGER IF EXISTS {} ON audit_events').format(sql.Identifier(trigger)))
    db.execute(sql.SQL('DROP FUNCTION IF EXISTS {}()').format(sql.Identifier(trigger)))


original = current()['value']
web = cli('settings', 'web', 'show')
try:
    rows = admin.get(url + '/api/v1/runtime-settings').json()
    assert {r['section'] for r in rows} == {'web', 'requests', 'uploads', 'metrics', 'maintenance'}
    snapshot = current()
    changed = {**snapshot['value'], 'interval': '61s'}
    payload = {'value': changed, 'revision': snapshot['revision']}
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: admin.put(url + '/api/v1/runtime-settings/maintenance', json=payload), range(2)))
    assert sum(r.status_code == 200 for r in results) == 1, [r.status_code for r in results]
    saved = current()
    assert saved['value'] == changed and saved['revision'] == snapshot['revision'] + 1
    for invalid in [{**changed, 'unknown': True}, {**changed, 'interval': 'invalid'}]:
        reply = admin.put(url + '/api/v1/runtime-settings/maintenance',
                          json={'revision': saved['revision'], 'value': invalid})
        assert reply.status_code >= 400
        assert current()['revision'] == saved['revision']
    document.write_text(json.dumps({**changed, 'interval': '62s'}))
    cli('settings', 'set', 'maintenance', str(document), '--rev', str(saved['revision']))
    cli('settings', 'set', 'maintenance', str(document), '--rev', str(saved['revision']), success=False)
    assert current()['revision'] == saved['revision'] + 1
    audit = db.execute("SELECT source,outcome,detail FROM audit_events WHERE action='runtime.settings' AND target='maintenance' ORDER BY id DESC LIMIT 1").fetchone()
    assert audit[:2] == ('cli', 'succeeded') and audit[2]['revision'] == current()['revision']
    cli('settings', 'reset', 'maintenance')
    assert current()['revision'] == saved['revision'] + 2
    reply = admin.post(url + '/api/v1/runtime-settings/maintenance/reset')
    assert reply.status_code == 200 and reply.json()['revision'] == saved['revision'] + 3
    cli('settings', 'web', 'set', '--gravatar-base-url', 'https://avatars.example.test/avatar/', '--rev', str(web['revision']))
    cli('settings', 'web', 'set', '--gravatar-base-url', web['gravatar_base_url'], '--rev', str(web['revision']), success=False)
    assert cli('settings', 'web', 'show')['revision'] == web['revision'] + 1
    print('PASS settings revision races, validation, CLI parity, resets and atomic audit metadata', flush=True)

    created = []
    for suffix, purpose, algorithm in [('aes', 'block', 'aes-256-gcm'), ('chacha', 'block', 'chacha20-poly1305'), ('access', 'access', None), ('signing', 'signing', None)]:
        data = {'name': prefix + '-' + suffix, 'purpose': purpose}
        if algorithm:
            data['algorithm'] = algorithm
        reply = admin.post(url + '/api/v1/keys', json=data)
        assert reply.status_code == 201, reply.status_code
        key = reply.json()
        assert key['algorithm'] == (algorithm or 'raw') and len(key['fingerprint']) == 64
        assert not {'secret', 'secret_encrypted', 'protected', 'material'} & key.keys()
        stored = db.execute('SELECT secret_encrypted,fingerprint FROM managed_keys WHERE id=%s', (key['id'],)).fetchone()
        assert len(stored[0]) > 32 and stored[1].hex() == key['fingerprint']
        created.append(key['id'])
    listed = cli('key', 'list')
    assert set(created) <= {key['id'] for key in listed}
    used = next(key for key in listed if key['in_use'])
    reply = admin.delete(url + '/api/v1/keys/' + used['id'])
    assert reply.status_code == 400 and reply.json()['code'] == 'InvalidRequest'
    for id in created:
        assert admin.delete(url + '/api/v1/keys/' + id).status_code == 204
        assert db.execute('SELECT secret_encrypted IS NULL AND revoked_at IS NOT NULL FROM managed_keys WHERE id=%s', (id,)).fetchone()[0]
    assert admin.delete(url + '/api/v1/keys/' + str(uuid.uuid4())).status_code == 404
    for data in [{'name': '__credentials', 'purpose': 'block'}, {'name': prefix + '/bad', 'purpose': 'block'}, {'name': prefix, 'purpose': 'access', 'algorithm': 'aes-256-gcm'}]:
        assert admin.post(url + '/api/v1/keys', json=data).status_code >= 400
    print('PASS encrypted key creation, supported algorithms, secret-free views and in-use revocation fencing', flush=True)

    db.execute(sql.SQL("CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='runtime.settings' OR (NEW.action='managed_key.create' AND NEW.detail->>'name'={}) THEN RAISE EXCEPTION 'settings audit boundary'; END IF; RETURN NEW; END $$").format(sql.Identifier(trigger), sql.Literal(prefix + '-rollback')))
    db.execute(sql.SQL('CREATE TRIGGER {} BEFORE UPDATE ON audit_events FOR EACH ROW EXECUTE FUNCTION {}()').format(sql.Identifier(trigger), sql.Identifier(trigger)))
    before = current()
    document.write_text(json.dumps({**before['value'], 'interval': '63s'}))
    cli('settings', 'set', 'maintenance', str(document), success=False)
    assert current()['value'] == before['value'] and current()['revision'] == before['revision']
    assert admin.post(url + '/api/v1/runtime-settings/maintenance/reset').status_code == 500
    assert current()['revision'] == before['revision']
    assert admin.post(url + '/api/v1/keys', json={'name': prefix + '-rollback', 'purpose': 'block'}).status_code == 500
    assert db.execute('SELECT count(*) FROM managed_keys WHERE name=%s', (prefix + '-rollback',)).fetchone()[0] == 0
    print('PASS audit failure rolls back CLI settings, reset and managed key creation', flush=True)
finally:
    drop_trigger()
    document.write_text(json.dumps(original))
    cli('settings', 'set', 'maintenance', str(document))
    cli('settings', 'web', 'set', '--gravatar-base-url', web['gravatar_base_url'])
    document.unlink(missing_ok=True)
    db.close()
