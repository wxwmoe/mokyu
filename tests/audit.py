"""Durable management intents, atomic security history, redaction and scoped journal access."""
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from psycopg import sql
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
database = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text()
db = psycopg.connect(database, autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
prefix = 'audit-' + uuid.uuid4().hex[:10]
password = 'Audit-test-' + uuid.uuid4().hex
users, tokens = [], []
trigger = '_audit_test_' + uuid.uuid4().hex[:10]


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, check=True, timeout=20)
    return json.loads(result.stdout)


def login(name, secret):
    client = requests.Session()
    result = client.post(url + '/api/login', headers={'Origin': url}, json={'username': name, 'password': secret}, timeout=15)
    assert result.status_code == 200, result.status_code
    client.headers.update({'Origin': url, 'X-CSRF-Token': result.json()['csrf_token']})
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
admin.post(url + '/api/me/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']})
try:
    # The intent exists before the business transaction can acquire its administration lock.
    lock = psycopg.connect(database, autocommit=True)
    lock.execute('SELECT pg_advisory_lock(734922709851002)')
    before = db.execute('SELECT COALESCE(max(id),0) FROM audit_events').fetchone()[0]
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(admin.post, url + '/api/users', json={'username': prefix, 'password': password, 'role': 'member', 'must_change_password': False}, timeout=20)
        try:
            deadline = time.monotonic() + 10
            intent = None
            while not intent:
                intent = db.execute("SELECT id,outcome FROM audit_events WHERE id>%s AND action='POST /api/users'", (before,)).fetchone()
                assert time.monotonic() < deadline
                time.sleep(.02)
            assert intent[1] == 'unknown'
        finally:
            lock.execute('SELECT pg_advisory_unlock(734922709851002)')
            lock.close()
        reply = pending.result()
    assert reply.status_code == 201, reply.status_code
    user = reply.json()
    users.append(user['id'])
    event = db.execute('SELECT action,target,outcome,request_id,detail::text FROM audit_events WHERE id=%s', (intent[0],)).fetchone()
    assert event[:3] == ('user.create', user['id'], 'succeeded')
    assert event[3] == reply.headers['X-Request-ID'] and password not in event[4]
    print('PASS durable intent precedes mutation; completed user event shares transaction and Request ID', flush=True)

    member = login(prefix, password)
    issued = member.post(url + '/api/tokens', json={'label': prefix, 'grants': []})
    if issued.status_code == 403 and issued.json()['code'] == 'ReauthenticationRequired':
        member.post(url + '/api/me/reauth', json={'password': password})
        issued = member.post(url + '/api/tokens', json={'label': prefix, 'grants': []})
    assert issued.status_code == 201, issued.status_code
    secret = issued.json()['secret']
    token = issued.json()['token']['id']
    tokens.append(token)
    own = member.get(url + '/api/audit').json()['events']
    assert own and all(row['actor_id'] == user['id'] for row in own)
    assert member.get(url + '/api/audit/' + str(intent[0])).status_code == 404
    assert requests.get(url + '/api/audit', headers={'Authorization': 'Bearer ' + secret}).status_code == 403
    assert secret not in admin.get(url + '/api/audit', params={'actor': prefix}).text
    assert member.post(url + '/api/users', json={'username': 'blocked', 'password': password, 'role': 'admin'}).status_code == 403
    assert member.get(url + '/api/audit', params={'outcome': 'failed'}).json()['events']
    with ThreadPoolExecutor(max_workers=2) as pool:
        attempts = [pool.submit(requests.post, url + '/api/login', headers={'Origin': url}, json={'username': prefix, 'password': password + '-incorrect'}) for _ in range(2)]
        assert all(attempt.result().status_code == 403 for attempt in attempts)
    failures = db.execute("SELECT detail FROM audit_events WHERE actor_id=%s AND action='session.login' AND outcome='failed'", (user['id'],)).fetchall()
    assert len(failures) == 1 and failures[0][0]['identity_verified'] is False
    print('PASS member isolation, denied mutations, Bearer boundary and throttled unidentified sign-in attempts', flush=True)

    # If the atomic audit update fails, the key itself must roll back.
    bucket = next(item for item in admin.get(url + '/api/buckets').json() if item['name'] == 'media')
    db.execute(sql.SQL("CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='credential.create' AND NEW.detail->>'label'='{}' THEN RAISE EXCEPTION 'audit regression boundary'; END IF; RETURN NEW; END $$").format(sql.Identifier(trigger), sql.SQL(prefix)))
    db.execute(sql.SQL('CREATE TRIGGER {} BEFORE UPDATE ON audit_events FOR EACH ROW EXECUTE FUNCTION {}()').format(sql.Identifier(trigger), sql.Identifier(trigger)))
    try:
        rejected = admin.post(url + '/api/credentials', json={'label': prefix, 'project_id': bucket['project_id'], 'grants': []})
        assert rejected.status_code == 500
        assert db.execute('SELECT count(*) FROM credentials WHERE label=%s', (prefix,)).fetchone()[0] == 0
        assert db.execute('SELECT outcome FROM audit_events WHERE request_id=%s', (rejected.headers['X-Request-ID'],)).fetchone()[0] == 'failed'
    finally:
        db.execute(sql.SQL('DROP TRIGGER IF EXISTS {} ON audit_events').format(sql.Identifier(trigger)))
        db.execute(sql.SQL('DROP FUNCTION IF EXISTS {}()').format(sql.Identifier(trigger)))
    cli('token', 'revoke', token)
    assert cli('audit', '--action', 'token.revoke')['events'][0]['source'] == 'cli'
    print('PASS audit-write failure rolls back key creation; CLI mutations share the journal', flush=True)

    page = admin.get(url + '/api/audit', params={'limit': 2}).json()
    following = admin.get(url + '/api/audit', params={'limit': 2, 'after': page['next']}).json()
    assert not {row['id'] for row in page['events']} & {row['id'] for row in following['events']}
    exported = member.get(url + '/api/audit/export', params={'limit': 2})
    assert exported.status_code == 200 and exported.headers['Content-Type'].startswith('application/x-ndjson')
    assert exported.headers['X-Next-Cursor']
    assert all(json.loads(line)['actor_id'] == user['id'] for line in exported.text.splitlines())
    assert admin.get(url + '/api/audit/export', params={'limit': 101}).status_code == 400
    assert admin.get(url + '/api/audit', params={'after': '-1'}).status_code == 400
    assert admin.patch(url + '/api/users/' + user['id'], json={'role': 'admin'}).status_code == 200
    elevated = login(prefix, password)
    child = elevated.post(url + '/api/users', json={'username': prefix + '-child', 'password': password, 'role': 'member', 'must_change_password': False})
    assert child.status_code == 201
    users.append(child.json()['id'])
    restricted_history = db.execute('SELECT id FROM audit_events WHERE request_id=%s', (child.headers['X-Request-ID'],)).fetchone()[0]
    assert admin.patch(url + '/api/users/' + user['id'], json={'role': 'member'}).status_code == 200
    demoted = login(prefix, password)
    assert demoted.get(url + '/api/audit/' + str(restricted_history)).status_code == 404
    assert all(row['id'] != str(restricted_history) for row in demoted.get(url + '/api/audit').json()['events'])
    assert admin.delete(url + '/api/users/' + user['id']).status_code == 204
    history = admin.get(url + '/api/audit', params={'actor': prefix}).json()['events']
    assert history and all(row['actor_label'] == prefix for row in history)
    assert secret not in json.dumps(history) and password not in json.dumps(history)
    old = db.execute("INSERT INTO audit_events(actor_label,source,action,created_at,outcome) VALUES(%s,'cli','retention.probe',now()-interval '91 days','succeeded') RETURNING id", (prefix,)).fetchone()[0]
    cli('cleanup', 'run')
    assert db.execute('SELECT count(*) FROM audit_events WHERE id=%s', (old,)).fetchone()[0] == 0
    assert db.execute('SELECT count(*) FROM audit_events WHERE id=%s', (intent[0],)).fetchone()[0] == 1
    print('PASS pagination, bounded JSONL export, deleted-actor history, redaction and retention cleanup', flush=True)
finally:
    db.execute(sql.SQL('DROP TRIGGER IF EXISTS {} ON audit_events').format(sql.Identifier(trigger)))
    db.execute(sql.SQL('DROP FUNCTION IF EXISTS {}()').format(sql.Identifier(trigger)))
    for token in tokens:
        admin.delete(url + '/api/tokens/' + token)
    for user in users:
        admin.delete(url + '/api/users/' + user)
    db.close()
