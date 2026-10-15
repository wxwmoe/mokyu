"""Maintenance policies, publishing barriers, confirmed actions and administrator boundaries."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import psycopg
import requests
from tests.integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))
def login(name, password):
    client = requests.Session()
    response = client.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={'username': name, 'password': password})
    assert response.status_code == 200, response.text
    client.headers.update({'Origin': url, 'X-CSRF-Token': response.json()['csrf_token']})
    return client
admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
assert admin.post(url + '/api/v1/account/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204
def post(path, body=None, status=200):
    response = admin.post(url + path, json=body, timeout=50)
    assert response.status_code == status, (path, response.status_code, response.text)
    return response.json() if status != 204 else None
def wait(task, state='completed'):
    for _ in range(600):
        result = admin.get(url + '/api/v1/tasks/' + task, timeout=5)
        assert result.status_code == 200, result.text
        job = result.json()
        if job['status'] == state:
            return job
        assert job['status'] not in ['failed', 'paused'], job
        time.sleep(.1)
    raise AssertionError('task did not finish')

before = admin.get(url + '/api/v1/maintenance').json()
assert len(before['controls']) == 7, before
fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'pack-before-publish'
key = 'maintenance-' + uuid.uuid4().hex
user = None
writing = None
try:
    for policy in before['controls']:
        post('/api/v1/maintenance/' + policy['policy'] + '/actions', {'action': 'pause'})
    assert cli('maintenance', 'status')['controls'][0]['paused']
    cli('maintenance', 'resume', 'cleanup')
    cleanup = post('/api/v1/maintenance/cleanup/actions', {'action': 'run'})['task_id']
    done = wait(cleanup)
    assert done['created_by'] == 'tester' and done['source'] == 'web', done
    assert db.execute('SELECT bool_and(started_at IS NOT NULL) FROM tasks WHERE parent_task_id=%s', (cleanup,)).fetchone()[0]
    assert isinstance(done['processed'], str)
    post('/api/v1/maintenance/cleanup/actions', {'action': 'pause'})
    name, password = key[:28], uuid.uuid4().hex
    user = post('/api/v1/users', {'username': name, 'password': password, 'role': 'member', 'must_change_password': False}, 201)['id']
    member = login(name, password)
    for path in ['/api/v1/maintenance','/api/v1/tasks','/api/v1/system/config','/api/v1/system/status']:
        assert member.get(url + path).status_code == 403, path
    assert member.get(url + '/api/v1/keys').status_code == 403
    assert member.post(url + '/api/v1/keys', json={'name': 'denied', 'purpose': 'access'}).status_code == 403
    fields = admin.get(url + '/api/v1/system/config').json()
    for secret_field in ['database.password', 'instance.deployment_key']:
        assert any(f['key'] == secret_field and f['value'] == '[redacted]' for f in fields)
    assert any(f['origin'] == 'config' and f['value_mode'] == 'budget' for f in fields)
    managed = post('/api/v1/keys', {'name': 'maintenance-' + uuid.uuid4().hex, 'purpose': 'access'}, 201)
    assert managed['purpose'] == 'access' and managed['algorithm'] == 'raw' and not managed['in_use']
    assert 'secret' not in managed and 'secret_encrypted' not in managed
    listed = admin.get(url + '/api/v1/keys').json()
    assert any(row['id'] == managed['id'] and row['fingerprint'] == managed['fingerprint'] for row in listed)
    assert admin.delete(url + '/api/v1/keys/' + managed['id']).status_code == 204
    print('PASS seven shared policies, task provenance, scoped denial, redacted configuration and database key metadata', flush=True)

    post('/api/v1/maintenance/pack-creation', {'enabled': True}, 204)
    post('/api/v1/maintenance/unpack/preview', {'all': True}, 409)
    raw = os.urandom(12 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    fault.with_suffix('.hit').unlink(missing_ok=True)
    fault.touch()
    post('/api/v1/maintenance/pack/actions', {'action': 'resume'})
    task = post('/api/v1/maintenance/pack/actions', {'action': 'run'})['task_id']
    for _ in range(600):
        if fault.with_suffix('.hit').exists():
            break
        time.sleep(.1)
    else:
        raise AssertionError('pack publish barrier not reached')
    post('/api/v1/maintenance/pack-creation', {'enabled': False}, 204)
    blocked = post('/api/v1/maintenance/unpack/preview', {'all': True})
    post('/api/v1/maintenance/unpack/execute', {'preview_id': blocked['id'], 'confirmation': blocked['confirmation']}, 409)
    ready = db.execute("SELECT count(*) FROM packs WHERE state='ready'").fetchone()[0]
    fault.unlink()
    wait(task, 'failed')
    assert db.execute("SELECT count(*) FROM packs WHERE state='ready'").fetchone()[0] == ready
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    post('/api/v1/maintenance/pack/actions', {'action': 'pause'})
    print('PASS stopping pack creation blocks an already prepared pack from publishing and preserves readable sources', flush=True)

    post('/api/v1/maintenance/pack-creation', {'enabled': True}, 204)
    post('/api/v1/maintenance/pack/actions', {'action': 'resume'})
    wait(post('/api/v1/maintenance/pack/actions', {'action': 'run'})['task_id'])
    post('/api/v1/maintenance/pack/actions', {'action': 'pause'})
    post('/api/v1/maintenance/pack-creation', {'enabled': False}, 204)
    page = admin.get(url + '/api/v1/packs', params={'limit': 1})
    assert page.status_code == 200 and len(page.json()['packs']) == 1, page.text
    packed = str(db.execute("SELECT id FROM packs WHERE state='ready' ORDER BY id DESC LIMIT 1").fetchone()[0])
    detail = admin.get(url + '/api/v1/packs/' + packed)
    assert detail.status_code == 200 and detail.json()['pack']['id'] == packed, detail.text
    members = detail.json()['members']
    expected = db.execute('SELECT count(*) FROM pack_members WHERE pack_id=%s', (packed,)).fetchone()[0]
    assert len(members) == min(expected, 4096)
    assert [row['ordinal'] for row in members] == sorted(row['ordinal'] for row in members)
    assert admin.get(url + '/api/v1/packs/0').status_code == 404
    assert admin.get(url + '/api/v1/packs', params={'limit': 0}).status_code == 400
    print('PASS pack listing and detail preserve string IDs, ordinal ordering, bounds and missing-row errors', flush=True)
    writing = db.execute("SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s", (bucket, key)).fetchone()[0]
    db.execute("UPDATE streams SET state='writing' WHERE id=%s", (writing,))
    preview = post('/api/v1/maintenance/unpack/preview', {'all': True})
    assert preview['impact']['packs'] > 0
    post('/api/v1/maintenance/unpack/execute', {'preview_id': preview['id'], 'confirmation': 'wrong'}, 412)
    execute = {'preview_id': preview['id'], 'confirmation': preview['confirmation']}
    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda _: post('/api/v1/maintenance/unpack/execute', execute), range(2)))
    assert results[0]['task_id'] == results[1]['task_id'], results
    task = results[0]['task_id']
    assert task == preview['id']
    wait(task)
    assert post('/api/v1/maintenance/unpack/execute', execute)['task_id'] == task
    assert member.post(url + '/api/v1/maintenance/unpack/execute', json=execute).status_code == 403
    # Recover a lost response even if the action finished before the preview receipt committed.
    db.execute('UPDATE maintenance_previews SET task_id=NULL WHERE id=%s', (preview['id'],))
    assert post('/api/v1/maintenance/unpack/execute', execute)['task_id'] == task
    assert not db.execute("SELECT EXISTS(SELECT 1 FROM packs WHERE state='ready' AND EXISTS(SELECT 1 FROM chunks WHERE pack_id=packs.id AND state='ready'))").fetchone()[0]
    db.execute("UPDATE streams SET state='ready' WHERE id=%s", (writing,))
    writing = None
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    db.execute("UPDATE maintenance_previews SET expires_at=now()-interval '1 second' WHERE id=%s", (preview['id'],))
    post('/api/v1/maintenance/unpack/execute', execute, 409)
    print('PASS typed confirmation, concurrent execution, expiry, scoped denial, all-pack drain and durable replay after lost receipt', flush=True)

    post('/api/v1/maintenance/mode', {'enabled': True}, 204)
    wait(post('/api/v1/maintenance/flush')['task_id'])
    scan = post('/api/v1/maintenance/sweep', {'older_than': '48h'})['task_id']
    wait(scan)
    sweep = post('/api/v1/maintenance/sweep/preview', {'task_id': scan})
    task = post('/api/v1/maintenance/sweep/execute', {'preview_id': sweep['id'], 'confirmation': sweep['confirmation']})['task_id']
    wait(task)
    post('/api/v1/maintenance/mode', {'enabled': False}, 204)
    check = post('/api/v1/integrity', {'mode': 'metadata', 'bucket': bucket, 'key': key})['task_id']
    result = wait(check)
    children = db.execute('SELECT id FROM tasks WHERE parent_task_id=%s', (check,)).fetchall()
    assert len(children) == 1, children
    check = str(children[0][0])
    result = wait(check)
    assert result['detail']['issues'] == 0, result
    issues = admin.get(url + f'/api/v1/tasks/{check}/issues').json()
    assert not issues['items'] and 'groups' in issues
    assert admin.get(url + f'/api/v1/tasks/{check}/report').text.rstrip().endswith('}')
    jobs = admin.get(url + '/api/v1/tasks?policy=integrity&limit=1').json()
    assert jobs['items'] and jobs['items'][0]['policy'] == 'integrity'
    assert jobs['next_cursor'], jobs
    following = admin.get(url + '/api/v1/tasks', params={'policy': 'integrity', 'limit': 1, 'token': jobs['next_cursor']})
    assert following.status_code == 200 and following.json()['items'][0]['id'] != jobs['items'][0]['id'], following.text
    assert admin.get(url + '/api/v1/tasks', params={'policy': 'cleanup', 'token': jobs['next_cursor']}).status_code == 400
    assert admin.get(url + '/api/v1/tasks', params={'state': 'invalid'}).status_code == 400
    assert admin.get(url + '/api/v1/tasks/' + str(uuid.uuid4())).status_code == 404
    print('PASS maintenance flush, bound sweep confirmation, inspection, report and task filters', flush=True)
finally:
    if writing:
        db.execute("UPDATE streams SET state='ready' WHERE id=%s", (writing,))
    fault.unlink(missing_ok=True)
    post('/api/v1/maintenance/mode', {'enabled': False}, 204)
    if not db.execute("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='unpack' AND detail->>'all'='true' AND state<>'completed')").fetchone()[0]:
        post('/api/v1/maintenance/pack-creation', {'enabled': before['pack_creation_enabled']}, 204)
    for policy in before['controls']:
        post('/api/v1/maintenance/' + policy['policy'] + '/actions', {'action': 'pause' if policy['paused'] else 'resume'})
    if user:
        admin.delete(url + '/api/v1/users/' + user)
    s3.delete_object(Bucket=bucket, Key=key)
