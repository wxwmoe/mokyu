"""Maintenance policies, publishing barriers, confirmed actions and administrator boundaries."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
import psycopg
import requests
from integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))
def login(name, password):
    client = requests.Session()
    response = client.post(url + '/api/login', headers={'Origin': url}, json={'username': name, 'password': password})
    assert response.status_code == 200, response.text
    client.headers.update({'Origin': url, 'X-CSRF-Token': response.json()['csrf_token']})
    return client
admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
assert admin.post(url + '/api/me/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204
def post(path, body=None, status=200):
    response = admin.post(url + path, json=body, timeout=50)
    assert response.status_code == status, (path, response.status_code, response.text)
    return response.json() if status != 204 else None
def wait(task, state='completed'):
    for _ in range(600):
        result = admin.get(url + '/api/tasks/' + task, timeout=5)
        assert result.status_code == 200, result.text
        job = result.json()
        if job['state'] == state:
            return job
        assert job['state'] not in ['failed', 'paused'], job
        time.sleep(.1)
    raise AssertionError('task did not finish')

before = admin.get(url + '/api/maintenance').json()
assert len(before['controls']) == 7, before
fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'pack-before-publish'
key = 'maintenance-' + uuid.uuid4().hex
user = None
writing = None
try:
    for policy in before['controls']:
        post('/api/maintenance/' + policy['kind'] + '/actions', {'action': 'pause'})
    assert cli('maintenance', 'status')['controls'][0]['paused']
    cli('maintenance', 'resume', 'cleanup')
    cleanup = post('/api/maintenance/cleanup/actions', {'action': 'run'})['task_id']
    done = wait(cleanup)
    assert done['created_by'] == 'tester' and done['source'] == 'web' and done['started_at']
    assert isinstance(done['processed'], str)
    post('/api/maintenance/cleanup/actions', {'action': 'pause'})
    name, password = key[:28], uuid.uuid4().hex
    user = post('/api/users', {'username': name, 'password': password, 'role': 'member', 'must_change_password': False}, 201)['id']
    member = login(name, password)
    for path in ['/api/maintenance','/api/tasks','/api/service/config','/api/service/status']:
        assert member.get(url + path).status_code == 403, path
    assert member.post(url + '/api/service/key-material').status_code == 403
    fields = admin.get(url + '/api/service/config').json()
    assert fields and all(f['value'] == '[redacted]' for f in fields if f['key'] in ['database.password','backend.access_key','backend.secret_key','security.credential_key_file'])
    assert any(f['source'] == 'computed' for f in fields)
    material = post('/api/service/key-material')['secret']
    assert len(material) == 64 and material not in admin.get(url + '/api/audit?limit=5').text
    print('PASS seven shared policies, task provenance, scoped denial, redacted configuration and fresh key material', flush=True)

    post('/api/maintenance/pack-creation', {'enabled': True}, 204)
    post('/api/maintenance/unpack/preview', {'all': True}, 409)
    raw = os.urandom(12 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    fault.with_suffix('.hit').unlink(missing_ok=True)
    fault.touch()
    post('/api/maintenance/pack/actions', {'action': 'resume'})
    task = post('/api/maintenance/pack/actions', {'action': 'run'})['task_id']
    for _ in range(600):
        if fault.with_suffix('.hit').exists():
            break
        time.sleep(.1)
    else:
        raise AssertionError('pack publish barrier not reached')
    post('/api/maintenance/pack-creation', {'enabled': False}, 204)
    blocked = post('/api/maintenance/unpack/preview', {'all': True})
    post('/api/maintenance/unpack/execute', {'preview_id': blocked['id'], 'confirmation': blocked['confirmation']}, 409)
    ready = db.execute("SELECT count(*) FROM packs WHERE state='ready'").fetchone()[0]
    fault.unlink()
    wait(task, 'failed')
    assert db.execute("SELECT count(*) FROM packs WHERE state='ready'").fetchone()[0] == ready
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    post('/api/maintenance/pack/actions', {'action': 'pause'})
    print('PASS stopping pack creation blocks an already prepared pack from publishing and preserves readable sources', flush=True)

    post('/api/maintenance/pack-creation', {'enabled': True}, 204)
    post('/api/maintenance/pack/actions', {'action': 'resume'})
    wait(post('/api/maintenance/pack/actions', {'action': 'run'})['task_id'])
    post('/api/maintenance/pack/actions', {'action': 'pause'})
    post('/api/maintenance/pack-creation', {'enabled': False}, 204)
    writing = db.execute("SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.key=%s", (bucket, key)).fetchone()[0]
    db.execute("UPDATE streams SET state='writing' WHERE id=%s", (writing,))
    preview = post('/api/maintenance/unpack/preview', {'all': True})
    assert preview['impact']['packs'] > 0
    post('/api/maintenance/unpack/execute', {'preview_id': preview['id'], 'confirmation': 'wrong'}, 412)
    execute = {'preview_id': preview['id'], 'confirmation': preview['confirmation']}
    task = post('/api/maintenance/unpack/execute', execute)['task_id']
    assert task == preview['id']
    wait(task)
    assert post('/api/maintenance/unpack/execute', execute)['task_id'] == task
    # Recover a lost response even if the action finished before the preview receipt committed.
    db.execute('UPDATE maintenance_previews SET task_id=NULL WHERE id=%s', (preview['id'],))
    assert post('/api/maintenance/unpack/execute', execute)['task_id'] == task
    assert not db.execute("SELECT EXISTS(SELECT 1 FROM packs WHERE state='ready' AND EXISTS(SELECT 1 FROM chunks WHERE pack_id=packs.id AND state='ready'))").fetchone()[0]
    db.execute("UPDATE streams SET state='ready' WHERE id=%s", (writing,))
    writing = None
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    print('PASS typed confirmation, all-pack drain, retained sources and durable replay after lost receipt', flush=True)

    post('/api/maintenance/mode', {'enabled': True}, 204)
    wait(post('/api/maintenance/flush')['task_id'])
    scan = post('/api/maintenance/sweep', {'older_than': '48h'})['task_id']
    wait(scan)
    sweep = post('/api/maintenance/sweep/preview', {'task_id': scan})
    task = post('/api/maintenance/sweep/execute', {'preview_id': sweep['id'], 'confirmation': sweep['confirmation']})['task_id']
    wait(task)
    post('/api/maintenance/mode', {'enabled': False}, 204)
    check = post('/api/integrity', {'mode': 'metadata', 'bucket': bucket, 'key': key})['task_id']
    result = wait(check)
    assert result['detail']['issues'] == 0, result
    issues = admin.get(url + f'/api/tasks/{check}/issues').json()
    assert not issues['issues'] and 'groups' in issues
    assert admin.get(url + f'/api/tasks/{check}/report').text.rstrip().endswith('}')
    jobs = admin.get(url + '/api/tasks?kind=integrity&limit=1').json()
    assert jobs['tasks'] and jobs['tasks'][0]['policy'] == 'integrity'
    print('PASS maintenance flush, bound sweep confirmation, inspection, report and task filters', flush=True)
finally:
    if writing:
        db.execute("UPDATE streams SET state='ready' WHERE id=%s", (writing,))
    fault.unlink(missing_ok=True)
    post('/api/maintenance/mode', {'enabled': False}, 204)
    if not db.execute("SELECT EXISTS(SELECT 1 FROM tasks WHERE kind='unpack' AND detail->>'all'='true' AND state<>'completed')").fetchone()[0]:
        post('/api/maintenance/pack-creation', {'enabled': before['pack_creation_enabled']}, 204)
    for policy in before['controls']:
        post('/api/maintenance/' + policy['kind'] + '/actions', {'action': 'pause' if policy['paused'] else 'resume'})
    if user:
        admin.delete(url + '/api/users/' + user)
    s3.delete_object(Bucket=bucket, Key=key)
