"""Runtime settings and administrative operations follow the actual database backend."""
import json
import os
import subprocess
import time
import tomllib
from pathlib import Path

import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
config = tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())
cluster = config['database'].get('type', 'postgres') == 'postgres'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
url = os.environ['MOKYU_TEST_WEB']
admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url},
                   json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=15)
reply.raise_for_status()
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
assert admin.post(url + '/api/v1/account/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204


def cli(*args, success=True):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=30)
    assert (result.returncode == 0) == success, result.stderr
    return json.loads(result.stdout) if success else None


assert cli('status')['role'] == 'single'
uploads = admin.get(url + '/api/v1/runtime-settings/uploads').json()
fields = {row['key'] for row in admin.get(url + '/api/v1/system/config').json()}
cli_uploads = cli('settings', 'get', 'uploads')
for key in ['staging_storage_id', 'staging_bytes_limit']:
    assert (key in uploads['value']) == cluster
    assert (key in cli_uploads) == cluster
    assert ('uploads.' + key in fields) == cluster
    if not cluster:
        reply = admin.put(url + '/api/v1/runtime-settings/uploads',
                          json={'value': {key: None}, 'revision': uploads['revision']})
        assert reply.status_code == 400 and reply.json()['code'] == 'InvalidArgument'
        assert admin.get(url + '/api/v1/runtime-settings/uploads').json()['revision'] == uploads['revision']

for path in ['status', 'nodes']:
    reply = admin.get(url + '/api/v1/cluster/' + path)
    assert reply.status_code == (200 if cluster else 501), reply.status_code
    if not cluster:
        assert reply.json()['code'] == 'NotImplemented'
    cli('cluster', path, success=cluster)
if not cluster:
    for path, payload in [('mode', {'mode': 'cluster'}), ('nodes/absent/release', {'confirm': True})]:
        reply = admin.post(url + '/api/v1/cluster/' + path, json=payload)
        assert reply.status_code == 501 and reply.json()['code'] == 'NotImplemented'
    reply = requests.post(url + '/api/v1/cluster/recover', json={'confirm': True},
                          headers={'X-Mokyu-Deployment-Key': config['instance']['deployment_key']})
    assert reply.status_code == 501 and reply.json()['code'] == 'NotImplemented'
    cli('cluster', 'mode', 'cluster', success=False)
print('PASS backend-specific settings and cluster API/CLI capability boundary', flush=True)

job = cli('integrity', 'check', '--mode', 'metadata')['task_id']
for _ in range(600):
    task = cli('task', 'show', job)
    if task['state'] in ('completed', 'failed', 'paused'):
        break
    time.sleep(.2)
assert task['state'] == 'completed', task['state']
internal = {'parent_task_id', 'execution_scope', 'target_node_id', 'lease_node_id',
            'lease_token', 'lease_expires_at', 'batch_cursor', 'batch_total', 'batch_completed'}
assert not internal & task.keys()
assert 'role' not in task['detail'] and 'children' not in task['detail']
assert all(not internal & row.keys() for row in cli('task', 'list'))
response = admin.get(url + '/api/v1/tasks/' + job)
response.raise_for_status()
assert not internal & response.json().keys()
response = admin.get(url + '/api/v1/tasks/' + job + '/report')
response.raise_for_status()
report = [json.loads(line) for line in response.iter_lines()]
assert report[0]['task']['id'] == job and not internal & report[0]['task'].keys()
assert report[-1]['type'] == 'end'
print('PASS single task CLI, API and direct report omit cluster ownership fields', flush=True)
