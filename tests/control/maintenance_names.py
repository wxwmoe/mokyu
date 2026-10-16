"""Maintenance policies retain task routing, filtering and pause barriers."""
import os
import time
from pathlib import Path

import psycopg
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={
    'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD'],
})
reply.raise_for_status()
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)


def policy(action, status=200):
    reply = admin.post(url + '/api/v1/maintenance/reuse/actions', json={'action': action}, timeout=15)
    assert reply.status_code == status, reply.text
    return reply.json()


def task(task_id):
    reply = admin.get(url + '/api/v1/tasks/' + task_id, timeout=10)
    reply.raise_for_status()
    return reply.json()


before = admin.get(url + '/api/v1/maintenance').json()
controls = {item['policy']: item for item in before['controls']}
assert set(controls) == {'pack', 'reuse', 'reclaim', 'repack', 'range', 'gc', 'cleanup'}
try:
    policy('pause')
    assert db.execute("SELECT paused FROM maintenance_controls WHERE policy='reuse'").fetchone() == (True,)
    policy('run', 409)
    policy('resume')
    task_id = policy('run')['task_id']
    for _ in range(200):
        result = task(task_id)
        assert result['task_type'] == 'pack' and result['policy'] == 'reuse', result
        assert result['detail']['policy'] == 'reuse' and 'kind' not in result['detail'], result
        assert result['status'] not in ('failed', 'paused'), result
        if result['status'] == 'completed':
            break
        time.sleep(.1)
    else:
        raise AssertionError('reuse task did not complete')
    children = db.execute('SELECT state,detail FROM tasks WHERE parent_task_id=%s', (task_id,)).fetchall()
    if result['detail'].get('execution_mode') == 'single':
        assert not children and 'role' not in result['detail']
        assert result['detail']['storage_index'] == len(result['detail']['storages'])
    else:
        assert children and all(state == 'completed' and detail['policy'] == 'reuse' for state, detail in children)
    found = admin.get(url + '/api/v1/tasks', params={'policy': 'reuse'}).json()['items']
    assert any(item['id'] == task_id for item in found)
    assert all(item['policy'] == 'reuse' for item in found)
    other = admin.get(url + '/api/v1/tasks', params={'policy': 'cleanup'}).json()['items']
    assert all(item['policy'] == 'cleanup' for item in other)
    assert admin.get(url + '/api/v1/tasks', params={'kind': 'reuse'}).status_code in (400, 422)
    assert requests.get(url + '/api/v1/maintenance').status_code == 403
    print('PASS policy controls, pause barrier, pack task creation/claim/completion and task filtering', flush=True)
finally:
    policy('pause' if controls['reuse']['paused'] else 'resume')
    db.close()
