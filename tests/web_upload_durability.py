"""A completed browser upload must distinguish local acceptance from remote durability."""
import base64
import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
assert os.environ.get('MOKYU_TEST_UPLOAD_CACHE') == 'enabled'
url = os.environ['MOKYU_TEST_WEB']
client = requests.Session()
reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
assert reply.status_code == 200
client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
bucket = json.loads(subprocess.check_output(command + ['bucket', 'create', 'durability-' + uuid.uuid4().hex[:10]], text=True))
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'upload-before-work'
data = os.urandom(6 * 1024 * 1024 + 31)
try:
    marker.touch()
    reply = client.post(url + '/api/buckets/' + bucket['id'] + '/uploads', json={'client_id': str(uuid.uuid4()), 'key': 'pending', 'file_name': 'pending', 'size': str(len(data))})
    assert reply.status_code == 201, reply.text
    path = url + '/api/uploads/' + reply.json()['id']
    reply = client.put(path + '/parts/1', data=data, headers={'X-Content-SHA256': base64.b64encode(hashlib.sha256(data).digest()).decode()}, timeout=60)
    assert reply.status_code == 200, reply.text
    reply = client.post(path + '/complete', json={'parts': [reply.json()]}, timeout=60)
    assert reply.status_code == 200, reply.text
    assert (reply.json()['state'], reply.json()['remote_state']) == ('completed', 'pending'), reply.json()
    assert client.get(url + '/api/download', params={'bucket': bucket['id'], 'key': 'pending'}).content == data
    source = client.get(url + '/api/buckets/' + bucket['id'] + '/objects', params={'q': 'pending', 'mode': 'exact'}).json()['objects'][0]
    for action, key, target in [('copy', 'pending', 'copy'), ('move', 'pending', 'moved')]:
        result = client.post(url + '/api/media/actions', json={'bucket': bucket['id'], 'action': action, 'target_bucket': bucket['id'],
            'objects': [{'client_id': str(uuid.uuid4()), 'key': key, 'version': source['id'], 'target_key': target}]}).json()['results'][0]
        assert result['status'] == 200, result
        assert client.get(url + '/api/download', params={'bucket': bucket['id'], 'key': target}).content == data
    result = client.post(url + '/api/media/actions', json={'bucket': bucket['id'], 'action': 'delete',
        'objects': [{'client_id': str(uuid.uuid4()), 'key': 'moved', 'version': source['id']}]}).json()['results'][0]
    assert result['status'] == 200
    assert client.get(url + '/api/download', params={'bucket': bucket['id'], 'key': 'copy'}).content == data
    marker.unlink()
    for _ in range(200):
        row = client.get(path).json()
        if row['remote_state'] == 'stored':
            break
        time.sleep(.1)
    else:
        raise AssertionError(row)
    assert client.get(url + '/api/download', params={'bucket': bucket['id'], 'key': 'copy'}).content == data
    print('PASS pending upload copy/move/delete retain shared local data and become stored only after backend flush', flush=True)
finally:
    marker.unlink(missing_ok=True)
