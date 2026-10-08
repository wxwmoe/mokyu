"""Check the management schema and safe error envelope on an isolated service."""
import json
import os
import subprocess

import requests

url = os.environ['MOKYU_TEST_WEB']
session = requests.Session()
info = session.get(url + '/api/v1/info')
info.raise_for_status()
assert info.json()['product'] == 'Mokyu'
assert info.json()['contract'] == 'mokyu-control-v1'
schema = session.get(url + '/api/v1/openapi.json')
schema.raise_for_status()
schema = schema.json()
assert schema['openapi'].startswith('3.')
assert not any(path.startswith('/api/') and not path.startswith('/api/v1/') for path in schema['paths'])
for path, method in [('/api/v1/info', 'get'), ('/api/v1/auth/login', 'post'), ('/api/v1/auth/logout', 'post'),
                     ('/api/v1/auth/session', 'get'), ('/api/v1/buckets', 'get'),
                     ('/api/v1/buckets/{bucket}/website', 'put'),
                     ('/api/v1/users', 'post'), ('/api/v1/projects', 'post'),
                     ('/api/v1/api-tokens', 'post'), ('/api/v1/s3-credentials', 'post'),
                     ('/api/v1/quotas/{scope_type}/{scope_id}', 'put'), ('/api/v1/audit', 'get'),
                     ('/api/v1/buckets/{bucket}/objects', 'get'),
                     ('/api/v1/buckets/{bucket}/cors', 'get'),
                     ('/api/v1/packs', 'get'), ('/api/v1/packs/{id}', 'get'),
                     ('/api/v1/cache/flush', 'post'), ('/api/v1/tasks/{id}/report', 'get'),
                     ('/api/v1/cli', 'post'),
                     ('/api/v1/storages/{name}/profile', 'get'),
                     ('/api/v1/storages/{name}/key', 'put'),
                     ('/api/v1/storages/{name}/unpack', 'post'),
                     ('/api/v1/domains', 'get'), ('/api/v1/domains', 'post'),
                     ('/api/v1/domains/{host}', 'delete'),
                     ('/api/v1/cluster/recover', 'post'),
                     ('/api/v1/buckets/{bucket}/uploads', 'post'),
                     ('/api/v1/media/actions', 'post'), ('/api/v1/usage/summary', 'get'),
                     ('/api/v1/maintenance', 'get'), ('/api/v1/system/config', 'get')]:
    assert method in schema['paths'][path]


def error(reply, status, code):
    assert reply.status_code == status, reply.text
    assert reply.headers['content-type'].startswith('application/json')
    payload = reply.json()
    assert payload['code'] == code, payload
    assert payload['request_id'] == reply.headers['x-request-id']
    assert isinstance(payload['error'], str)
    assert 'private' in reply.headers['cache-control']


error(session.get(url + '/api/v1/auth/session'), 403, 'AccessDenied')
error(session.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={
    'username': 'tester', 'password': 'not-reflected-secret', 'unexpected': True,
}), 422, 'InvalidArgument')
error(session.post(url + '/api/v1/auth/login', headers={'Origin': url, 'Content-Type': 'application/json'},
                   data='{'), 400, 'InvalidArgument')
error(session.get(url + '/api/v1/unknown'), 404, 'NotFound')
error(session.get(url + '/api/unknown'), 404, 'NotFound')
method = session.put(url + '/api/v1/info')
error(method, 405, 'MethodNotAllowed')
assert 'GET' in method.headers['allow']

reply = session.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={
    'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD'],
})
reply.raise_for_status()
csrf = reply.json()['csrf_token']
me = session.get(url + '/api/v1/auth/session')
me.raise_for_status()
assert me.json()['username'] == 'tester' and me.json()['csrf_token'] == csrf
buckets = session.get(url + '/api/v1/buckets')
buckets.raise_for_status()
assert isinstance(buckets.json(), list)
for bucket in buckets.json():
    assert set(bucket) == {'id', 'name', 'status', 'cors', 'website_enabled',
                           'index_document', 'error_document', 'created_at', 'project_id', 'actions',
                           'revision', 'uploads_paused', 'public_base_url'}

if binary := os.environ.get('MOKYU_TEST_BINARY'):
    exported = subprocess.run([binary, '--config', '/missing/config.toml', 'tool', 'openapi'],
                              check=True, capture_output=True, text=True)
    assert json.loads(exported.stdout) == schema
print('PASS generated contract, typed responses, safe errors, request IDs and offline schema export')
