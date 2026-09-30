"""Check the management schema and safe error envelope on an isolated service."""
import json
import os
import subprocess

import requests

url = os.environ['MOKYU_TEST_WEB']
session = requests.Session()
info = session.get(url + '/api/info')
info.raise_for_status()
assert info.json()['product'] == 'Mokyu'
assert info.json()['contract'] == 'mokyu-manage-1'
schema = session.get(url + '/api/openapi.json')
schema.raise_for_status()
schema = schema.json()
assert schema['openapi'].startswith('3.')
for path, method in [('/api/info', 'get'), ('/api/login', 'post'), ('/api/logout', 'post'),
                     ('/api/session', 'get'), ('/api/buckets', 'get'),
                     ('/api/buckets/{bucket}/website', 'put')]:
    assert method in schema['paths'][path]


def error(reply, status, code):
    assert reply.status_code == status, reply.text
    assert reply.headers['content-type'].startswith('application/json')
    payload = reply.json()
    assert payload['code'] == code, payload
    assert payload['request_id'] == reply.headers['x-request-id']
    assert isinstance(payload['error'], str)
    assert 'private' in reply.headers['cache-control']


error(session.get(url + '/api/session'), 403, 'AccessDenied')
error(session.post(url + '/api/login', headers={'Origin': url}, json={
    'username': 'tester', 'password': 'not-reflected-secret', 'unexpected': True,
}), 422, 'InvalidArgument')
error(session.post(url + '/api/login', headers={'Origin': url, 'Content-Type': 'application/json'},
                   data='{'), 400, 'InvalidArgument')
error(session.get(url + '/api/unknown'), 404, 'NotFound')
method = session.put(url + '/api/info')
error(method, 405, 'MethodNotAllowed')
assert 'GET' in method.headers['allow']

reply = session.post(url + '/api/login', headers={'Origin': url}, json={
    'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD'],
})
reply.raise_for_status()
csrf = reply.json()['csrf_token']
me = session.get(url + '/api/session')
me.raise_for_status()
assert me.json()['username'] == 'tester' and me.json()['csrf_token'] == csrf
buckets = session.get(url + '/api/buckets')
buckets.raise_for_status()
assert isinstance(buckets.json(), list)
for bucket in buckets.json():
    assert set(bucket) == {'id', 'name', 'state', 'cors', 'website_enabled',
                           'index_document', 'error_document', 'created_at', 'project_id', 'actions'}

if binary := os.environ.get('MOKYU_TEST_BINARY'):
    exported = subprocess.run([binary, '--config', '/missing/config.toml', 'api-schema'],
                              check=True, capture_output=True, text=True)
    assert json.loads(exported.stdout) == schema
print('PASS generated contract, typed responses, safe errors, request IDs and offline schema export')
