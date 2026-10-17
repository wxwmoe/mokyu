"""Real cluster reports, pagination, settings rollout and read-only authorization."""
import json
import os
from pathlib import Path
import time

import psycopg
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
password = Path(os.environ['MOKYU_TEST_PASSWORD_FILE']).read_text().strip()
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_URL_FILE']).read_text(), autocommit=True)
admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin':url}, json={'username':'tester','password':password},timeout=15)
assert reply.status_code == 200
admin.headers.update({'Origin':url,'X-CSRF-Token':reply.json()['csrf_token']})

def get(path):
    reply = admin.get(url + path,timeout=15)
    assert reply.status_code == 200,(path,reply.status_code,reply.text)
    return reply.json()

def wait(check):
    until = time.monotonic()+35
    while time.monotonic()<until:
        if check():
            return
        time.sleep(.2)
    raise AssertionError('cluster did not converge')

expected = int(os.environ.get('MOKYU_TEST_NODES', '4'))
wait(lambda: len(get('/api/v1/cluster/nodes')['items']) == expected and all(row['report_fresh'] for row in get('/api/v1/cluster/nodes')['items']))
nodes = get('/api/v1/cluster/nodes')['items']
assert {node['role'] for node in nodes} == {'control','data','worker'}
assert all(node['contact']=='online' for node in nodes)
assert all(node['configuration'] in ('applied','equivalent') for node in nodes)
page = get('/api/v1/cluster/nodes?limit=2')
assert len(page['items']) == 2 and page['next_cursor']
next_page = get('/api/v1/cluster/nodes?limit=2&after='+page['next_cursor'])
assert len(next_page['items']) == 2
assert bool(next_page['next_cursor']) == (expected > 4)
assert {row['id'] for row in page['items']}.isdisjoint(row['id'] for row in next_page['items'])
for query in ['limit=201','limit=0','role=invalid','after=broken','role=data&limit=2&after='+page['next_cursor']]:
    assert admin.get(url+'/api/v1/cluster/nodes?'+query).status_code == 400,query
assert len(get('/api/v1/cluster/nodes?role=data')['items']) == 2
assert len(get('/api/v1/cluster/nodes?name=data1')['items']) == 1
assert all(row['work']=='normal' for row in get('/api/v1/cluster/nodes?work=normal')['items'])
assert requests.get(url+'/api/v1/cluster/nodes',timeout=10).status_code in (401,403)
data = next(node for node in nodes if node['role']=='data')
config = get('/api/v1/cluster/nodes/'+data['id']+'/config')
fields = {row['key']:row for row in config['fields']}
assert fields['instance.name']['value'] == data['name']
assert fields['database.program']['value'] == 'PostgreSQL'
assert fields['instance.deployment_key']['value']=='[redacted]'
assert fields['database.password']['value']=='[redacted]'
detail = get('/api/v1/cluster/nodes/'+data['id']+'/status')
assert detail['boot_id'] and detail['status']['work'] in ('normal','maintenance')
assert len(detail['tasks'])<=20
assert 'response_cache' in detail['status'] and 'compression' in detail['status']
assert sum(int(group['count']) for group in get('/api/v1/cluster/status')['nodes'])==expected

# Polling remains readable after recent authentication expires; mutations do not.
db.execute("UPDATE sessions SET reauthenticated_at=now()-interval '10 minutes' WHERE user_id=(SELECT id FROM web_users WHERE username='tester')")
for path in ['/api/v1/cluster/status','/api/v1/cluster/nodes','/api/v1/cluster/settings', '/api/v1/cluster/nodes/'+data['id']+'/status','/api/v1/cluster/nodes/'+data['id']+'/config']:
    get(path)
reply=admin.post(url+'/api/v1/cluster/nodes/'+data['name']+'/release',json={'confirm':True})
assert reply.status_code==403 and reply.json()['code']=='ReauthenticationRequired'
assert admin.post(url+'/api/v1/account/reauth',json={'password':password}).status_code==204

original = get('/api/v1/runtime-settings/requests')
try:
    changed={**original['value'],'read_idle_timeout':'121s'}
    reply=admin.put(url+'/api/v1/runtime-settings/requests',json={'value':changed,'revision':original['revision']})
    assert reply.status_code==200,reply.text
    rows=get('/api/v1/cluster/nodes')['items']
    assert all(row['configuration']=='restart_required' for row in rows)
    actual=get('/api/v1/cluster/nodes/'+data['id']+'/config')
    assert next(row['value'] for row in actual['fields'] if row['key']=='requests.read_idle_timeout')==original['value']['read_idle_timeout']
finally:
    current=get('/api/v1/runtime-settings/requests')
    assert admin.put(url+'/api/v1/runtime-settings/requests',json={'value':original['value'],'revision':current['revision']}).status_code==200
assert all(row['configuration']=='equivalent' for row in get('/api/v1/cluster/nodes')['items'])
print('PASS node reports, scoped pagination, bounded detail, redaction, settings values and authentication freshness')
