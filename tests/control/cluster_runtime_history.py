"""Bounded cluster history on real nodes; requires an isolated deployment."""
import json
import os
from pathlib import Path
import time
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
password = Path(os.environ['MOKYU_TEST_PASSWORD_FILE']).read_text().strip()
client = requests.Session()
reply = client.post(url+'/api/v1/auth/login',headers={'Origin':url},json={'username':'tester','password':password},timeout=15)
assert reply.ok

def get(path):
    reply = client.get(url+path,timeout=15)
    assert reply.ok,(path,reply.status_code,reply.text)
    assert {'private','no-store'} <= {v.strip() for v in reply.headers.get('cache-control','').split(',')}
    return reply.json()

nodes = get('/api/v1/cluster/nodes')['items']
until=time.monotonic()+210
while True:
    initial=(get('/api/v1/usage/runtime').get('cluster') or {}).get('current')
    if initial and initial['rate_nodes']==len(nodes):
        baseline=initial['at']
        break
    assert time.monotonic()<until,'cluster runtime baselines did not converge'
    time.sleep(2)
endpoints = json.loads(os.environ['MOKYU_TEST_DATA_ENDPOINTS'])
assert len(endpoints) == 2 and all(endpoint.startswith(('http://', 'https://')) for endpoint in endpoints)
for endpoint,count in zip(endpoints, [12,24]):
    for _ in range(count):
        requests.get(endpoint.rstrip('/')+'/runtime-observation-missing',timeout=10)
until=time.monotonic()+210
while True:
    runtime=get('/api/v1/usage/runtime')
    current=(runtime.get('cluster') or {}).get('current')
    if current and current['at']>baseline and current['rate_nodes']==len(nodes) and current['rates']['requests']>0:
        break
    assert time.monotonic()<until,'cluster runtime did not converge'
    time.sleep(2)
assert runtime['scope']=='cluster' and runtime['current'] is None and runtime['history']==[]
assert current['expected_nodes']==len(nodes) and current['reported_nodes']==len(nodes)
assert runtime['cluster']['stale'] is False
assert current['gauges'] and current['rates']['requests']>=0
assert current['window_start']<=current['at']
for node in nodes:
    history=get('/api/v1/cluster/nodes/'+node['id']+'/runtime?hours=1&limit=2')['history']
    assert 1<=len(history)<=2
    assert all(p['node_id']==node['id'] and p['boot_id'] for p in history)
    if node['role']=='control':
        assert all(p['point']['requests']=='0' for p in history),'management traffic entered public read statistics'
for path in ['/api/v1/usage/runtime?hours=169','/api/v1/usage/runtime?limit=2049','/api/v1/cluster/nodes/'+nodes[0]['id']+'/runtime?limit=0']:
    reply=client.get(url+path,timeout=15)
    assert reply.status_code==400
    assert reply.json()['code']=='InvalidArgument'
    assert reply.headers.get('x-request-id')
assert requests.get(url+'/api/v1/usage/runtime',timeout=15).status_code in (401,403)
print('PASS real-node runtime, public request traffic, bounded history, private cache policy and validation')
