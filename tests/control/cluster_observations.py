"""Observation failures must not become public read failures; isolated fixture only."""
import json
import os
from pathlib import Path
import secrets
import time
import uuid
import boto3
from botocore.config import Config
import psycopg
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url=os.environ['MOKYU_TEST_WEB']
password=Path(os.environ['MOKYU_TEST_PASSWORD_FILE']).read_text().strip()
db=psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_URL_FILE']).read_text().strip(),autocommit=True)
checkpoint=Path(os.environ['MOKYU_TEST_OBSERVATIONS_FILE'])
phase=os.environ.get('MOKYU_TEST_PHASE','reports')
client=requests.Session()
login=client.post(url+'/api/v1/auth/login',headers={'Origin':url},json={'username':'tester','password':password},timeout=15)
assert login.ok
client.headers.update({'Origin':url,'X-CSRF-Token':login.json()['csrf_token']})
def get(path):
    result=client.get(url+path,timeout=15)
    assert result.ok,(path,result.status_code,result.text)
    return result.json()
def wait(check,seconds=45):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        result=check()
        if result: return result
        time.sleep(.5)
    raise AssertionError('observation did not converge')
nodes=get('/api/v1/cluster/nodes')['items']
target=next(n for n in nodes if n['name'].endswith('data2'))
endpoint=os.environ['MOKYU_TEST_DATA1']
credentials=json.loads(Path(os.environ['MOKYU_TEST_S3_CREDENTIALS']).read_text())
s3=boto3.client('s3',endpoint_url=endpoint,region_name='us-east-1',aws_access_key_id=credentials['access_key'],aws_secret_access_key=credentials['secret_key'],config=Config(signature_version='s3v4',s3={'addressing_style':'path'},request_checksum_calculation='when_required'))
key='runtime-preview/sample.txt'
body=b'Mokyu runtime observation preview.\n'*4096
def read_public():
    reply=requests.get(endpoint+'/media/'+key,headers={'Accept-Encoding':'gzip'},timeout=15)
    assert reply.ok and reply.content==body
    partial=requests.get(endpoint+'/media/'+key,headers={'Range':'bytes=31-300','Accept-Encoding':'gzip'},timeout=15)
    assert partial.status_code==206 and partial.content==body[31:301]
    assert not partial.headers['ETag'].startswith('W/')

if phase=='reports':
    s3.put_object(Bucket='media',Key=key,Body=body,ACL='public-read',ContentType='text/plain')
    read_public()
    detail=get('/api/v1/cluster/nodes/'+target['id']+'/status')
    member_password=secrets.token_urlsafe(24)
    member_name='observer-'+uuid.uuid4().hex[:8]
    created=client.post(url+'/api/v1/users',json={'username':member_name,'password':member_password,'role':'member','must_change_password':False},timeout=15)
    assert created.status_code==201,created.text
    saved={'node':target['id'],'boot':detail['boot_id'],'member':member_name,'password':member_password,'user_id':created.json()['id']}
    checkpoint.write_text(json.dumps(saved));checkpoint.chmod(0o600)
    member=requests.Session()
    assert member.post(url+'/api/v1/auth/login',headers={'Origin':url},json={'username':member_name,'password':member_password},timeout=15).ok
    for path in ['/api/v1/cluster/status','/api/v1/cluster/nodes','/api/v1/cluster/settings','/api/v1/usage/runtime']+[f'/api/v1/cluster/nodes/{target["id"]}/{part}' for part in ['status','config','runtime']]:
        assert member.get(url+path,timeout=15).status_code==403,path
    # Hold only one report row; heartbeats and public reads must keep progressing.
    with db.transaction():
        db.execute('SELECT 1 FROM node_reports WHERE node_id=%s FOR UPDATE',(target['id'],))
        before=db.execute('SELECT last_seen_at FROM nodes WHERE id=%s',(target['id'],)).fetchone()[0]
        wait(lambda: get('/api/v1/cluster/nodes/'+target['id']+'/status')['node']['report_fresh'] is False,40)
        node=get('/api/v1/cluster/nodes/'+target['id']+'/status')['node']
        assert node['contact']=='online' and node['configuration']=='unknown',node
        assert db.execute('SELECT last_seen_at FROM nodes WHERE id=%s',(target['id'],)).fetchone()[0]>before
        read_public()
    wait(lambda: get('/api/v1/cluster/nodes/'+target['id']+'/status')['node']['report_fresh'])
    # Page reads do not produce diagnostic history writes.
    for _ in range(12): get('/api/v1/usage/runtime')
    assert db.execute("SELECT max(count) FROM (SELECT count(*) FROM node_runtime_samples GROUP BY node_id,date_trunc('minute',received_at)) s").fetchone()[0]<=2
    print('PASS member authorization, delayed reports, independent heartbeat, public reads and bounded sampling',flush=True)
elif phase=='stopped':
    wait(lambda: get('/api/v1/cluster/nodes/'+target['id']+'/status')['node']['contact']=='unresponsive')
    wait(lambda: (get('/api/v1/usage/runtime')['cluster']['current'] or {}).get('reported_nodes',len(nodes))<len(nodes),140)
    read_public()
    print('PASS stopped data node retains identity and yields partial coverage',flush=True)
elif phase=='restarted':
    saved=json.loads(checkpoint.read_text())
    wait(lambda: get('/api/v1/cluster/nodes/'+saved['node']+'/status')['node']['report_fresh'])
    detail=get('/api/v1/cluster/nodes/'+saved['node']+'/status')
    assert detail['boot_id']!=saved['boot']
    history=get('/api/v1/cluster/nodes/'+saved['node']+'/runtime')['history']
    assert len({p['boot_id'] for p in history})>=2
    read_public()
    print('PASS restart uses a new boot and preserves historical identity',flush=True)
elif phase=='no-worker':
    worker=next(n for n in nodes if n['role']=='worker')
    reply=client.post(url+'/api/v1/cluster/nodes/'+worker['name']+'/release',json={'confirm':True},timeout=15)
    assert reply.ok,reply.text
    assert not get('/api/v1/cluster/nodes?role=worker')['items']
    summary=get('/api/v1/cluster/status')['nodes']
    assert sum(int(r['count']) for r in summary if r['contact']!='released' and r['role']=='worker')==0
    read_public()
    s3.put_object(Bucket='media',Key=key,Body=body,ACL='public-read',ContentType='text/plain')
    read_public()
    print('PASS public reads and writes without a registered worker',flush=True)
