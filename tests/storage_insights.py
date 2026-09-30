"""Exact interval attribution, shared packs, pending data and scoped snapshot access."""
import json
import os
import time
import subprocess
import uuid
from decimal import Decimal
from pathlib import Path

import psycopg
import requests
from integration import s3, bucket as s3_bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
connection = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip()
unit = 131072

# Temporary tables shadow the application relations on this connection only.
with psycopg.connect(connection) as db:
    db.execute('''CREATE TEMP TABLE buckets(id uuid,project_id uuid);
      CREATE TEMP TABLE objects(bucket_id uuid,stream_id uuid);
      CREATE TEMP TABLE extents(stream_id uuid,chunk_id bigint,source_offset int,length int);
      CREATE TEMP TABLE chunks(id bigint,pack_id bigint,unreferenced_at timestamptz);
      CREATE TEMP TABLE chunk_locations(id bigint,chunk_id bigint,state text,stored_size bigint);
      CREATE TEMP TABLE packs(id bigint,state text,stored_size bigint);
      CREATE TEMP TABLE quota_accounts(kind text,id uuid,used_bytes bigint,object_count bigint);
      CREATE TEMP TABLE storage_insights(id text,bucket_ids uuid[],as_of timestamptz,data jsonb);''')
    buckets = {name: uuid.uuid4() for name in ['alpha', 'beta', 'gamma', 'delta', 'epsilon']}
    project_a = uuid.uuid4()
    refs = [('alpha', 1, 0, 16), ('alpha', 1, 0, 16), ('alpha', 5, 0, 4), ('beta', 1, 0, 16), ('beta', 2, 0, 16), ('gamma', 1, 0, 4), ('gamma', 1, 2, 4), ('delta', 3, 0, 2), ('epsilon', 4, 0, 4)]
    for name, id in buckets.items():
        db.execute('INSERT INTO buckets VALUES(%s,%s)', (id, project_a if name in ['alpha', 'gamma'] else uuid.uuid4()))
        db.execute('INSERT INTO storage_insights(id,bucket_ids) VALUES(%s,%s)', (name, [id]))
        matching = [r for r in refs if r[0] == name]
        db.execute("INSERT INTO quota_accounts VALUES('bucket',%s,%s,%s)", (id, sum(r[3] * unit for r in matching), len(matching)))
    db.execute("INSERT INTO storage_insights(id,bucket_ids) VALUES('global',NULL),('project-a',%s)", ([buckets['alpha'], buckets['gamma']],))
    for name, chunk, offset, length in refs:
        stream = uuid.uuid4()
        db.execute('INSERT INTO objects VALUES(%s,%s)', (buckets[name], stream))
        db.execute('INSERT INTO extents VALUES(%s,%s,%s,%s)', (stream, chunk, offset * unit, length * unit))
    db.execute('INSERT INTO chunks VALUES(1,1,NULL),(2,1,NULL),(3,2,NULL),(4,NULL,NULL),(5,NULL,NULL),(6,2,now())')
    db.execute("INSERT INTO chunk_locations VALUES(5,5,'ready',%s),(6,6,'retired',%s)", (2 * unit, 5 * unit))
    db.execute("INSERT INTO packs VALUES(1,'ready',%s),(2,'ready',%s)", (8 * unit, 8 * unit))
    db.execute(Path('/work/src/manage/storage_insights.sql').read_text(), prepare=False)
    values = dict(db.execute('SELECT id,data FROM storage_insights').fetchall())
    number = lambda name, key: Decimal(values[name][key]) / unit
    assert number('alpha', 'unique_bytes') == 20 and number('alpha', 'local_savings') == 16
    assert number('alpha', 'encoded_bytes') == Decimal('3.625')
    assert number('beta', 'encoded_bytes') == 6
    assert number('gamma', 'unique_bytes') == 6 and number('gamma', 'encoded_bytes') == Decimal('.375')
    assert number('delta', 'encoding_savings') == -6
    assert values['epsilon']['encoded_bytes'] is None and values['epsilon']['encoding_savings'] is None
    assert number('epsilon', 'pending_bytes') == 4
    assert number('project-a', 'unique_bytes') == 20 and number('project-a', 'encoded_bytes') == 4
    assert number('global', 'unique_bytes') == 42 and number('global', 'encoded_known_bytes') == 18
    bridge = values['global']['physical']
    assert int(bridge['indexed_bytes']) == 23 * unit and int(bridge['selected_bytes']) == 18 * unit and int(bridge['gc_bytes']) == 5 * unit
    assert sum(Decimal(v['encoded_bytes']) for k, v in values.items() if k in buckets and v['encoded_bytes'] is not None) == 18 * unit
    for value in values.values():
        if value['encoded_bytes'] is not None:
            assert abs(Decimal(value['logical_bytes']) - Decimal(value['encoded_bytes']) - sum(Decimal(value[k]) for k in ['local_savings', 'shared_savings', 'encoding_savings'])) < Decimal('.00001')
    assert all(v['physical'] is None for k, v in values.items() if k != 'global')
    # Real retained copies and encoding expansion are not counted as extra deduplication.
    db.execute("INSERT INTO chunk_locations VALUES(7,1,'retired',%s)", (4 * unit,))
    db.execute(Path('/work/src/manage/storage_insights.sql').read_text(), prepare=False)
    bridge = db.execute("SELECT data->'physical' FROM storage_insights WHERE id='global'").fetchone()[0]
    assert int(bridge['selected_bytes']) + int(bridge['retained_bytes']) + int(bridge['gc_bytes']) == int(bridge['indexed_bytes']) == 27 * unit
    db.rollback()
print('PASS exact intervals, project union, proportional shared packs, negative savings, pending unknown and physical conservation', flush=True)


def login(name, password):
    client = requests.Session()
    result = client.post(url + '/api/login', headers={'Origin': url}, json={'username': name, 'password': password})
    assert result.status_code == 200, result.text
    client.headers.update({'Origin': url, 'X-CSRF-Token': result.json()['csrf_token']})
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
assert admin.post(url + '/api/me/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204
name = 'insight-' + uuid.uuid4().hex[:8]
secret = uuid.uuid4().hex
user = admin.post(url + '/api/users', json={'username': name, 'password': secret, 'role': 'member', 'must_change_password': False}).json()['id']
bucket = next(b for b in admin.get(url + '/api/buckets').json() if b['name'] == 'media')
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))

key = name + '-packed.bin'
raw = os.urandom(15 * 1024 * 1024)
s3.put_object(Bucket=s3_bucket, Key=key, Body=raw)
task = cli('pack', 'run')['task_id']
for _ in range(600):
    state = cli('task', 'show', task)
    assert state['state'] not in ['failed', 'paused'], state
    if state['state'] == 'completed':
        break
    time.sleep(.1)
else:
    raise AssertionError('packing did not finish')
with psycopg.connect(connection) as db:
    source, pack = db.execute('SELECT o.stream_id,c.pack_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.bucket_id=%s AND o.key=%s AND c.pack_id IS NOT NULL LIMIT 1', (bucket['id'], key)).fetchone()
    count = db.execute('SELECT count(*) FROM pack_members WHERE pack_id=%s', (pack,)).fetchone()[0]
assert count > 2
hidden = cli('bucket', 'create', name + '-hidden')
result = admin.post(url + '/api/media/actions', json={'bucket': bucket['id'], 'action': 'copy', 'target_bucket': hidden['id'], 'objects': [{'client_id': str(uuid.uuid4()), 'key': key, 'version': str(source), 'target_key': 'private-reference.bin'}]})
assert result.status_code == 200 and result.json()['results'][0]['status'] == 200, result.text
with psycopg.connect(connection) as db:
    first_length = db.execute('SELECT length FROM extents WHERE stream_id=%s ORDER BY offset_bytes LIMIT 1', (source,)).fetchone()[0]
s3.put_object(Bucket=s3_bucket, Key=key, Body=raw[:first_length])
with psycopg.connect(connection) as db:
    source = db.execute('SELECT stream_id FROM objects WHERE bucket_id=%s AND key=%s', (bucket['id'], key)).fetchone()[0]
try:
    assert admin.put(url + f"/api/projects/{bucket['project_id']}/members/{user}", json={'role': 'reader', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'storage.inspect']}]}).status_code == 204
    member = login(name, secret)
    detail = member.get(url + '/api/storage/packs/' + str(pack))
    assert detail.status_code == 200, detail.text
    assert len(detail.json()['members']) == 1
    assert len(admin.get(url + '/api/storage/packs/' + str(pack)).json()['members']) == count
    linked = member.get(url + '/api/storage/packs/' + str(pack) + '/objects').json()
    assert linked['objects'] and all(o['bucket_id'] == bucket['id'] for o in linked['objects'])
    chunks = member.get(url + '/api/object/chunks', params={'bucket': bucket['id'], 'key': key, 'version': str(source)})
    assert chunks.status_code == 200 and chunks.json()['chunks'], chunks.text
    assert all(c['key_id'] is None and c['reads'] is None for c in chunks.json()['chunks'])
    def ready(client, query=''):
        deadline = time.monotonic() + 45
        while True:
            result = client.get(url + '/api/insights' + query, timeout=5)
            assert result.status_code == 200, result.text
            value = result.json()
            if value['usage'] is not None:
                return value
            assert time.monotonic() < deadline, value
            time.sleep(.25)
    global_usage = ready(admin)
    assert global_usage['scope'] == 'deployment' and global_usage['usage']['physical'] is not None
    visible = ready(member)
    assert visible['scope'] == 'visible' and visible['bucket_count'] == 1 and visible['usage']['physical'] is None
    assert member.get(url + '/api/insights/runtime').status_code == 403
    assert member.get(url + '/api/insights?bucket=' + str(uuid.uuid4())).status_code == 403
    runtime = admin.get(url + '/api/insights/runtime').json()
    assert runtime['current']['rss_bytes'] is not None and runtime['history']
    assert len(runtime['history']) <= 2048
    start = time.monotonic()
    with psycopg.connect(connection) as db:
        db.execute('LOCK TABLE extents IN ACCESS EXCLUSIVE MODE')
        assert member.get(url + '/api/insights', timeout=3).status_code == 200
    assert time.monotonic() - start < 3
    for pack in member.get(url + '/api/storage/packs').json()['packs']:
        assert pack['raw_size'] is None and pack['stored_size'] is None and pack['member_count'] is None
        detail = member.get(url + '/api/storage/packs/' + pack['id']).json()
        assert detail['scoped'] and all(m['ordinal'] is None and m['raw_size'] is None for m in detail['members'])
        linked = member.get(url + '/api/storage/packs/' + pack['id'] + '/objects').json()
        assert all(o['bucket_id'] == bucket['id'] for o in linked['objects'])
        first = member.get(url + '/api/storage/packs/' + pack['id'], params={'limit': 1}).json()
        if first['next']:
            second = member.get(url + '/api/storage/packs/' + pack['id'], params={'limit': 1, 'after': first['next']}).json()
            assert first['members'][0]['chunk_id'] != second['members'][0]['chunk_id']
    assert admin.delete(url + f"/api/projects/{bucket['project_id']}/members/{user}").status_code == 204
    denied = member.get(url + '/api/insights?bucket=' + bucket['id'])
    assert denied.status_code == 403, denied.text
    empty = ready(member)
    assert empty['bucket_count'] == 0 and empty['usage']['logical_bytes'] == '0'
    assert member.get(url + '/api/storage/packs').json()['packs'] == []
    assert requests.get(url + '/api/insights').status_code == 403
    print('PASS cached reads avoid extent scans, scoped metadata and runtime privacy, live revocation and bounded persistent history', flush=True)
finally:
    admin.delete(url + '/api/users/' + user)
    s3.delete_object(Bucket=s3_bucket, Key=key)
    preview = admin.post(url + '/api/buckets/' + hidden['id'] + '/purge/preview').json()
    admin.post(url + '/api/buckets/' + hidden['id'] + '/purge', json={'confirm_name': hidden['name'], 'confirmation': preview['confirmation']})
