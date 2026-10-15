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
from tests.integration import s3, bucket as s3_bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
connection = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip()
unit = 131072
queries = (Path(__file__).resolve().parents[2] / 'src/model/backend/postgres/queries.rs').read_text()
usage_sql = queries.split('const USAGE_SUMMARY: &str = r#"', 1)[1].split('"#;', 1)[0]

# Temporary tables shadow the application relations on this connection only.
with psycopg.connect(connection) as db:
    db.execute('''CREATE TEMP TABLE buckets(id uuid,project_id uuid);
      CREATE TEMP TABLE objects(bucket_id uuid,stream_id uuid);
      CREATE TEMP TABLE extents(stream_id uuid,chunk_id bigint,source_offset int,length int);
      CREATE TEMP TABLE chunks(id bigint,pack_id bigint,unreferenced_at timestamptz);
      CREATE TEMP TABLE chunk_encodings(encoding_id bigint,chunk_id bigint,state text,standalone_size bigint);
      CREATE TEMP TABLE packs(id bigint,state text,stored_size bigint);
      CREATE TEMP TABLE quota_scopes(scope text,scope_id uuid,used_bytes bigint,object_count bigint);
      CREATE TEMP TABLE usage_summary(id text,bucket_ids uuid[],as_of timestamptz,data jsonb);''')
    buckets = {name: uuid.uuid4() for name in ['alpha', 'beta', 'gamma', 'delta', 'epsilon']}
    project_a = uuid.uuid4()
    refs = [('alpha', 1, 0, 16), ('alpha', 1, 0, 16), ('alpha', 5, 0, 4), ('beta', 1, 0, 16), ('beta', 2, 0, 16), ('gamma', 1, 0, 4), ('gamma', 1, 2, 4), ('delta', 3, 0, 2), ('epsilon', 4, 0, 4)]
    for name, id in buckets.items():
        db.execute('INSERT INTO buckets VALUES(%s,%s)', (id, project_a if name in ['alpha', 'gamma'] else uuid.uuid4()))
        db.execute('INSERT INTO usage_summary(id,bucket_ids) VALUES(%s,%s)', (name, [id]))
        matching = [r for r in refs if r[0] == name]
        db.execute("INSERT INTO quota_scopes VALUES('bucket',%s,%s,%s)", (id, sum(r[3] * unit for r in matching), len(matching)))
    db.execute("INSERT INTO usage_summary(id,bucket_ids) VALUES('global',NULL),('project-a',%s)", ([buckets['alpha'], buckets['gamma']],))
    for name, chunk, offset, length in refs:
        stream = uuid.uuid4()
        db.execute('INSERT INTO objects VALUES(%s,%s)', (buckets[name], stream))
        db.execute('INSERT INTO extents VALUES(%s,%s,%s,%s)', (stream, chunk, offset * unit, length * unit))
    db.execute('INSERT INTO chunks VALUES(1,1,NULL),(2,1,NULL),(3,2,NULL),(4,NULL,NULL),(5,NULL,NULL),(6,2,now())')
    db.execute("INSERT INTO chunk_encodings VALUES(5,5,'ready',%s),(6,6,'retired',%s)", (2 * unit, 5 * unit))
    db.execute("INSERT INTO packs VALUES(1,'ready',%s),(2,'ready',%s)", (8 * unit, 8 * unit))
    db.execute(usage_sql, prepare=False)
    values = dict(db.execute('SELECT id,data FROM usage_summary').fetchall())
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
    db.execute("INSERT INTO chunk_encodings VALUES(7,1,'retired',%s)", (4 * unit,))
    db.execute(usage_sql, prepare=False)
    bridge = db.execute("SELECT data->'physical' FROM usage_summary WHERE id='global'").fetchone()[0]
    assert int(bridge['selected_bytes']) + int(bridge['retained_bytes']) + int(bridge['gc_bytes']) == int(bridge['indexed_bytes']) == 27 * unit
    db.rollback()
print('PASS exact intervals, project union, proportional shared packs, negative savings, pending unknown and physical conservation', flush=True)


def login(name, password):
    client = requests.Session()
    result = client.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={'username': name, 'password': password})
    assert result.status_code == 200, result.text
    client.headers.update({'Origin': url, 'X-CSRF-Token': result.json()['csrf_token']})
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
assert admin.post(url + '/api/v1/account/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204
name = 'insight-' + uuid.uuid4().hex[:8]
secret = uuid.uuid4().hex
user = admin.post(url + '/api/v1/users', json={'username': name, 'password': secret, 'role': 'member', 'must_change_password': False}).json()['id']
bucket = next(b for b in admin.get(url + '/api/v1/buckets').json() if b['name'] == s3_bucket)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))

before = admin.get(url + '/api/v1/maintenance').json()
hidden = None
key = name + '-packed.bin'
try:
    for policy in before['controls']:
        if policy['policy'] in ['pack', 'reuse', 'reclaim', 'repack', 'range']:
            cli('maintenance', 'pause', policy['policy'])
    cli('maintenance', 'resume', 'pack')
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
        source, pack = db.execute('SELECT o.stream_id,c.pack_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.bucket_id=%s AND o.object_key=%s AND c.pack_id IS NOT NULL LIMIT 1', (bucket['id'], key)).fetchone()
        count = db.execute('SELECT count(*) FROM pack_members WHERE pack_id=%s', (pack,)).fetchone()[0]
    assert count > 2
    storage = cli('storage', 'show', os.environ['MOKYU_TEST_STORAGE'])['id']
    cli('maintenance', 'pause', 'pack')
    hidden = cli('bucket', 'create', name + '-hidden', '--storage', storage)
    result = admin.post(url + '/api/v1/media/actions', json={'bucket': bucket['id'], 'action': 'copy', 'target_bucket': hidden['id'], 'objects': [{'client_id': str(uuid.uuid4()), 'key': key, 'version': str(source), 'target_key': 'private-reference.bin'}]})
    assert result.status_code == 200 and result.json()['results'][0]['status'] == 200, result.text
    with psycopg.connect(connection) as db:
        first_length = db.execute('SELECT length FROM extents WHERE stream_id=%s ORDER BY offset_bytes LIMIT 1', (source,)).fetchone()[0]
    s3.put_object(Bucket=s3_bucket, Key=key, Body=raw[:first_length])
    with psycopg.connect(connection) as db:
        source = db.execute('SELECT stream_id FROM objects WHERE bucket_id=%s AND object_key=%s', (bucket['id'], key)).fetchone()[0]
    assert admin.put(url + f"/api/v1/projects/{bucket['project_id']}/members/{user}", json={'role': 'reader', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'storage.inspect']}]}).status_code == 204
    member = login(name, secret)
    detail = member.get(url + '/api/v1/storage/packs/' + str(pack))
    assert detail.status_code == 200, detail.text
    assert len(detail.json()['items']) == 1
    assert len(admin.get(url + '/api/v1/storage/packs/' + str(pack)).json()['items']) == count
    linked = member.get(url + '/api/v1/storage/packs/' + str(pack) + '/objects').json()
    assert linked['items'] and all(o['bucket_id'] == bucket['id'] for o in linked['items']), (linked, detail.json(), bucket['id'])
    chunks = member.get(url + f"/api/v1/buckets/{bucket['id']}/object/chunks", params={ 'key': key, 'version': str(source)})
    assert chunks.status_code == 200 and chunks.json()['items'], chunks.text
    assert all(c['block_key_id'] is None and c['reads'] is None for c in chunks.json()['items'])
    with psycopg.connect(connection, autocommit=True) as db:
        ids = [row[0] for row in db.execute('SELECT chunk_id FROM extents WHERE stream_id=%s AND chunk_id IS NOT NULL', (source,))]
        def access_counts():
            return db.execute('SELECT coalesce(sum(reads),0),coalesce(sum(range_reads),0),coalesce(sum(bytes),0) FROM chunk_access_stats WHERE chunk_id=ANY(%s)', (ids,)).fetchone()
        previous = access_counts()
        assert s3.get_object(Bucket=s3_bucket, Key=key)['Body'].read() == raw[:first_length]
        assert s3.get_object(Bucket=s3_bucket, Key=key, Range='bytes=0-31')['Body'].read() == raw[:32]
        deadline = time.monotonic() + 90
        while True:
            counts = access_counts()
            if counts[0] >= previous[0] + 2 and counts[1] >= previous[1] + 1 and counts[2] >= previous[2] + first_length + 32:
                break
            assert time.monotonic() < deadline, (previous, counts)
            time.sleep(.2)
        assert db.execute('SELECT count(*) FROM chunk_access_windows WHERE chunk_id=ANY(%s)', (ids,)).fetchone()[0] > 0
    print('PASS full and Range reads persist access totals and hourly windows', flush=True)
    def ready(client, query=''):
        deadline = time.monotonic() + 45
        while True:
            result = client.get(url + '/api/v1/usage/summary' + query, timeout=5)
            assert result.status_code == 200, result.text
            value = result.json()
            if value['usage'] is not None:
                return value
            assert time.monotonic() < deadline, value
            time.sleep(.25)
    global_usage = ready(admin)
    assert global_usage['scope'] == 'deployment' and global_usage['usage']['physical'] is not None
    inventory = cli('status')['storage']
    assert all(field in inventory['snapshot'] for field in ['logical_bytes', 'live', 'cleanup', 'database']), inventory
    assert 'storage_id' not in inventory['snapshot'], 'a child inventory replaced the deployment snapshot'
    assert inventory['last_error'] is None
    visible = ready(member)
    assert visible['scope'] == 'visible' and visible['bucket_count'] == 1 and visible['usage']['physical'] is None
    assert member.get(url + '/api/v1/usage/runtime').status_code == 403
    assert member.get(url + '/api/v1/usage/summary?bucket=' + str(uuid.uuid4())).status_code == 403
    runtime = admin.get(url + '/api/v1/usage/runtime').json()
    assert runtime['current']['rss_bytes'] is not None and runtime['history']
    assert len(runtime['history']) <= 2048
    start = time.monotonic()
    with psycopg.connect(connection) as db:
        db.execute('LOCK TABLE extents IN ACCESS EXCLUSIVE MODE')
        assert member.get(url + '/api/v1/usage/summary', timeout=3).status_code == 200
    assert time.monotonic() - start < 3
    for pack in member.get(url + '/api/v1/storage/packs').json()['items']:
        assert pack['raw_size'] is None and pack['stored_size'] is None and pack['member_count'] is None
        detail = member.get(url + '/api/v1/storage/packs/' + pack['id']).json()
        assert detail['scoped'] and all(m['ordinal'] is None and m['raw_size'] is None for m in detail['items'])
        linked = member.get(url + '/api/v1/storage/packs/' + pack['id'] + '/objects').json()
        assert all(o['bucket_id'] == bucket['id'] for o in linked['items'])
        first = member.get(url + '/api/v1/storage/packs/' + pack['id'], params={'limit': 1}).json()
        if first['next_cursor']:
            second = member.get(url + '/api/v1/storage/packs/' + pack['id'], params={'limit': 1, 'after': first['next_cursor']}).json()
            assert first['items'][0]['chunk_id'] != second['items'][0]['chunk_id']
    assert admin.delete(url + f"/api/v1/projects/{bucket['project_id']}/members/{user}").status_code == 204
    denied = member.get(url + '/api/v1/usage/summary?bucket=' + bucket['id'])
    assert denied.status_code == 403, denied.text
    empty = ready(member)
    assert empty['bucket_count'] == 0 and empty['usage']['logical_bytes'] == '0'
    assert member.get(url + '/api/v1/storage/packs').json()['items'] == []
    assert requests.get(url + '/api/v1/usage/summary').status_code == 403
    print('PASS cached reads avoid extent scans, scoped metadata and runtime privacy, live revocation and bounded persistent history', flush=True)
finally:
    admin.delete(url + '/api/v1/users/' + user)
    s3.delete_object(Bucket=s3_bucket, Key=key)
    if hidden:
        preview = admin.post(url + '/api/v1/buckets/' + hidden['id'] + '/purge/preview').json()
        admin.post(url + '/api/v1/buckets/' + hidden['id'] + '/purge', json={'confirm_name': hidden['name'], 'confirmation': preview['confirmation']})
    for policy in before['controls']:
        if policy['policy'] in ['pack', 'reuse', 'reclaim', 'repack', 'range']:
            cli('maintenance', 'pause' if policy['paused'] else 'resume', policy['policy'])
