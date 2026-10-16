"""Run staged cluster regressions around externally controlled role restarts."""
import hashlib
import json
import os
import time
import tomllib
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
from psycopg import sql
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
fixture = json.loads(Path(os.environ['MOKYU_TEST_CLUSTER_FIXTURE']).read_text())
phase = os.environ['MOKYU_TEST_CLUSTER_PHASE']
url = fixture['control']
db = psycopg.connect(Path(fixture['dsn']).read_text(), autocommit=True)
saved = Path(fixture['checkpoint'])
deployment_key = tomllib.loads(Path(fixture['config']).read_text())['instance']['deployment_key']
credentials = json.loads(Path(fixture['credentials']).read_text())
s3 = boto3.client('s3', endpoint_url=fixture['data'], region_name='us-east-1',
                  aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, request_checksum_calculation='when_required'))


def wait(predicate, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.15)
    raise AssertionError('cluster condition timed out')


def rpc(command, state=None, status=200):
    state = state or json.loads(Path(fixture['control_state']).read_text())
    reply = requests.post(url + '/api/v1/cli', json=command,
                          headers={'x-mokyu-node-id': state['node_id'],
                                   'x-mokyu-node-credential': state['credential']}, timeout=30)
    assert reply.status_code == status, (reply.status_code, reply.text)
    return reply.json().get('result')


def login():
    client = requests.Session()
    reply = client.post(url + '/api/v1/auth/login', headers={'Origin': url},
                        json={'username': 'tester', 'password': Path(fixture['password']).read_text()}, timeout=15)
    assert reply.status_code == 200
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    assert client.post(url + '/api/v1/account/reauth',
                       json={'password': Path(fixture['password']).read_text()}).status_code == 204
    return client


def complete(admin, task):
    def check():
        result = admin.get(url + '/api/v1/tasks/' + task, timeout=15)
        assert result.status_code == 200
        value = result.json()
        assert value['status'] not in ('failed', 'paused'), value
        return value['status'] == 'completed'
    wait(check, 120)


if phase == 'live':
    admin = login()
    status = rpc({'Status': {'ready': True}})
    assert status['role'] == 'control' and status['node_registered'] and not status['mode_mismatch']
    nodes = rpc({'Cluster': 'Nodes'})
    assert {row['role'] for row in nodes if row['enabled']} == {'control', 'data', 'worker'}
    assert admin.get(url + '/api/v1/cluster/nodes').json() == nodes
    cluster = admin.get(url + '/api/v1/cluster/status').json()
    assert cluster == rpc({'Cluster': 'Status'}) == {'mode': 'cluster', 'enabled_nodes': 3, 'local_storages': 0}
    assert admin.post(url + '/api/v1/cluster/mode', json={'mode': 'single'}).status_code >= 400
    assert rpc({'Cluster': 'Status'})['mode'] == 'cluster'
    seen = dict(db.execute('SELECT id,last_seen_at FROM nodes WHERE enabled').fetchall())
    wait(lambda: all(stamp > seen[id] for id, stamp in db.execute('SELECT id,last_seen_at FROM nodes WHERE enabled').fetchall()), 25)
    raw = os.urandom(9 * 1024 * 1024 + 17)
    s3.put_object(Bucket=fixture['bucket'], Key='cluster-source', Body=raw)
    assert s3.get_object(Bucket=fixture['bucket'], Key='cluster-source')['Body'].read() == raw
    assert s3.get_object(Bucket=fixture['bucket'], Key='cluster-source', Range='bytes=31-4097')['Body'].read() == raw[31:4098]
    job = admin.post(url + '/api/v1/maintenance/pack/actions', json={'action': 'run'}, timeout=20)
    assert job.status_code == 200
    complete(admin, job.json()['task_id'])
    assert db.execute('SELECT count(*) FROM tasks WHERE parent_task_id=%s', (job.json()['task_id'],)).fetchone()[0] > 0
    assert s3.get_object(Bucket=fixture['bucket'], Key='cluster-source')['Body'].read() == raw
    token = rpc({'Token': {'Create': {'username': 'tester', 'file': '-', 'document': {'label': 'cluster-test', 'expires_in': '1h'}}}})
    assert token['token']['active']
    rpc({'Token': {'Revoke': {'id': token['token']['id']}}})
    print('PASS real control/data/worker registration, heartbeats, guarded mode change, S3/Range, child work and node CLI tokens', flush=True)

    worker = next(row for row in nodes if row['role'] == 'worker')
    task, permit, lease = [uuid.uuid4() for _ in range(3)]
    trigger = '_cluster_release_' + uuid.uuid4().hex[:12]
    try:
        db.execute("INSERT INTO tasks(id,task_type,state,execution_scope,target_node_id,lease_node_id,lease_token,lease_expires_at) "
                   "VALUES(%s,'pack','running','node',%s,%s,%s,now()+interval '5 minutes')",
                   (task, worker['id'], worker['id'], lease))
        db.execute("INSERT INTO storage_permits(token,storage_id,direction,holder_id,expires_at) VALUES(%s,%s,'control',%s,now()+interval '5 minutes')",
                   (permit, fixture['storage'], worker['id']))
        db.execute(sql.SQL("CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'controlled release failure'; END$$").format(sql.Identifier(trigger)))
        db.execute(sql.SQL('CREATE TRIGGER {} BEFORE DELETE ON storage_permits FOR EACH ROW WHEN (OLD.token={}) EXECUTE FUNCTION {}()')
                   .format(sql.Identifier(trigger), sql.Literal(permit), sql.Identifier(trigger)))
        failed = admin.post(url + '/api/v1/cluster/nodes/' + worker['name'] + '/release', json={'confirm': True})
        assert failed.status_code >= 400
        assert db.execute('SELECT enabled FROM nodes WHERE id=%s', (worker['id'],)).fetchone()[0]
        assert db.execute('SELECT state,lease_token FROM tasks WHERE id=%s', (task,)).fetchone() == ('running', lease)
        assert db.execute('SELECT count(*) FROM storage_permits WHERE token=%s', (permit,)).fetchone()[0] == 1
        db.execute(sql.SQL('DROP TRIGGER {} ON storage_permits').format(sql.Identifier(trigger)))
        db.execute(sql.SQL('DROP FUNCTION {}()').format(sql.Identifier(trigger)))
        released = rpc({'Cluster': {'Release': {'name': worker['name'], 'confirm': True}}})
        assert released['released'] == worker['name']
        assert db.execute('SELECT enabled,credential_encrypted FROM nodes WHERE id=%s', (worker['id'],)).fetchone() == (False, None)
        assert db.execute('SELECT state,lease_token,lease_node_id FROM tasks WHERE id=%s', (task,)).fetchone() == ('paused', None, None)
        assert db.execute('SELECT count(*) FROM storage_permits WHERE token=%s', (permit,)).fetchone()[0] == 0
        saved.write_text(json.dumps({'digest': hashlib.sha256(raw).hexdigest(), 'worker': worker['id'],
                                     'old_control': json.loads(Path(fixture['control_state']).read_text())}))
    finally:
        db.execute(sql.SQL('DROP TRIGGER IF EXISTS {} ON storage_permits').format(sql.Identifier(trigger)))
        db.execute(sql.SQL('DROP FUNCTION IF EXISTS {}()').format(sql.Identifier(trigger)))
        db.execute('DELETE FROM storage_permits WHERE token=%s', (permit,))
        db.execute('DELETE FROM tasks WHERE id=%s', (task,))
    print('PASS failed release rolls back registry/task/permit changes; CLI release commits them together', flush=True)
elif phase == 'rejoin':
    checkpoint = json.loads(saved.read_text())
    state = json.loads(Path(fixture['worker_state']).read_text())
    assert state['node_id'] == checkpoint['worker']
    wait(lambda: rpc({'Cluster': 'Status'})['enabled_nodes'] == 3)
    assert not db.execute('SELECT recovery_required FROM nodes WHERE id=%s', (state['node_id'],)).fetchone()[0]
    assert hashlib.sha256(s3.get_object(Bucket=fixture['bucket'], Key='cluster-source')['Body'].read()).hexdigest() == checkpoint['digest']
    print('PASS released worker rejoins with the same registry ID and shared objects remain readable', flush=True)
elif phase == 'recover':
    checkpoint = json.loads(saved.read_text())
    assert not Path(fixture['control_state']).exists()
    assert db.execute("SELECT recovery_required FROM nodes WHERE name='cluster-control'").fetchone()[0]
    denied = requests.post(url + '/api/v1/cluster/recover', json={'confirm': True}, timeout=15)
    assert denied.status_code == 403
    restored = requests.post(url + '/api/v1/cluster/recover', headers={'x-mokyu-deployment-key': deployment_key},
                             json={'confirm': True}, timeout=30)
    assert restored.status_code == 200, (restored.status_code, restored.text)
    current = json.loads(Path(fixture['control_state']).read_text())
    assert current['node_id'] == checkpoint['old_control']['node_id']
    assert current['credential'] != checkpoint['old_control']['credential']
    rpc({'Status': {'ready': True}}, checkpoint['old_control'], status=500)
    print('PASS missing control identity requires explicit deployment-key recovery and rotates the credential', flush=True)
elif phase == 'runtime':
    admin = login()
    status = rpc({'Status': {'ready': True}})
    assert status['deployment_mode'] == 'cluster' and not status['mode_mismatch']
    assert 0 <= status['db_pool_idle'] <= status['db_pool_size']
    def listeners():
        return db.execute("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                          "AND rtrim(query, ';') IN ('LISTEN \"mokyu_storage_profiles\"', 'LISTEN \"mokyu_tasks\"')").fetchall()
    wait(lambda: len(listeners()) == 5)
    previous = listeners()
    for (pid,) in previous:
        db.execute('SELECT pg_terminate_backend(%s)', (pid,))
    wait(lambda: len(listeners()) == 5 and not set(previous).intersection(listeners()), 30)
    observer = psycopg.connect(Path(fixture['dsn']).read_text(), autocommit=True)
    observer.execute('LISTEN mokyu_storage_profiles')
    observer.execute('LISTEN mokyu_tasks')
    seen = dict(db.execute('SELECT id,last_seen_at FROM nodes WHERE enabled').fetchall())
    wait(lambda: all(stamp > seen[id] for id, stamp in db.execute('SELECT id,last_seen_at FROM nodes WHERE enabled').fetchall()), 25)
    name = db.execute('SELECT name FROM storages WHERE id=%s', (fixture['storage'],)).fetchone()[0]
    profile_url = url + '/api/v1/storages/' + name + '/profile'
    original = admin.get(profile_url, timeout=15).json()
    key = 'profile-notification-' + uuid.uuid4().hex
    payload = {}
    try:
        reply = admin.put(profile_url, json={**original, 'compression_enabled': True,
                                            'pack_enabled': False, 'upload_cache': False}, timeout=20)
        assert reply.status_code == 200, reply.status_code
        def refreshed():
            payload['raw'] = b''.join(os.urandom(1024) * 1024 for _ in range(9))
            s3.put_object(Bucket=fixture['bucket'], Key=key, Body=payload['raw'])
            return db.execute('SELECT bool_and(c.standalone_compressed) FROM objects o '
                              'JOIN buckets b ON b.id=o.bucket_id JOIN extents x ON x.stream_id=o.stream_id '
                              'JOIN chunks c ON c.id=x.chunk_id '
                              'WHERE b.name=%s AND o.object_key=%s', (fixture['bucket'], key)).fetchone()[0]
        wait(refreshed, 15)
        assert s3.get_object(Bucket=fixture['bucket'], Key=key)['Body'].read() == payload['raw']
        wait(lambda: not db.execute('SELECT EXISTS(SELECT 1 FROM pending_uploads p JOIN extents x ON x.chunk_id=p.chunk_id '
                                    'JOIN objects o ON o.stream_id=x.stream_id JOIN buckets b ON b.id=o.bucket_id '
                                    'WHERE b.name=%s AND o.object_key=%s)', (fixture['bucket'], key)).fetchone()[0])
        current = admin.get(profile_url, timeout=15).json()
        assert admin.put(profile_url, json={**current, 'pack_enabled': True}, timeout=20).status_code == 200
        job = admin.post(url + '/api/v1/maintenance/pack/actions', json={'action': 'run'}, timeout=20)
        assert job.status_code == 200
        complete(admin, job.json()['task_id'])
        assert db.execute('SELECT bool_and(p.pack_compressed) FROM objects o JOIN buckets b ON b.id=o.bucket_id '
                          'JOIN extents x ON x.stream_id=o.stream_id JOIN chunks c ON c.id=x.chunk_id '
                          'JOIN packs p ON p.id=c.pack_id WHERE b.name=%s AND o.object_key=%s',
                          (fixture['bucket'], key)).fetchone()[0]
        assert s3.get_object(Bucket=fixture['bucket'], Key=key)['Body'].read() == payload['raw']
        channels = {notice.channel for notice in observer.notifies(timeout=.5)}
        assert channels == {'mokyu_storage_profiles', 'mokyu_tasks'}, channels
    finally:
        observer.close()
        current = admin.get(profile_url, timeout=15).json()
        reply = admin.put(profile_url, json={**original, 'revision': current['revision']}, timeout=20)
        assert reply.status_code == 200
        s3.delete_object(Bucket=fixture['bucket'], Key=key)
    print('PASS listener reconnection, runtime status, heartbeats and cross-node profiles preserve compressed chunk/pack bytes', flush=True)
elif phase == 'execution':
    admin = login()
    directories = {role: Path('/data/cluster') / role / 'faults' for role in ('data', 'worker')}
    assert all(path.is_dir() for path in directories.values()), 'start both roles with their isolated fault directories'
    name = db.execute('SELECT name FROM storages WHERE id=%s', (fixture['storage'],)).fetchone()[0]
    profile_url = url + '/api/v1/storages/' + name + '/profile'
    original = admin.get(profile_url, timeout=15).json()
    key = 'execution-fence-' + uuid.uuid4().hex
    markers = []
    try:
        assert admin.put(profile_url, json={**original, 'pack_enabled': False, 'upload_cache': False,
                                            'compression_enabled': True}, timeout=20).status_code == 200
        time.sleep(.5)
        raw = b''.join(os.urandom(1024) * 1024 for _ in range(9))
        s3.put_object(Bucket=fixture['bucket'], Key=key, Body=raw)
        def independent_sources():
            return db.execute("SELECT count(DISTINCT c.id)>1 AND bool_and(c.pack_id IS NULL AND l.state='ready' AND u.chunk_id IS NULL) "
                              'FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents x ON x.stream_id=o.stream_id '
                              'JOIN chunks c ON c.id=x.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id '
                              'LEFT JOIN pending_uploads u ON u.chunk_id=c.id WHERE b.name=%s AND o.object_key=%s',
                              (fixture['bucket'], key)).fetchone()[0]
        wait(independent_sources, 30)
        current = admin.get(profile_url, timeout=15).json()
        assert admin.put(profile_url, json={**current, 'pack_enabled': True}, timeout=20).status_code == 200
        time.sleep(.5)
        for directory in directories.values():
            marker = directory / 'pack-before-publish'
            marker.with_suffix('.hit').unlink(missing_ok=True)
            marker.touch()
            markers.append(marker)
        reply = admin.post(url + '/api/v1/maintenance/pack/actions', json={'action': 'run'}, timeout=20)
        assert reply.status_code == 200
        parent = reply.json()['task_id']
        def first_execution():
            row = db.execute("SELECT t.id,n.role,t.lease_token FROM tasks t JOIN nodes n ON n.id=t.lease_node_id "
                             "WHERE t.parent_task_id=%s AND t.state='running'", (parent,)).fetchone()
            return row if row and (directories[row[1]] / 'pack-before-publish.hit').exists() else None
        task, role, token = wait(first_execution, 120)
        old_packs = [row[0] for row in db.execute("SELECT id FROM packs WHERE owner_task=%s AND state='preparing'", (task,))]
        assert old_packs
        finished = directories[role] / ('task-finished-' + str(token))
        finished.touch()
        markers.append(finished)
        for action in ('pause', 'resume'):
            assert admin.post(url + '/api/v1/tasks/' + parent + '/actions', json={'action': action}, timeout=20).status_code == 204
        other = 'worker' if role == 'data' else 'data'
        wait(lambda: (directories[other] / 'pack-before-publish.hit').exists(), 120)
        next_token, next_role = db.execute('SELECT t.lease_token,n.role FROM tasks t JOIN nodes n ON n.id=t.lease_node_id WHERE t.id=%s', (task,)).fetchone()
        assert next_token != token and next_role == other
        new_packs = [row[0] for row in db.execute("SELECT id FROM packs WHERE owner_task=%s AND state='preparing'", (task,))]
        assert new_packs and set(old_packs).isdisjoint(new_packs)
        (directories[role] / 'pack-before-publish').unlink()
        wait(finished.with_suffix('.hit').exists, 60)
        assert db.execute('SELECT state,lease_token FROM tasks WHERE id=%s', (task,)).fetchone() == ('running', next_token)
        assert db.execute("SELECT bool_and(state='preparing') FROM packs WHERE id=ANY(%s)", (new_packs,)).fetchone()[0]
        assert db.execute("SELECT bool_and(state='retired') FROM packs WHERE id=ANY(%s)", (old_packs,)).fetchone()[0]
        assert db.execute('SELECT count(*) FROM pack_inputs WHERE task_id=%s', (task,)).fetchone()[0] > 0
        for marker in markers:
            marker.unlink(missing_ok=True)
        complete(admin, parent)
        assert s3.get_object(Bucket=fixture['bucket'], Key=key)['Body'].read() == raw
        assert s3.get_object(Bucket=fixture['bucket'], Key=key, Range='bytes=31-4097')['Body'].read() == raw[31:4098]
        assert db.execute('SELECT count(*) FROM pack_inputs WHERE task_id=%s', (task,)).fetchone()[0] == 0
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        current = admin.get(profile_url, timeout=15).json()
        assert admin.put(profile_url, json={**original, 'revision': current['revision']}, timeout=20).status_code == 200
        s3.delete_object(Bucket=fixture['bucket'], Key=key)
    print('PASS actual data/worker handoff rejects stale publication and cleanup; resumed pack retains full and Range bytes', flush=True)
elif phase == 'usage':
    admin = login()
    def latest():
        row = db.execute("SELECT id FROM tasks WHERE task_type='usage' AND parent_task_id IS NULL ORDER BY created_at DESC LIMIT 1").fetchone()
        return str(row[0]) if row else None
    task = wait(latest)
    complete(admin, task)
    storages = {str(row[0]) for row in db.execute('SELECT id FROM storages WHERE enabled')}
    children = db.execute('SELECT state,detail,processed FROM tasks WHERE parent_task_id=%s', (task,)).fetchall()
    assert {detail['storage_id'] for _, detail, _ in children} == storages
    assert all(state == 'completed' and count == 1 for state, _, count in children)
    assert db.execute('SELECT batch_total,batch_completed FROM tasks WHERE id=%s', (task,)).fetchone() == (len(children), len(children))
    inventory = rpc({'Status': {'ready': False}})['storage']
    assert inventory['snapshot'] and inventory['last_error'] is None
    assert 'storage_id' not in inventory['snapshot']
    assert db.execute('SELECT count(*) FROM usage_history').fetchone()[0] > 0
    print('PASS cluster usage children, snapshot publication, aggregate progress and persisted history', flush=True)
elif phase == 'lifecycle':
    admin = login()
    original = dict(db.execute("SELECT policy,paused FROM maintenance_controls WHERE policy IN ('gc','cleanup')"))
    storages = {str(row[0]) for row in db.execute('SELECT id FROM storages WHERE enabled')}
    expired = uuid.uuid4()
    db.execute("INSERT INTO tasks(id,task_type,state,updated_at) VALUES(%s,'sweep','completed',now()-interval '100 years')", (expired,))
    try:
        for policy in ('cleanup', 'gc'):
            endpoint = url + '/api/v1/maintenance/' + policy + '/actions'
            assert admin.post(endpoint, json={'action': 'resume'}, timeout=15).status_code == 200
            reply = admin.post(endpoint, json={'action': 'run'}, timeout=20)
            reply.raise_for_status()
            task = reply.json()['task_id']
            complete(admin, task)
            children = db.execute('SELECT state,detail,started_at FROM tasks WHERE parent_task_id=%s', (task,)).fetchall()
            assert len(children) == len(storages) + (policy == 'cleanup'), children
            assert all(state == 'completed' and detail['role'] == 'child' and started for state, detail, started in children)
            assert {detail['storage_id'] for _, detail, _ in children if 'storage_id' in detail} == storages
            assert db.execute('SELECT batch_total,batch_completed FROM tasks WHERE id=%s', (task,)).fetchone() == (len(children), len(children))
            if policy == 'cleanup':
                history = [detail for _, detail, _ in children if detail.get('phase') == 'history']
                assert len(history) == 1 and history[0]['report']['last_run']['deleted']['tasks'] >= 1
                assert not db.execute('SELECT EXISTS(SELECT 1 FROM tasks WHERE id=%s)', (expired,)).fetchone()[0]
        checkpoint = json.loads(saved.read_text())
        assert hashlib.sha256(s3.get_object(Bucket=fixture['bucket'], Key='cluster-source')['Body'].read()).hexdigest() == checkpoint['digest']
    finally:
        for policy, paused in original.items():
            assert admin.post(url + '/api/v1/maintenance/' + policy + '/actions',
                              json={'action': 'pause' if paused else 'resume'}, timeout=15).status_code == 200
    print('PASS cluster cleanup/history and GC children, aggregated progress, policy controls and retained data', flush=True)
elif phase == 'rate':
    admin = login()
    name = db.execute('SELECT name FROM storages WHERE id=%s', (fixture['storage'],)).fetchone()[0]
    endpoint = url + '/api/v1/storages/' + name + '/profile'
    original = admin.get(endpoint, timeout=15).json()
    db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')

    def rate_calls():
        return db.execute("SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                          "WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                          "AND query LIKE '%%storage_rate_state%%' AND query NOT LIKE '%%pg_stat_statements%%'").fetchone()[0]

    before = rate_calls()
    try:
        reply = admin.put(endpoint, json={**original, 'integrity_rps': 16, 'integrity_bandwidth': '64MiB'}, timeout=15)
        assert reply.status_code == 200
        reply = admin.post(url + '/api/v1/integrity', json={'mode': 'full', 'bucket': fixture['bucket'], 'key': 'cluster-source'}, timeout=15)
        assert reply.status_code == 200
        task = reply.json()['task_id']
        complete(admin, task)
        checked, issues = db.execute("SELECT coalesce(sum((detail->>'chunks_checked')::bigint),0), "
                                     "coalesce(sum((detail->>'issues')::bigint),0) FROM tasks WHERE parent_task_id=%s", (task,)).fetchone()
        assert checked > 0 and issues == 0, (checked, issues)
        assert rate_calls() > before
        assert db.execute('SELECT count(*) FROM storage_rate_state WHERE storage_id=%s', (fixture['storage'],)).fetchone()[0] == 2
    finally:
        current = admin.get(endpoint, timeout=15).json()
        reply = admin.put(endpoint, json={**original, 'revision': current['revision']}, timeout=15)
        assert reply.status_code == 200
    print('PASS cluster full integrity checks retain shared request and byte rate accounting', flush=True)
elif phase == 'permits':
    admin = login()
    name = db.execute('SELECT name FROM storages WHERE id=%s', (fixture['storage'],)).fetchone()[0]
    endpoint = url + '/api/v1/storages/' + name + '/profile'
    reply = admin.get(endpoint, timeout=15)
    assert reply.status_code == 200
    original = reply.json()
    marker = Path('/data/cluster/data/faults/pack-download')
    try:
        reply = admin.put(endpoint, json={**original, 'read_concurrency': 1, 'cache_enabled': False}, timeout=15)
        assert reply.status_code == 200
        job = rpc({'Pack': {'Run': {'mode': 'pack'}}})
        complete(admin, job['task_id'])
        assert db.execute("SELECT EXISTS(SELECT 1 FROM objects o JOIN extents e ON e.stream_id=o.stream_id "
                          "JOIN chunks c ON c.id=e.chunk_id WHERE o.object_key='cluster-source' AND c.pack_id IS NOT NULL)").fetchone()[0]
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        with ThreadPoolExecutor(max_workers=1) as pool:
            read = pool.submit(lambda: s3.get_object(Bucket=fixture['bucket'], Key='cluster-source')['Body'].read())
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or read.done())
                assert not read.done(), 'pack read did not reach its shared permit barrier'
                assert db.execute("SELECT count(*) FROM storage_permits WHERE storage_id=%s "
                                  "AND direction='read' AND expires_at>now()", (fixture['storage'],)).fetchone()[0] == 1
            finally:
                marker.unlink(missing_ok=True)
            assert hashlib.sha256(read.result(timeout=30)).hexdigest() == json.loads(saved.read_text())['digest']
        wait(lambda: db.execute("SELECT count(*) FROM storage_permits WHERE storage_id=%s AND direction='read'",
                                (fixture['storage'],)).fetchone()[0] == 0)
    finally:
        marker.unlink(missing_ok=True)
        current = admin.get(endpoint, timeout=15).json()
        reply = admin.put(endpoint, json={**original, 'revision': current['revision']}, timeout=15)
        assert reply.status_code == 200
    print('PASS cluster packed reads acquire/release the shared storage read permit and preserve bytes', flush=True)
elif phase == 'streams':
    from tests.recovery.stream_lifetime import run
    run(s3, fixture['bucket'], db, Path('/data/cluster/data/faults'), lambda: rpc({'Gc': 'Run'}), False)
    print('PASS cluster data readers and writers are protected from control-node GC by database leases', flush=True)
elif phase == 'claims':
    marker = Path('/data/cluster/data/faults/upload-cache-committed')
    keys = ['claim-' + uuid.uuid4().hex for _ in range(2)]
    raw = os.urandom(32768)
    try:
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        with ThreadPoolExecutor(max_workers=2) as pool:
            upload = pool.submit(s3.put_object, Bucket=fixture['bucket'], Key=keys[0], Body=raw)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or upload.done())
                assert not upload.done(), 'cluster write did not reach the persistence barrier'
                row = db.execute('SELECT c.id,c.storage_id,c.hash FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id '
                                 'WHERE s.object_key=%s', (keys[0],)).fetchone()
                assert row
                with psycopg.connect(Path(fixture['dsn']).read_text()) as probe:
                    assert not probe.execute("SELECT pg_try_advisory_xact_lock(hashtextextended(%s::uuid::text || ':' || encode(%s::bytea,'hex'),734922709851001))",
                                             (row[1], row[2])).fetchone()[0]
                duplicate = pool.submit(s3.put_object, Bucket=fixture['bucket'], Key=keys[1], Body=raw)
                time.sleep(.2)
                assert not duplicate.done()
            finally:
                marker.unlink(missing_ok=True)
            upload.result(timeout=30)
            duplicate.result(timeout=30)
        for key in keys:
            assert s3.get_object(Bucket=fixture['bucket'], Key=key)['Body'].read() == raw
        ids = db.execute('SELECT e.chunk_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id '
                         'WHERE o.object_key=ANY(%s)', (keys,)).fetchall()
        assert ids == [(row[0],), (row[0],)]
    finally:
        marker.unlink(missing_ok=True)
        for key in keys:
            s3.delete_object(Bucket=fixture['bucket'], Key=key)
    print('PASS cluster hash guard excludes another database session until persistence and preserves concurrent deduplication', flush=True)
elif phase == 'rewrite':
    admin = login()
    name = db.execute('SELECT name FROM storages WHERE id=%s', (fixture['storage'],)).fetchone()[0]
    endpoint = url + '/api/v1/storages/' + name + '/rewrite'
    key = 'rewrite-cluster-' + uuid.uuid4().hex
    raw = os.urandom(9 * 1024 * 1024 + 23)
    s3.put_object(Bucket=fixture['bucket'], Key=key, Body=raw)
    try:
        wait(lambda: not db.execute('SELECT EXISTS(SELECT 1 FROM pending_uploads p JOIN extents e ON e.chunk_id=p.chunk_id '
                                    'JOIN objects o ON o.stream_id=e.stream_id JOIN buckets b ON b.id=o.bucket_id '
                                    'WHERE b.name=%s AND o.object_key=%s)', (fixture['bucket'], key)).fetchone()[0])
        reply = admin.post(endpoint, json={'action': 'preview'}, timeout=20)
        assert reply.status_code == 200
        preview = reply.json()
        assert preview['preview']['loose_chunks'] + preview['preview']['packed_chunks'] > 0
        reply = admin.post(endpoint, json={'action': 'start', 'confirmation': preview['confirmation']}, timeout=20)
        assert reply.status_code == 200
        job = reply.json()
        assert job['children'] > 1
        parent = job['task_id']
        assert db.execute('SELECT count(*) FROM tasks WHERE parent_task_id=%s', (parent,)).fetchone()[0] == job['children']
        complete(admin, parent)
        assert db.execute("SELECT bool_and(state='completed' AND detail->>'role'='child') FROM tasks WHERE parent_task_id=%s",
                          (parent,)).fetchone()[0]
        reply = admin.post(endpoint, json={'action': 'status'}, timeout=20)
        assert reply.status_code == 200 and reply.json()['id'] == parent
        assert s3.get_object(Bucket=fixture['bucket'], Key=key)['Body'].read() == raw
        assert s3.get_object(Bucket=fixture['bucket'], Key=key, Range='bytes=999-9999')['Body'].read() == raw[999:10000]
        reply = admin.post(endpoint, json={'action': 'preview'}, timeout=20)
        assert reply.status_code == 200
        assert reply.json()['preview']['loose_chunks'] == reply.json()['preview']['packed_chunks'] == 0
    finally:
        s3.delete_object(Bucket=fixture['bucket'], Key=key)
    print('PASS cluster rewrite retains shards, task lookup and full/Range bytes', flush=True)
elif phase == 'final':
    checkpoint = json.loads(saved.read_text())
    assert rpc({'Status': {'ready': True}})['node_registered']
    assert rpc({'Cluster': 'Status'})['enabled_nodes'] == 3
    assert hashlib.sha256(s3.get_object(Bucket=fixture['bucket'], Key='cluster-source')['Body'].read()).hexdigest() == checkpoint['digest']
    assert db.execute('SELECT bool_and(enabled AND NOT recovery_required) FROM nodes').fetchone()[0]
    print('PASS recovered control restarts with all roles healthy and data intact', flush=True)
else:
    raise ValueError('unknown cluster regression phase')
db.close()
