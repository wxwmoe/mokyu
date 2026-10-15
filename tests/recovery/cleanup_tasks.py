"""Scoped cleanup tasks against disposable PostgreSQL and local/S3 staging.

Uses MOKYU_TEST_BINARY, MOKYU_TEST_CONFIG, MOKYU_TEST_DATABASE_FILE and
MOKYU_TEST_CREDENTIALS (a bucket credential). Run once in single mode and once
with control + worker in cluster mode. In single mode, the test arms a
fault-injection pause so the normal local sweeper cannot mask the scoped path;
MOKYU_TEST_DATA and MOKYU_TEST_FAULT_DIR expose its local directories.
In cluster mode, MOKYU_TEST_BACKEND is a JSON file with endpoint, region, bucket,
access_key and secret_key for the configured shared staging S3 fixture.
"""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']


def cli(*args):
    result = subprocess.run([*command, *args], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def scalar(query, args=()):
    row = db.execute(query, args).fetchone()
    return row[0] if row else None


def wait(predicate, message):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if value := predicate():
            return value
        time.sleep(.05)
    raise AssertionError(message)


def task_done(task):
    state, error = db.execute('SELECT state,error FROM tasks WHERE id=%s', (task,)).fetchone()
    assert state not in ('failed', 'paused'), (task, state, error)
    return state == 'completed'


def check_children(parent, history_task):
    wait(lambda: task_done(parent), 'cleanup parent did not complete')
    children = db.execute('SELECT state,detail,execution_scope,target_node_id,started_at FROM tasks '
                          'WHERE parent_task_id=%s', (parent,)).fetchall()
    assert len(children) == len(storages) + 1, children
    assert all(row[0] == 'completed' and row[4] is not None for row in children), children
    history = [row for row in children if row[1].get('phase') == 'history']
    assert len(history) == 1 and 'storage_id' not in history[0][1], children
    assert history[0][2] == 'cluster'
    report = history[0][1]['report']['last_run']
    assert report['last_error'] is None and report['deleted']['tasks'] >= 1, report
    assert not scalar('SELECT EXISTS(SELECT 1 FROM tasks WHERE id=%s)', (history_task,))
    scoped = {uuid.UUID(row[1]['storage_id']): row for row in children if 'storage_id' in row[1]}
    assert scoped.keys() == storages.keys(), children
    for storage, row in scoped.items():
        assert row[2] == ('node' if storages[storage] == 'local' else 'cluster'), row
        if storages[storage] == 'local':
            assert (row[3] is not None) == (mode == 'cluster'), row
    assert db.execute('SELECT batch_total,batch_completed FROM tasks WHERE id=%s', (parent,)).fetchone() == (len(children), len(children))
    return scoped


bucket = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())['bucket']
bucket_id, storage_id = db.execute('SELECT id,storage_id FROM buckets WHERE name=%s', (bucket,)).fetchone()
storages = dict(db.execute('SELECT id,type FROM storages WHERE enabled').fetchall())
mode = scalar('SELECT deployment_mode FROM mokyu_meta')
staging_id = None
staging_prefix = ''
backend = None
if mode == 'single':
    marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'local-cleanup'
    marker.touch()
    wait(lambda: marker.with_suffix('.hit').exists(), 'local sweeper was not isolated')
    directory = Path(os.environ['MOKYU_TEST_DATA']) / 'multipart'
    directory.mkdir(parents=True, exist_ok=True)
else:
    assert mode == 'cluster', mode
    staging_id = uuid.UUID(scalar("SELECT value->>'staging_storage_id' FROM runtime_settings WHERE section='uploads'"))
    settings = json.loads(Path(os.environ['MOKYU_TEST_BACKEND']).read_text())
    remote_bucket, staging_prefix = db.execute('SELECT remote_bucket_name,prefix FROM storages WHERE id=%s AND type=\'s3\'', (staging_id,)).fetchone()
    assert remote_bucket == settings['bucket'] and staging_id in storages
    backend = boto3.client('s3', endpoint_url=settings['endpoint'], region_name=settings['region'],
                           aws_access_key_id=settings['access_key'], aws_secret_access_key=settings['secret_key'],
                           config=Config(connect_timeout=3, read_timeout=3, retries={'max_attempts': 0},
                                         s3={'addressing_style': 'path'}))


def remote_key(key):
    return '/'.join(filter(None, [staging_prefix, key]))


def exists(fragment, key):
    if backend is None:
        return (directory / fragment.hex).exists()
    try:
        backend.head_object(Bucket=remote_bucket, Key=remote_key(key))
        return True
    except ClientError as error:
        assert error.response['Error']['Code'] in ('404', 'NoSuchKey'), error
        return False


def stream(state, lease=False):
    ident = uuid.uuid4()
    db.execute("INSERT INTO streams(id,bucket_id,object_key,stream_type,state,touched_at) "
               "VALUES(%s,%s,%s,'part',%s,now()-interval '100 days')", (ident, bucket_id, ident.hex, state))
    if lease:
        db.execute("INSERT INTO stream_leases(id,stream_id,lease_type,expires_at) "
                   "VALUES(%s,%s,'write',now()+interval '1 hour')", (uuid.uuid4(), ident))
    return ident


def chunk(owner, state):
    return scalar("INSERT INTO chunks(storage_id,owner_stream,hash,raw_size,encryption_algorithm,state) "
                  "VALUES(%s,%s,%s,64,'none',%s) RETURNING id", (storage_id, owner, os.urandom(32), state))


def fragment(owner, sealed):
    ident, body = uuid.uuid4(), os.urandom(64)
    key = '__mokyu/multipart/fragment/' + ident.hex if backend else None
    if backend:
        backend.put_object(Bucket=remote_bucket, Key=remote_key(key), Body=body)
        db.execute('INSERT INTO staging_objects(storage_id,object_key,bytes,created_at) '
                   "VALUES(%s,%s,%s,now()-interval '10 minutes')", (staging_id, key, len(body)))
    else:
        (directory / ident.hex).write_bytes(body)
    db.execute('INSERT INTO fragments(id,owner_stream,sealed,size,hash,staging_storage_id,staging_object_key) '
               'VALUES(%s,%s,%s,%s,%s,%s,%s)', (ident, owner, sealed, len(body), os.urandom(32), staging_id, key))
    assert exists(ident, key)
    return ident, key


def expired_task():
    ident = uuid.uuid4()
    db.execute("INSERT INTO tasks(id,task_type,state,updated_at) VALUES(%s,'sweep','completed',now()-interval '100 years')", (ident,))
    return ident


assert not scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='cleanup' AND state IN ('queued','running','paused'))")
cleanup_paused = scalar("SELECT paused FROM maintenance_controls WHERE policy='cleanup'")
try:
    retired, writing, live = stream('retired'), stream('writing', True), stream('ready')
    stale_writing = stream('writing')
    leased_retired = stream('retired')
    read_lease = uuid.uuid4()
    db.execute("INSERT INTO stream_leases(id,stream_id,lease_type,expires_at) "
               "VALUES(%s,%s,'read',now()+interval '1 hour')", (read_lease, leased_retired))
    owned = chunk(retired, 'preparing')
    detached, shared = chunk(None, 'ready'), chunk(None, 'ready')
    for sid, offset, cid in [(retired, 0, detached), (retired, 64, shared), (live, 0, shared)]:
        db.execute('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES(%s,%s,64,%s)', (sid, offset, cid))
    obsolete = fragment(retired, True)
    active = fragment(writing, False)
    referenced = fragment(live, True)
    db.execute('INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES(%s,128,64,%s)', (retired, obsolete[0]))
    db.execute('INSERT INTO extents(stream_id,offset_bytes,length,fragment_id) VALUES(%s,64,64,%s)', (live, referenced[0]))
    history_task = expired_task()
    orphan = '__mokyu/multipart/pending/' + uuid.uuid4().hex
    recent = '__mokyu/multipart/pending/' + uuid.uuid4().hex
    if backend:
        for key, age in [(orphan, 10), (recent, 0)]:
            backend.put_object(Bucket=remote_bucket, Key=remote_key(key), Body=b'orphan')
            db.execute('INSERT INTO staging_objects(storage_id,object_key,bytes,created_at) '
                       "VALUES(%s,%s,6,now()-%s*interval '1 minute')", (staging_id, key, age))
    if cleanup_paused:
        cli('maintenance', 'resume', 'cleanup')
    parent = cli('maintenance', 'run', 'cleanup')['task_id']
    scoped = check_children(parent, history_task)
    assert scoped[storage_id][1]['report']['deleted'] > 0, scoped
    assert scalar('SELECT count(*) FROM extents WHERE stream_id=%s', (retired,)) == 0
    assert db.execute('SELECT owner_stream,state,unreferenced_at IS NOT NULL FROM chunks WHERE id=%s', (owned,)).fetchone() == (None, 'failed', True)
    assert scalar('SELECT unreferenced_at IS NOT NULL FROM chunks WHERE id=%s', (detached,))
    assert scalar('SELECT unreferenced_at FROM chunks WHERE id=%s', (shared,)) is None
    assert not scalar('SELECT EXISTS(SELECT 1 FROM fragments WHERE id=%s)', (obsolete[0],))
    assert not exists(*obsolete), 'fragment metadata was removed without its physical data'
    if backend:
        assert not scalar('SELECT EXISTS(SELECT 1 FROM staging_objects WHERE storage_id=%s AND object_key=%s)', (staging_id, obsolete[1]))
        assert not exists(None, orphan)
        assert not scalar('SELECT EXISTS(SELECT 1 FROM staging_objects WHERE storage_id=%s AND object_key=%s)', (staging_id, orphan))
        assert exists(None, recent)
        assert scalar('SELECT EXISTS(SELECT 1 FROM staging_objects WHERE storage_id=%s AND object_key=%s)', (staging_id, recent))
    for keep in (active, referenced):
        assert scalar('SELECT EXISTS(SELECT 1 FROM fragments WHERE id=%s)', (keep[0],)) and exists(*keep)
    assert scalar('SELECT state FROM streams WHERE id=%s', (writing,)) == 'writing'
    assert not scalar('SELECT EXISTS(SELECT 1 FROM streams WHERE id=%s)', (stale_writing,))
    assert scalar('SELECT state FROM streams WHERE id=%s', (leased_retired,)) == 'retired'
    print('PASS cleanup task tree, history deletion, chunk reference timestamps and ' + mode + ' fragment reclamation/protection', flush=True)

    # A second pass finishes the now-empty stream and is safe to repeat.
    db.execute('DELETE FROM stream_leases WHERE id=%s', (read_lease,))
    history_task = expired_task()
    parent = cli('maintenance', 'run', 'cleanup')['task_id']
    check_children(parent, history_task)
    assert not scalar('SELECT EXISTS(SELECT 1 FROM streams WHERE id=%s)', (retired,))
    assert not scalar('SELECT EXISTS(SELECT 1 FROM streams WHERE id=%s)', (leased_retired,))
    for keep in (active, referenced):
        assert scalar('SELECT EXISTS(SELECT 1 FROM fragments WHERE id=%s)', (keep[0],)) and exists(*keep)
    print('PASS periodic history child, eventual stream removal and repeat cleanup', flush=True)
finally:
    if cleanup_paused:
        cli('maintenance', 'pause', 'cleanup')
    if mode == 'single':
        marker.unlink(missing_ok=True)
    db.close()
