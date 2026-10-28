"""Single-process refresh and task wake-up without PostgreSQL coordination."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import requests
from tests.integration import bucket, s3

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
dsn = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text()
db = psycopg.connect(dsn, autocommit=True)
observer = psycopg.connect(dsn, autocommit=True)
observer.execute('LISTEN mokyu_notifications')
observer.execute('LISTEN mokyu_tasks')
assert db.execute('SELECT deployment_mode FROM mokyu_meta WHERE singleton').fetchone()[0] == 'single'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
url = os.environ['MOKYU_TEST_WEB']
name = os.environ['MOKYU_TEST_STORAGE']
profile_url = url + '/api/v1/storages/' + name + '/profile'
document = Path(os.environ['MOKYU_TEST_DATA']) / 'coordination-profile.json'


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True, timeout=30))


def leader_calls():
    return db.execute("SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                      "WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                      "AND query='SELECT pg_try_advisory_xact_lock($1)'").fetchone()[0]


def isolated():
    assert not db.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                          "AND pid<>%s AND query LIKE 'LISTEN %%')", (observer.info.backend_pid,)).fetchone()[0]
    assert not db.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' "
                          "AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                          "AND classid=%s AND objid BETWEEN %s AND %s)",
                          (734922709851031 >> 32, 734922709851031 & 0xffffffff,
                           734922709851034 & 0xffffffff)).fetchone()[0]


admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url},
                   json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=15)
assert reply.status_code == 200
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
original = cli('storage', 'profile', 'show', name)
before = leader_calls()
keys = []
try:
    isolated()
    for compressed in (False, True):
        current = cli('storage', 'profile', 'show', name)
        profile = {**current, 'compression_enabled': compressed, 'upload_cache': False, 'pack_enabled': False}
        if compressed:
            reply = admin.put(profile_url, json=profile, timeout=20)
            assert reply.status_code == 200
            assert admin.put(profile_url, json=profile, timeout=20).status_code == 409
        else:
            document.write_text(json.dumps(profile))
            cli('storage', 'profile', 'set', name, str(document))
        key = 'coordination-' + uuid.uuid4().hex
        keys.append(key)
        raw = os.urandom(1024) * 2048
        s3.put_object(Bucket=bucket, Key=key, Body=raw)
        actual = db.execute('SELECT bool_and(c.standalone_compressed) FROM objects o '
                            'JOIN buckets b ON b.id=o.bucket_id JOIN extents x ON x.stream_id=o.stream_id '
                            'JOIN chunks c ON c.id=x.chunk_id WHERE b.name=%s AND o.object_key=%s',
                            (bucket, key)).fetchone()[0]
        assert actual is compressed, (compressed, actual)
        assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    job = cli('maintenance', 'run', 'cleanup')['task_id']
    deadline = time.monotonic() + 60
    while cli('task', 'show', job)['state'] != 'completed':
        assert time.monotonic() < deadline, 'local task did not complete'
        time.sleep(.1)
    assert not list(observer.notifies(timeout=.5)), 'single runtime sent a cluster notification'
    isolated()
    assert leader_calls() == before, 'single runtime attempted leader election'
    print('PASS single has no listeners, leader locks or notifications; CLI/Web profiles and queued work are immediate', flush=True)
finally:
    current = cli('storage', 'profile', 'show', name)
    document.write_text(json.dumps({**original, 'revision': current['revision']}))
    cli('storage', 'profile', 'set', name, str(document))
    document.unlink(missing_ok=True)
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
    observer.close()
    db.close()
