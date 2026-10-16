"""Single queue idle cost, enqueue/wait race and persisted upload retry deadlines."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
from tests.integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
assert db.execute("SELECT deployment_mode='single' FROM mokyu_meta").fetchone()[0]
db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
sleeping = faults / 'scheduler-before-wait'
upload = faults / 'upload-before-work'
storage = os.environ['MOKYU_TEST_STORAGE']
profile_file = Path(os.environ['MOKYU_TEST_DATA']) / 'scheduler-profile.json'
keys = []


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate, seconds=15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.05)
    raise AssertionError('single scheduler did not wake before its fallback interval')


def arm(marker):
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()


def claims():
    return db.execute("SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                      "WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                      "AND query LIKE 'UPDATE tasks SET state=%%started_at=coalesce%%'").fetchone()[0]


original = cli('storage', 'profile', 'show', storage)
policies = dict(db.execute('SELECT policy,paused FROM maintenance_controls'))
try:
    for policy in policies:
        cli('maintenance', 'pause', policy)
    profile_file.write_text(json.dumps({**original, 'upload_cache': True, 'pack_enabled': False}))
    cli('storage', 'profile', 'set', storage, str(profile_file))
    assert cli('cache', 'status')['effective_upload_bytes'] > 0
    wait(lambda: db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0] == 0)
    time.sleep(.3)
    before = claims()
    time.sleep(4)
    delta = claims() - before
    assert delta <= 6, delta
    print('PASS idle single queue claim queries in four seconds:', delta, flush=True)

    arm(sleeping)
    cli('maintenance', 'resume', 'cleanup')
    wait(sleeping.with_suffix('.hit').exists)
    job = cli('maintenance', 'run', 'cleanup')['task_id']
    sleeping.unlink()
    wait(lambda: cli('task', 'show', job)['state'] == 'completed')
    print('PASS enqueue committed between scan and wait is not lost', flush=True)

    for failed in (False, True):
        raw = os.urandom(128 * 1024)
        key = 'single-scheduler-' + uuid.uuid4().hex
        keys.append(key)
        marker = upload if failed else sleeping
        arm(marker)
        if not failed:
            cli('maintenance', 'resume', 'cleanup')
            wait(sleeping.with_suffix('.hit').exists)
        s3.put_object(Bucket=bucket, Key=key, Body=raw)
        if failed:
            wait(upload.with_suffix('.hit').exists)
        ids = [row[0] for row in db.execute('SELECT p.chunk_id FROM pending_uploads p JOIN extents e ON e.chunk_id=p.chunk_id '
                                           'JOIN objects o ON o.stream_id=e.stream_id WHERE o.object_key=%s', (key,))]
        assert ids
        if failed:
            task = db.execute('SELECT owner_task FROM pending_uploads WHERE chunk_id=%s', (ids[0],)).fetchone()[0]
            assert task
            db.execute("UPDATE tasks SET state='failed',error='isolated retry fixture' WHERE id=%s", (task,))
        else:
            assert db.execute('SELECT bool_and(owner_task IS NULL) FROM pending_uploads WHERE chunk_id=ANY(%s)', (ids,)).fetchone()[0]
        db.execute("UPDATE pending_uploads SET next_retry_at=now()+interval '4 seconds' WHERE chunk_id=ANY(%s)", (ids,))
        started = time.monotonic()
        marker.unlink()
        wait(lambda: db.execute('SELECT count(*) FROM pending_uploads WHERE chunk_id=ANY(%s)', (ids,)).fetchone()[0] == 0)
        assert time.monotonic() - started >= 3
        assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
        assert s3.get_object(Bucket=bucket, Key=key, Range='bytes=31-4097')['Body'].read() == raw[31:4098]
        print('PASS timed retry without external wake:', 'failed owner' if failed else 'unclaimed upload', flush=True)
finally:
    sleeping.unlink(missing_ok=True)
    upload.unlink(missing_ok=True)
    current = cli('storage', 'profile', 'show', storage)
    profile_file.write_text(json.dumps({**original, 'revision': current['revision']}))
    cli('storage', 'profile', 'set', storage, str(profile_file))
    for policy, paused in policies.items():
        cli('maintenance', 'pause' if paused else 'resume', policy)
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
