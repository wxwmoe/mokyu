"""Single task ownership does not expire or renew and fences paused physical work."""
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
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
storage = os.environ['MOKYU_TEST_STORAGE']
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
marker = faults / 'pack-before-publish'
finished = None
profile_file = Path(os.environ['MOKYU_TEST_DATA']) / 'execution-profile.json'
key = 'single-execution-' + uuid.uuid4().hex


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('single execution timed out')


def profile(**changes):
    current = cli('storage', 'profile', 'show', storage)
    profile_file.write_text(json.dumps({**current, **changes}))
    cli('storage', 'profile', 'set', storage, str(profile_file))


original = cli('storage', 'profile', 'show', storage)
try:
    profile(pack_enabled=False, upload_cache=False)
    raw = os.urandom(9 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    profile(pack_enabled=True)
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    job = cli('pack', 'run')['task_id']
    wait(marker.with_suffix('.hit').exists)
    token, node, expires = db.execute('SELECT lease_token,lease_node_id,lease_expires_at FROM tasks WHERE id=%s', (job,)).fetchone()
    assert token and node is None and expires is None
    prepared = [row[0] for row in db.execute("SELECT id FROM packs WHERE owner_task=%s AND state='preparing'", (job,))]
    assert prepared
    # A previous scheduler renewed at 30 seconds even while publication was held.
    time.sleep(33)
    assert db.execute('SELECT state,lease_token,lease_node_id,lease_expires_at FROM tasks WHERE id=%s', (job,)).fetchone() == ('running', token, None, None)
    finished = faults / ('task-finished-' + str(token))
    finished.with_suffix('.hit').unlink(missing_ok=True)
    finished.touch()
    cli('task', 'pause', job)
    cli('task', 'resume', job)
    marker.unlink()
    wait(finished.with_suffix('.hit').exists)
    assert db.execute('SELECT state FROM tasks WHERE id=%s', (job,)).fetchone()[0] == 'queued'
    assert db.execute("SELECT bool_and(state='preparing') FROM packs WHERE id=ANY(%s)", (prepared,)).fetchone()[0]
    finished.unlink()
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
    assert cli('task', 'show', job)['state'] == 'completed'
    assert db.execute("SELECT bool_and(state='retired') FROM packs WHERE id=ANY(%s)", (prepared,)).fetchone()[0]
    assert db.execute('SELECT lease_token,lease_node_id,lease_expires_at FROM tasks WHERE id=%s', (job,)).fetchone() == (None, None, None)
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    assert s3.get_object(Bucket=bucket, Key=key, Range='bytes=31-4097')['Body'].read() == raw[31:4098]
    print('PASS single task has no expiring lease, pause/resume fences stale publication, full and Range bytes survive', flush=True)
finally:
    marker.unlink(missing_ok=True)
    if finished:
        finished.unlink(missing_ok=True)
    original.pop('revision', None)
    profile(**original)
    s3.delete_object(Bucket=bucket, Key=key)
