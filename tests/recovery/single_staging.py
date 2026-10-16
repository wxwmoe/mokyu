"""Single local pending bytes and multipart work without the shared staging ledger."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
from tests.integration import s3, bucket, multipart

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
assert db.execute("SELECT deployment_mode='single' FROM mokyu_meta").fetchone()[0]
db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
storage = os.environ['MOKYU_TEST_STORAGE']
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'upload-before-work'
profile_file = Path(os.environ['MOKYU_TEST_DATA']) / 'staging-profile.json'
key = 'single-staging-' + uuid.uuid4().hex


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError('single staging operation timed out')


def calls():
    return db.execute("SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                      "WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                      "AND query LIKE '%staging_objects%'").fetchone()[0]


original = cli('storage', 'profile', 'show', storage)
paused = db.execute("SELECT paused FROM maintenance_controls WHERE policy='cleanup'").fetchone()[0]
before = calls()
try:
    profile_file.write_text(json.dumps({**original, 'upload_cache': True, 'pack_enabled': False}))
    cli('storage', 'profile', 'set', storage, str(profile_file))
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    raw = os.urandom(6 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    wait(marker.with_suffix('.hit').exists)
    assert db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0] > 0
    cli('maintenance', 'resume', 'cleanup')
    task = cli('maintenance', 'run', 'cleanup')['task_id']
    wait(lambda: cli('task', 'show', task)['state'] == 'completed')
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    assert s3.get_object(Bucket=bucket, Key=key, Range='bytes=31-4097')['Body'].read() == raw[31:4098]
    multipart()
    marker.unlink()
    wait(lambda: db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0] == 0)
    assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
    assert calls() == before, 'single runtime queried the shared staging ledger'
    print('PASS local pending source survives cleanup and multipart; shared staging SQL count is zero', flush=True)
finally:
    marker.unlink(missing_ok=True)
    current = cli('storage', 'profile', 'show', storage)
    profile_file.write_text(json.dumps({**original, 'revision': current['revision']}))
    cli('storage', 'profile', 'set', storage, str(profile_file))
    cli('maintenance', 'pause' if paused else 'resume', 'cleanup')
    s3.delete_object(Bucket=bucket, Key=key)
