"""An integrity budget survives profile reload and a paused rate-limited task."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
from tests.integration import s3, bucket, read

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
assert db.execute('SELECT deployment_mode FROM mokyu_meta WHERE singleton').fetchone()[0] == 'single'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = os.environ['MOKYU_TEST_STORAGE']
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'integrity-rate-waiting'
document = Path(os.environ['MOKYU_TEST_DATA']) / 'rate-profile.json'
key, raw = 'single-rate-' + uuid.uuid4().hex, os.urandom(128 * 1024)
job = None


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True, timeout=20))


def wait(predicate):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if value := predicate():
            return value
        time.sleep(.03)
    raise AssertionError('integrity rate condition timed out')


def done(task):
    state = cli('task', 'show', task)['state']
    assert state not in ('failed', 'paused'), state
    return state == 'completed'


original = cli('storage', 'profile', 'show', name)


def configure(bandwidth):
    current = cli('storage', 'profile', 'show', name)
    document.write_text(json.dumps({**current, 'integrity_rps': 16, 'integrity_bandwidth': bandwidth}))
    cli('storage', 'profile', 'set', name, str(document))


try:
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    configure('4KiB')
    job = cli('integrity', 'check', '--mode', 'full', '--bucket', bucket, '--key', key)['task_id']
    wait(lambda: done(job))
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    configure('4KiB')
    job = cli('integrity', 'check', '--mode', 'full', '--bucket', bucket, '--key', key)['task_id']
    wait(lambda: marker.with_suffix('.hit').exists() or done(job))
    assert marker.with_suffix('.hit').exists(), 'profile reload reset the consumed byte budget'
    cli('task', 'pause', job)
    assert cli('task', 'show', job)['state'] == 'paused'
    configure('256MiB')
    marker.unlink()
    cli('task', 'resume', job)
    wait(lambda: done(job))
    assert read(key)[0] == raw
    assert db.execute('SELECT count(*) FROM storage_rate_state').fetchone()[0] == 0
    print('PASS single rate debt survives profile reload; paused waiter resumes under the new budget', flush=True)
finally:
    marker.unlink(missing_ok=True)
    current = cli('storage', 'profile', 'show', name)
    document.write_text(json.dumps({**original, 'revision': current['revision']}))
    cli('storage', 'profile', 'set', name, str(document))
    document.unlink(missing_ok=True)
    if job and cli('task', 'show', job)['state'] == 'paused':
        cli('task', 'resume', job)
        wait(lambda: done(job))
    s3.delete_object(Bucket=bucket, Key=key)
    db.close()
