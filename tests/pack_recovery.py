"""Crash pack publication boundaries on an explicitly disposable gateway."""
import json
import fcntl
import os
import signal
import subprocess
import time
from pathlib import Path

import psycopg
from integration import s3, bucket

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
command = [os.environ['MGW_TEST_BINARY'], '--config', os.environ['MGW_TEST_CONFIG']]
faults = Path(os.environ['MGW_TEST_FAULT_DIR'])
pid = int(os.environ['MGW_TEST_GATEWAY_PID'])
process = None


def cli(*args):
    return json.loads(subprocess.check_output(command + ['cli', *args], stderr=subprocess.DEVNULL, text=True))


def wait(predicate):
    for _ in range(600):
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError('recovery condition timed out')


def ready():
    assert process.poll() is None, 'replacement gateway exited; inspect pack-recovery.log'
    try:
        return cli('status')['maintenance']
    except subprocess.CalledProcessError:
        return False


try:
    for index, point in enumerate(('pack-prepared', 'pack-stored', 'pack-before-publish')):
        raw = os.urandom(6 * 1024 * 1024 + index)
        key = f'recovery-{index}'
        s3.put_object(Bucket=bucket, Key=key, Body=raw)
        marker = faults / point
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        job = cli('pack', 'run')['task_id']
        wait(marker.with_suffix('.hit').exists)
        os.kill(pid, signal.SIGKILL)
        if process:
            process.wait(timeout=10)
        # A killed process may remain a zombie until the outer runner reaps it.
        wait(lambda: not Path(f'/proc/{pid}/stat').exists() or Path(f'/proc/{pid}/stat').read_text().split()[2] == 'Z')
        def released():
            with (Path(os.environ['MGW_TEST_DATA'])/'gateway.lock').open('rb') as lock:
                try:
                    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    return True
                except BlockingIOError:
                    return False
        wait(released)
        marker.unlink()
        log = open(Path(os.environ['MGW_TEST_RESULTS']) / 'pack-recovery.log', 'a')
        process = subprocess.Popen(command + ['serve', '--maintenance'], stdout=log, stderr=subprocess.STDOUT)
        pid = process.pid
        wait(ready)
        assert db.execute("SELECT state FROM tasks WHERE id=%s", (job,)).fetchone()[0] == 'paused'
        assert db.execute('SELECT count(*) FROM pack_inputs').fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM packs WHERE state='preparing'").fetchone()[0] == 0
        assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
        time.sleep(.3)
        assert cli('task', 'show', job)['state'] == 'paused'
        cli('maintenance', 'disable')
        cli('task', 'resume', job)
        wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'))
        assert cli('task', 'show', job)['state'] == 'completed'
        assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
        # Recovered unpublished objects remain indexed for normal grace-period GC.
        assert db.execute("SELECT count(*) FROM packs WHERE state='retired' AND unreferenced_at IS NULL").fetchone()[0] == 0
        print('PASS crash recovery and maintenance pause:', point, flush=True)
finally:
    for point in ('pack-prepared', 'pack-stored', 'pack-before-publish'):
        (faults / point).unlink(missing_ok=True)
    if process:
        process.terminate()
        process.wait(timeout=40)
