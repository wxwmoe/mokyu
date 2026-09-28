"""Local acknowledgement and physical publication crash boundaries; isolated use only."""
import fcntl
import json
import os
import signal
import subprocess
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from integration import s3, bucket, error

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
config = Path(os.environ['MOKYU_TEST_CONFIG'])
command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
data = Path(os.environ['MOKYU_TEST_DATA'])
pid = int(os.environ['MOKYU_TEST_GATEWAY_PID'])
process = None
points = ('upload-cache-written', 'upload-cache-durable', 'upload-cache-before-commit',
          'upload-cache-committed', 'object-published', 'pack-stored',
          'pack-before-publish', 'upload-before-release')


def cli(*args):
    return json.loads(subprocess.check_output(command + ['cli', *args], stderr=subprocess.DEVNULL, text=True))


def wait(predicate):
    for _ in range(600):
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError('crash recovery condition timed out')


def kill():
    os.kill(pid, signal.SIGKILL)
    if process:
        process.wait(timeout=10)
    def released():
        with (data / 'gateway.lock').open('rb') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(released)


def start():
    global process, pid
    process = subprocess.Popen(command + ['serve', '--maintenance'],
                               stdout=open(Path(os.environ['MOKYU_TEST_RESULTS']) / 'upload-crashes.log', 'a'),
                               stderr=subprocess.STDOUT)
    pid = process.pid
    def ready():
        assert process.poll() is None, 'replacement process exited; inspect upload-crashes.log'
        try:
            return cli('status')['maintenance']
        except subprocess.CalledProcessError:
            return False
    wait(ready)


def done(job):
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'))
    state = cli('task', 'show', job)
    assert state['state'] == 'completed', state


try:
    for index, point in enumerate(points):
        raw = os.urandom(6 * 1024 * 1024 + index)
        key = f'crash-{index}'
        marker = faults / point
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        if index < 5:
            (faults / 'upload-before-work').touch()
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(s3.put_object, Bucket=bucket, Key=key, Body=raw)
            if index >= 5:
                request.result(timeout=60)  # Acknowledged data must survive every later boundary.
            wait(marker.with_suffix('.hit').exists)
            kill()
            try:
                request.result(timeout=20)
            except Exception:
                assert index < 5
        marker.unlink()
        (faults / 'upload-before-work').unlink(missing_ok=True)
        start()
        if index >= 4:
            assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
        else:
            error('404', s3.head_object, Bucket=bucket, Key=key)
        assert db.execute('SELECT count(*) FROM pack_inputs').fetchone()[0] == 0
        done(cli('cache', 'flush')['task_id'])
        assert db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0] == 0
        if index >= 4:
            assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == raw
        cli('maintenance', 'disable')
        print('PASS publication crash boundary:', point, flush=True)

    # Logical dedup identity survives rotation; every NEW physical encoding uses the active write key.
    (faults / 'upload-before-work').touch()
    raw = os.urandom(7 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key='rotated-pending', Body=raw)
    ids = [r[0] for r in db.execute('SELECT chunk_id FROM pending_uploads').fetchall()]
    assert len(ids) > 1
    kill()
    (faults / 'upload-before-work').unlink()
    keyring = Path(tomllib.loads(config.read_text())['encryption']['keyring_file'])
    keys = keyring.read_text().replace('active="test"', 'active="rotated"')
    keyring.write_text(keys + '\n[keys.rotated]\nalgorithm="aes-256-gcm"\nkey="' + '82' * 32 + '"\n')
    start()
    assert s3.get_object(Bucket=bucket, Key='rotated-pending')['Body'].read() == raw
    done(cli('cache', 'flush')['task_id'])
    assert db.execute('SELECT DISTINCT c.key_id,l.key_id FROM chunks c JOIN chunk_locations l ON l.chunk_id=c.id WHERE c.id=ANY(%s) AND l.state=\'ready\'', (ids,)).fetchall() == [('test', 'rotated')]
    cli('maintenance', 'disable')
    db.execute("UPDATE chunks SET repack_after=now()-interval '1 hour'")
    db.execute('UPDATE pack_maintenance SET next_check_at=now()')
    done(cli('pack', 'run')['task_id'])
    packed = db.execute("SELECT DISTINCT p.id,p.key_id FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=ANY(%s)", (ids,)).fetchall()
    assert packed and all(p[1] == 'rotated' for p in packed), packed
    for pack_id, _ in packed:
        done(cli('pack', 'unpack', str(pack_id), '--execute')['task_id'])
    assert db.execute('SELECT DISTINCT l.key_id FROM chunk_locations l WHERE l.chunk_id=ANY(%s) AND l.state=\'ready\'', (ids,)).fetchall() == [('rotated',)]
    assert s3.get_object(Bucket=bucket, Key='rotated-pending')['Body'].read() == raw
    print('PASS key rotation across pending upload, packing and unpacking', flush=True)
finally:
    for point in (*points, 'upload-before-work'):
        (faults / point).unlink(missing_ok=True)
    if process and process.poll() is None:
        process.terminate()
        process.wait(timeout=40)
