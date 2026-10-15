"""Local acknowledgement and physical publication crash boundaries; isolated use only."""
import fcntl
import json
import os
import signal
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from tests.integration import s3, bucket, error

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
    return json.loads(subprocess.check_output(command + ['cli', *args], stderr=subprocess.DEVNULL, text=True, timeout=20))


def wait(predicate):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
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
    db.execute("UPDATE mokyu_meta SET maintenance=true")
    process = subprocess.Popen(command,
                               stdout=open(Path(os.environ['MOKYU_TEST_RESULTS']) / 'upload-crashes.log', 'a'),
                               stderr=subprocess.STDOUT)
    pid = process.pid
    if pid_file := os.environ.get('MOKYU_TEST_GATEWAY_PID_FILE'):
        Path(pid_file).write_text(str(pid))
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
    identity = db.execute('SELECT id,hash,raw_size FROM chunks WHERE id=ANY(%s) ORDER BY id', (ids,)).fetchall()
    rotated_name = 'rotated-' + uuid.uuid4().hex[:10]
    rotated = cli('key', 'create', rotated_name, 'block', '-a', 'aes-256-gcm')
    old_keys = db.execute('SELECT DISTINCT block_key_id FROM chunks WHERE id=ANY(%s)', (ids,)).fetchall()
    assert len(old_keys) == 1 and old_keys[0][0] is not None
    rotated_id = uuid.UUID(rotated['id'])
    cli('storage', 'key', 'set', os.environ['MOKYU_TEST_STORAGE'], rotated_name)
    kill()
    (faults / 'upload-before-work').unlink()
    start()
    assert s3.get_object(Bucket=bucket, Key='rotated-pending')['Body'].read() == raw
    done(cli('cache', 'flush')['task_id'])
    assert db.execute('SELECT DISTINCT c.block_key_id,l.block_key_id FROM chunks c JOIN chunk_encodings l ON l.chunk_id=c.id WHERE c.id=ANY(%s) AND l.state=\'ready\'', (ids,)).fetchall() == [(rotated_id, rotated_id)]
    assert db.execute('SELECT id,hash,raw_size FROM chunks WHERE id=ANY(%s) ORDER BY id', (ids,)).fetchall() == identity
    cli('maintenance', 'disable')
    db.execute("UPDATE chunks SET repack_after=now()-interval '1 hour'")
    db.execute('UPDATE pack_maintenance SET next_check_at=now()')
    done(cli('pack', 'run')['task_id'])
    packed = db.execute("SELECT DISTINCT p.id,p.block_key_id FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=ANY(%s)", (ids,)).fetchall()
    assert packed and all(p[1] == rotated_id for p in packed), packed
    for pack_id, _ in packed:
        done(cli('pack', 'unpack', str(pack_id), '--execute')['task_id'])
    assert db.execute('SELECT DISTINCT l.block_key_id FROM chunk_encodings l WHERE l.chunk_id=ANY(%s) AND l.state=\'ready\'', (ids,)).fetchall() == [(rotated_id,)]
    assert s3.get_object(Bucket=bucket, Key='rotated-pending')['Body'].read() == raw
    print('PASS key rotation across pending upload, packing and unpacking', flush=True)
finally:
    for point in (*points, 'upload-before-work'):
        (faults / point).unlink(missing_ok=True)
    if process and process.poll() is None:
        process.terminate()
        process.wait(timeout=40)
