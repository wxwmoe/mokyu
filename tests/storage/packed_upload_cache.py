"""Packed chunk reuse keeps distinct local cache identities across a restart."""
import fcntl
import json
import os
import signal
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
from tests.integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
assert db.execute("SELECT deployment_mode='single' FROM mokyu_meta").fetchone()[0]
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
data = Path(os.environ['MOKYU_TEST_DATA'])
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'upload-before-work'
store = os.environ['MOKYU_TEST_STORAGE']
keys = ['packed-cache-' + uuid.uuid4().hex for _ in range(2)]
raw = os.urandom(12 * 1024 * 1024 + 777)


def cli(*args):
    result = subprocess.run(command + ['cli', *args], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.1)
    raise AssertionError('packed cache operation timed out')


def profile(value):
    path = data / 'packed-cache-profile.json'
    value = {**value, 'revision': cli('storage', 'profile', 'show', store)['revision']}
    path.write_text(json.dumps(value), encoding='utf-8')
    try:
        cli('storage', 'profile', 'set', store, str(path))
    finally:
        path.unlink(missing_ok=True)


def pending(ids):
    return db.execute('SELECT count(*) FROM pending_uploads WHERE chunk_id=ANY(%s)', (ids,)).fetchone()[0]


def read():
    assert s3.get_object(Bucket=bucket, Key=keys[1])['Body'].read() == raw
    for start in (0, 4194300, len(raw)-97):
        end = min(start+8191, len(raw)-1)
        assert s3.get_object(Bucket=bucket, Key=keys[1], Range=f'bytes={start}-{end}')['Body'].read() == raw[start:end+1]


original = cli('storage', 'profile', 'show', store)
try:
    profile({**original, 'upload_cache': True, 'pack_enabled': True, 'pack_max_size': '32MiB'})
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    s3.put_object(Bucket=bucket, Key=keys[0], Body=raw)
    ids = [row[0] for row in db.execute('SELECT e.chunk_id FROM objects o JOIN streams s ON s.id=o.stream_id JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=s.id WHERE b.name=%s AND o.object_key=%s ORDER BY e.offset_bytes', (bucket, keys[0]))]
    assert len(ids) >= 2 and pending(ids) == len(ids)
    wait(marker.with_suffix('.hit').exists)
    marker.unlink()
    wait(lambda: pending(ids) == 0)
    assert db.execute("SELECT count(*) FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=ANY(%s) AND p.state='ready'", (ids,)).fetchone()[0] == len(ids)
    assert db.execute("SELECT count(*) FROM chunk_encodings WHERE chunk_id=ANY(%s) AND state='ready'", (ids,)).fetchone()[0] == 0

    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    s3.put_object(Bucket=bucket, Key=keys[1], Body=raw)
    wait(marker.with_suffix('.hit').exists)
    rows = db.execute('SELECT chunk_id,staging_object_key,staged_size FROM pending_uploads WHERE chunk_id=ANY(%s)', (ids,)).fetchall()
    assert len(rows) == len(ids), (len(rows), len(ids))
    names = [name for _, name, _ in rows]
    assert len(set(names)) == len(ids) and all(uuid.UUID(name.rsplit('.', 1)[0]).int for name in names), names
    reserved = sum(size for _, _, size in rows)
    assert cli('cache', 'status')['reserved_bytes'] >= reserved
    read()

    os.kill(int(Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE']).read_text()), signal.SIGKILL)
    def released():
        with (data / 'gateway.lock').open('rb') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(released)
    db.execute('UPDATE mokyu_meta SET maintenance=true')
    with open(Path(os.environ['MOKYU_TEST_RESULTS']) / 'packed-cache-app.log', 'a') as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   env={**os.environ, 'MOKYU_TEST_FAULT_DIR': str(marker.parent)})
    Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE']).write_text(str(process.pid))
    def ready():
        assert process.poll() is None, 'replacement process exited; inspect packed-cache-app.log'
        try:
            return cli('status')['maintenance']
        except AssertionError:
            return False
    wait(ready)
    assert cli('cache', 'status')['reserved_bytes'] >= reserved
    read()
    marker.unlink()
    job = cli('cache', 'flush')['task_id']
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'))
    assert cli('task', 'show', job)['state'] == 'completed'
    assert pending(ids) == 0
    assert db.execute('SELECT count(*) FROM cache_pins WHERE chunk_id=ANY(%s)', (ids,)).fetchone()[0] == 0
    read()
    print('PASS packed reuse has distinct cache identities, restart restores pins, flush preserves full and Range reads', flush=True)
finally:
    marker.unlink(missing_ok=True)
    cli('maintenance', 'disable')
    profile(original)
