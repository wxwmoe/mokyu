"""Reusing packed chunks preserves their locations and reads across a restart."""
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


def mappings(key):
    return db.execute('SELECT e.chunk_id,c.pack_id FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE b.name=%s AND o.object_key=%s ORDER BY e.offset_bytes', (bucket, key)).fetchall()


def read():
    assert s3.get_object(Bucket=bucket, Key=keys[1])['Body'].read() == raw
    for start in (0, 4194300, len(raw)-97):
        end = min(start+8191, len(raw)-1)
        assert s3.get_object(Bucket=bucket, Key=keys[1], Range=f'bytes={start}-{end}')['Body'].read() == raw[start:end+1]


original = cli('storage', 'profile', 'show', store)
try:
    profile({**original, 'cdc_enabled': True, 'deduplication_enabled': True, 'upload_cache': True, 'pack_enabled': True, 'pack_max_size': '32MiB', 'pack_upload_flush_timeout': '1s'})
    s3.put_object(Bucket=bucket, Key=keys[0], Body=raw)
    def ready_pack():
        rows = mappings(keys[0])
        return rows if len(rows) >= 2 and all(pack for _, pack in rows) else None
    original_pack = wait(ready_pack)
    ids = [chunk for chunk, _ in original_pack]
    def check_reuse():
        assert mappings(keys[0]) == mappings(keys[1]) == original_pack
        assert db.execute('SELECT count(*) FROM pending_uploads WHERE chunk_id=ANY(%s)', (ids,)).fetchone()[0] == 0
        assert db.execute('SELECT count(*) FROM cache_pins WHERE chunk_id=ANY(%s)', (ids,)).fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM chunks c JOIN packs p ON p.id=c.pack_id WHERE c.id=ANY(%s) AND p.state='ready'", (ids,)).fetchone()[0] == len(ids)
    assert db.execute("SELECT count(*) FROM chunk_encodings WHERE chunk_id=ANY(%s) AND state='ready'", (ids,)).fetchone()[0] == 0

    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    s3.put_object(Bucket=bucket, Key=keys[1], Body=raw)
    check_reuse()
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
    check_reuse()
    read()
    print('PASS packed reuse preserves member locations and full and Range reads across restart', flush=True)
finally:
    marker.unlink(missing_ok=True)
    cli('maintenance', 'disable')
    profile(original)
