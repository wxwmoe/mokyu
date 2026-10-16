"""Physical rewrite preserves bytes and rejects paused or interrupted publication."""
import fcntl
import json
import os
import signal
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
from tests.integration import bucket, s3

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
storage = os.environ['MOKYU_TEST_STORAGE']
storage_id = db.execute('SELECT id FROM storages WHERE name=%s', (storage,)).fetchone()[0]
data = Path(os.environ['MOKYU_TEST_DATA'])
pid_file = Path(os.environ['MOKYU_TEST_PID_FILE'])
fault_dir = Path(os.environ['MOKYU_TEST_FAULT_DIR'])


def cli(*args):
    result = subprocess.run(command + ['cli', *args], capture_output=True, text=True, timeout=25)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError('rewrite operation timed out')


def completed(job):
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
    row = cli('task', 'show', job)
    assert row['state'] == 'completed', row


def marker(name):
    path = fault_dir / name
    path.with_suffix('.hit').unlink(missing_ok=True)
    path.touch()
    return path


def identities():
    return db.execute("SELECT id,current_encoding_id FROM chunks WHERE storage_id=%s AND state='ready' ORDER BY id", (storage_id,)).fetchall()


def restart():
    def released():
        with (data / 'gateway.lock').open('r+') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(released)
    with (data / 'rewrite-test.log').open('a') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True,
                                   env={**os.environ, 'MOKYU_TEST_FAULT_DIR': str(fault_dir)})
    pid_file.write_text(str(process.pid))
    def ready():
        assert process.poll() is None, 'rewrite runtime stopped before readiness'
        return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=10).returncode == 0
    wait(ready)


raw = os.urandom(9 * 1024 * 1024 + 17)
s3.put_object(Bucket=bucket, Key='rewrite-source', Body=raw)
before = identities()
preview = cli('storage', 'rewrite', storage, 'preview')
assert preview['preview']['loose_chunks'] > 0
rejected = subprocess.run(command + ['cli', 'storage', 'rewrite', storage, 'start', '--confirm', 'invalid'], capture_output=True)
assert rejected.returncode != 0
assert identities() == before
point = marker('rewrite-chunk-stored')
try:
    job = cli('storage', 'rewrite', storage, 'start', '--confirm', preview['confirmation'])['task_id']
    wait(point.with_suffix('.hit').exists)
    assert db.execute('SELECT count(*) FROM tasks WHERE parent_task_id=%s', (job,)).fetchone()[0] == 0
    detail = cli('task', 'show', job)['detail']
    assert detail['execution_mode'] == 'single' and 'role' not in detail and 'shards' not in detail
    assert cli('storage', 'rewrite', storage, 'status')['id'] == job
    assert s3.get_object(Bucket=bucket, Key='rewrite-source')['Body'].read() == raw
    cli('task', 'pause', job)
finally:
    point.unlink(missing_ok=True)
wait(lambda: db.execute('SELECT count(*)=0 FROM pack_inputs p JOIN tasks t ON t.id=p.task_id WHERE t.id=%s OR t.parent_task_id=%s', (job, job)).fetchone()[0])
assert identities() == before
assert cli('task', 'show', job)['state'] == 'paused'
cli('task', 'resume', job)
completed(job)
assert all(old != new for (_, old), (_, new) in zip(before, identities()))
assert db.execute("SELECT count(*)=0 FROM chunk_encodings WHERE storage_id=%s AND state='ready' AND encryption_algorithm<>'none'", (storage_id,)).fetchone()[0]
assert s3.get_object(Bucket=bucket, Key='rewrite-source')['Body'].read() == raw
assert s3.get_object(Bucket=bucket, Key='rewrite-source', Range='bytes=1000000-3000000')['Body'].read() == raw[1000000:3000001]
assert cli('storage', 'rewrite', storage, 'preview')['preview']['loose_chunks'] == 0
print('PASS loose rewrite preview, confirmation, pause fence, resume, encoding identity and full/Range reads', flush=True)

packed_raw = os.urandom(9 * 1024 * 1024 + 29)
s3.put_object(Bucket=bucket, Key='rewrite-packed-source', Body=packed_raw)
completed(cli('pack', 'run')['task_id'])
old = db.execute("SELECT id FROM packs WHERE storage_id=%s AND state='ready' ORDER BY id", (storage_id,)).fetchall()
assert old
target = 'rewrite-' + uuid.uuid4().hex[:10]
key_id = cli('key', 'create', target, 'block', '-a', 'aes-256-gcm')['id']
preview = cli('storage', 'rewrite', storage, 'preview', '--key', target)
assert preview['preview']['packed_chunks'] > 0
point = marker('rewrite-pack-stored')
try:
    job = cli('storage', 'rewrite', storage, 'start', '--key', target, '--confirm', preview['confirmation'])['task_id']
    wait(point.with_suffix('.hit').exists)
    assert db.execute('SELECT count(*) FROM tasks WHERE parent_task_id=%s', (job,)).fetchone()[0] == 0
    detail = cli('task', 'show', job)['detail']
    assert detail['execution_mode'] == 'single' and 'role' not in detail and 'shards' not in detail
    assert cli('storage', 'rewrite', storage, 'status')['id'] == job
    assert s3.get_object(Bucket=bucket, Key='rewrite-packed-source')['Body'].read() == packed_raw
    cli('maintenance', 'enable')
    os.kill(int(pid_file.read_text()), signal.SIGKILL)
finally:
    point.unlink(missing_ok=True)
restart()
assert db.execute("SELECT id FROM packs WHERE storage_id=%s AND state='ready' ORDER BY id", (storage_id,)).fetchall() == old
assert db.execute("SELECT count(*) FROM packs WHERE storage_id=%s AND state='preparing'", (storage_id,)).fetchone()[0] == 0
assert s3.get_object(Bucket=bucket, Key='rewrite-packed-source')['Body'].read() == packed_raw
assert cli('task', 'show', job)['state'] == 'paused'
cli('maintenance', 'disable')
cli('task', 'resume', job)
completed(job)
assert db.execute("SELECT count(*) FROM packs WHERE storage_id=%s AND state='ready' AND block_key_id=%s", (storage_id, key_id)).fetchone()[0] == len(old)
assert all(state == 'retired' for (state,) in db.execute('SELECT state FROM packs WHERE id=ANY(%s)', ([row[0] for row in old],)).fetchall())
assert s3.get_object(Bucket=bucket, Key='rewrite-source')['Body'].read() == raw
assert s3.get_object(Bucket=bucket, Key='rewrite-packed-source')['Body'].read() == packed_raw
assert s3.get_object(Bucket=bucket, Key='rewrite-packed-source', Range='bytes=8000000-9000000')['Body'].read() == packed_raw[8000000:9000001]
assert cli('storage', 'rewrite', storage, 'preview', '--key', target)['preview']['packed_chunks'] == 0
print('PASS packed rewrite crash before publication, old-source retention, recovery, key change and full/Range reads', flush=True)
