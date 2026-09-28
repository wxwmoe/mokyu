"""Restart, quota and local-source recovery on an explicitly disposable deployment."""
import base64
import fcntl
import json
import os
import signal
import subprocess
import time
from pathlib import Path

import psycopg
from integration import s3, bucket

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
config = Path(os.environ['MGW_TEST_CONFIG'])
original = config.read_text()
command = [os.environ['MGW_TEST_BINARY'], '--config', str(config)]
faults = Path(os.environ['MGW_TEST_FAULT_DIR'])
data = Path(os.environ['MGW_TEST_DATA'])
pid = int(os.environ['MGW_TEST_GATEWAY_PID'])
process = None
backend_pid = int(os.environ['MGW_TEST_BACKEND_PID'])


def cli(*args):
    return json.loads(subprocess.check_output(command + ['cli', *args], stderr=subprocess.DEVNULL, text=True))


def wait(predicate, message='condition timed out'):
    for _ in range(600):
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError(message)


def kill():
    os.kill(pid, signal.SIGKILL)
    if process:
        process.wait(timeout=10)
    def released():
        with (data / 'gateway.lock').open('rb') as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(released, 'gateway data lock remained held')


def start(text, maintenance=True):
    global process, pid
    config.write_text(text)
    process = subprocess.Popen(command + ['serve'] + (['--maintenance'] if maintenance else []),
                               stdout=open(Path(os.environ['MGW_TEST_RESULTS']) / 'upload-recovery.log', 'a'),
                               stderr=subprocess.STDOUT)
    pid = process.pid
    def ready():
        assert process.poll() is None, 'gateway exited; inspect upload-recovery.log'
        try:
            cli('status')
            return True
        except subprocess.CalledProcessError:
            return False
    wait(ready)


def pending():
    return db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0]


def flush():
    job = cli('cache', 'flush')['task_id']
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'))
    assert cli('task', 'show', job)['state'] == 'completed', cli('task', 'show', job)
    assert pending() == 0


try:
    marker = faults / 'upload-before-work'
    marker.touch()
    raw = os.urandom(7 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key='durable', Body=raw)
    assert pending() > 0
    expected = cli('cache', 'status')['pending']['bytes']
    kill()
    marker.unlink()
    lowered = original.replace('max_size="128MiB"', 'max_size="0B"').replace('upload_cache=true', 'upload_cache=false')
    lowered = lowered.replace('allow_http=true', 'allow_http=true\nconnect_timeout="1s"\nrequest_timeout="1s"\nretry_timeout="1s"\nmax_retries=0')
    os.kill(backend_pid, signal.SIGSTOP)
    start(lowered)
    state = cli('cache', 'status')
    assert state['effective_upload_bytes'] == 0 and state['reserved_bytes'] == expected, state
    assert s3.get_object(Bucket=bucket, Key='durable')['Body'].read() == raw
    assert s3.get_object(Bucket=bucket, Key='durable', Range='bytes=123-999')['Body'].read() == raw[123:1000]
    time.sleep(.3)
    assert pending() > 0, 'maintenance restart performed an automatic upload'
    job = cli('cache', 'flush')['task_id']
    wait(lambda: cli('task', 'show', job)['state'] == 'failed', 'offline backend did not fail the explicit flush')
    assert pending() > 0 and cli('cache', 'status')['reserved_bytes'] == expected
    os.kill(backend_pid, signal.SIGCONT)
    pin = db.execute('SELECT c.storage_id,u.cache_compressed,u.cache_size FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id ORDER BY u.chunk_id LIMIT 1').fetchone()
    key = pin[0].hex
    path = data / 'chunks' / key[:2] / (key + ('.zst' if pin[1] else '.raw'))
    saved = path.read_bytes()
    path.write_bytes(b'corrupt')
    try:
        s3.get_object(Bucket=bucket, Key='durable')['Body'].read()
    except Exception:
        pass
    else:
        raise AssertionError('corrupt unique pending source was accepted')
    assert path.read_bytes() == b'corrupt' and pending() > 0
    path.write_bytes(saved)
    flush()
    assert cli('cache', 'status')['reserved_bytes'] == 0
    assert s3.get_object(Bucket=bucket, Key='durable')['Body'].read() == raw
    print('PASS crash recovery, zero-quota pin restoration, corrupt unique-source protection and explicit flush', flush=True)

    kill()
    tiny = original.replace('upload_cache=true', 'upload_cache=true\nupload_cache_size="2MiB"')
    start(tiny)
    cli('maintenance', 'disable')
    marker.touch()
    larger = os.urandom(11 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key='fallback', Body=larger)
    state = cli('cache', 'status')
    assert state['fallbacks'] == 1 and state['reserved_bytes'] <= 2 * 1024 * 1024, state
    assert db.execute("SELECT upload_cache_bypass FROM streams s JOIN objects o ON o.stream_id=s.id WHERE o.key='fallback'").fetchone()[0]
    assert s3.get_object(Bucket=bucket, Key='fallback')['Body'].read() == larger
    marker.unlink()
    wait(lambda: pending() == 0)
    print('PASS quota exhaustion switches the remaining request to synchronous upload', flush=True)

    kill()
    start(original + '\n[pack]\nenabled=false\n')
    cli('maintenance', 'disable')
    packs = db.execute('SELECT count(*) FROM packs').fetchone()[0]
    marker.touch()
    small = os.urandom(5 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key='pack-off', Body=small)
    assert pending() > 0
    marker.unlink()
    wait(lambda: pending() == 0)
    assert db.execute('SELECT count(*) FROM packs').fetchone()[0] == packs
    assert s3.get_object(Bucket=bucket, Key='pack-off')['Body'].read() == small
    print('PASS asynchronous upload with pack disabled', flush=True)

    kill()
    start(original + '\n[compression]\nstrategy="file_type"\n')
    cli('maintenance', 'disable')
    key = 'multipart-content-type'
    upload = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType='image/png')['UploadId']
    bodies = [base64.b64encode(os.urandom(8 * 1024 * 1024)) for _ in range(2)]
    parts = []
    for number, body in enumerate(bodies, 1):
        result = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=number, Body=body)
        parts.append({'PartNumber': number, 'ETag': result['ETag']})
        db.execute('UPDATE pending_uploads SET next_retry_at=now()')
        wait(lambda: pending() == 0)
    packed = db.execute("SELECT DISTINCT p.id,p.compressed FROM packs p JOIN chunks c ON c.pack_id=p.id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id WHERE s.object_key=%s AND p.state='ready'", (key,)).fetchall()
    assert packed and not any(row[1] for row in packed), packed
    s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={'Parts': parts})
    wait(lambda: pending() == 0)
    response = s3.get_object(Bucket=bucket, Key=key)
    assert response['ContentType'] == 'image/png'
    assert response['Body'].read() == b''.join(bodies)
    print('PASS multipart async packs retain initialization Content-Type for file_type compression', flush=True)
finally:
    os.kill(backend_pid, signal.SIGCONT)
    for name in ('upload-before-work', 'upload-cache-written', 'upload-cache-durable', 'upload-cache-before-commit', 'upload-cache-committed'):
        (faults / name).unlink(missing_ok=True)
    if process and process.poll() is None:
        process.terminate()
        process.wait(timeout=40)
