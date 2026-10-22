"""Flush boundaries, CLI waits and graceful shutdown preserve accepted cache writes."""
import datetime
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log_path = faults / 'shutdown-flush.log'
    log = log_path.open('a')
    process = None
    markers = []

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        def ready():
            assert process.poll() is None, 'service exited; inspect shutdown-flush.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'flush service did not start', seconds=60)

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    try:
        start()
        fixture, database = Fixture(), Database()
        assert fixture.backend, 'requires an isolated S3 backend'
        assert fixture.cli('cache', 'status')['effective_upload_bytes'] >= 32 * 1024 * 1024, 'requires at least 32MiB of upload cache'
        storage = uuid.UUID(fixture.store['id'])
        bucket = uuid.UUID(fixture.bucket_id)

        def configure(**values):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(values)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def arm(name):
            marker = fixture.arm(name)
            markers.append(marker)
            return marker

        def pending():
            return database.query('SELECT COUNT(*) AS n FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.storage_id=%s', storage)[0]['n']

        def source(key):
            return database.query('SELECT r.id,r.pending_upload FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s', bucket, key if database.type == 'postgres' else key.encode())[0]

        def settings(timeout):
            value = fixture.cli('settings', 'get', 'io')
            value['shutdown_flush_timeout'] = timeout
            fixture.cli('settings', 'set', 'io', '-', value=value)
            stop()
            start()

        configure(cdc_enabled=False, upload_cache=True, cache_enabled=True, deduplication_enabled=False, pack_range_enabled=False)
        data = os.urandom(128 * 1024)
        held = arm('raw-upload-before-process')
        fixture.put('first', data)
        wait(held.with_suffix('.hit').exists, 'raw worker did not pause')
        first = fixture.cli('cache', 'flush')['task_id']
        time.sleep(.02)
        fixture.put('later', data + b'later')
        later = source('later')['id']
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        if database.type == 'sqlite':
            future = int(future.timestamp() * 1_000_000)
        database.query('UPDATE raw_uploads SET next_attempt_at=%s WHERE source_id=%s', future, later)
        second = fixture.cli('cache', 'flush')['task_id']
        assert second != first, 'flush reused a task created before the latest accepted write'
        held.unlink()
        fixture.finished({'task_id': first})
        assert source('later')['pending_upload'], 'flush waited for a write after its captured boundary'
        past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        if database.type == 'sqlite':
            past = int(past.timestamp() * 1_000_000)
        database.query('UPDATE raw_uploads SET next_attempt_at=%s WHERE source_id=%s', past, later)
        reply = fixture.cli('cache', 'flush', '--wait', '--timeout', '20s')
        assert reply['state'] == 'completed' and reply['task_id'] != first and pending() == 0
        fixture.finished({'task_id': second})
        fixture.read('first', data)
        fixture.read('later', data + b'later')
        print('PASS each flush captures its own database timestamp and excludes later accepted writes', flush=True)

        held = arm('raw-upload-before-process')
        fixture.put('timeout', data + b'timeout')
        wait(held.with_suffix('.hit').exists, 'timeout worker did not pause')
        started = time.monotonic()
        reply = subprocess.run(command + ['cli', 'cache', 'flush', '--wait', '--timeout', '1s'], capture_output=True, text=True, timeout=10)
        assert reply.returncode != 0 and 'continues on the server' in reply.stderr, reply.stderr
        result = json.loads(reply.stdout)
        assert result['state'] == 'timeout' and 1 <= time.monotonic() - started < 5
        assert fixture.cli('task', 'show', result['task_id'])['state'] in ('queued', 'running')
        held.unlink()
        fixture.finished(result)
        print('PASS CLI timeout returns the persistent task id without cancelling accepted work', flush=True)

        held = arm('raw-upload-before-process')
        fixture.put('graceful', data + b'graceful')
        wait(held.with_suffix('.hit').exists, 'graceful worker did not pause')
        begin = log_path.stat().st_size
        process.terminate()
        time.sleep(.3)
        assert process.poll() is None, 'shutdown abandoned an accepted in-flight upload'
        held.unlink()
        process.wait(timeout=12)
        assert process.returncode == 0 and pending() == 0
        assert 'shutdown flush completed' in log_path.read_text()[begin:]
        start()
        fixture.read('graceful', data + b'graceful')
        print('PASS graceful shutdown keeps the existing upload worker alive through final publication', flush=True)

        large = os.urandom(8 * 1024 * 1024)
        fixture.put('streaming', large)
        fixture.cli('cache', 'flush', '--wait', '--timeout', '20s')
        body = fixture.client.get_object(Bucket=fixture.name, Key='streaming')['Body']
        prefix = body.read(64 * 1024)
        process.terminate()
        time.sleep(.2)
        assert process.poll() is None, 'shutdown truncated an active HTTP response'
        try:
            assert prefix + body.read() == large
        finally:
            body.close()
        process.wait(timeout=12)
        assert process.returncode == 0
        start()
        print('PASS shutdown closes idle keepalive connections and completes an active streaming response', flush=True)

        configure(cdc_enabled=True, write_processing='inline', pack_enabled=True,
                  pack_max_size='8MiB', pack_upload_flush_timeout='300s')
        held = arm('upload-before-prepare')
        chunks = os.urandom(3 * 1024 * 1024)
        fixture.put('chunks', chunks)
        count = database.query('SELECT COUNT(*) AS n FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.storage_id=%s', storage)[0]['n']
        assert count > 0
        process.terminate()
        time.sleep(.2)
        held.unlink()
        process.wait(timeout=12)
        assert process.returncode == 0
        assert database.query('SELECT COUNT(*) AS n FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.storage_id=%s', storage)[0]['n'] == 0
        start()
        fixture.read('chunks', chunks)
        print('PASS shutdown flush bypasses normal pack batching delays and publishes cached chunks', flush=True)

        configure(write_processing='background')
        held = arm('cdc-before-read')
        fixture.put('convert', data + b'convert')
        fixture.put('convert-second', data + b'convert-second')
        wait(held.with_suffix('.hit').exists, 'CDC worker did not pause')
        wait(lambda: database.query("SELECT count(*) AS n FROM tasks WHERE task_type='upload' AND state='running'")[0]['n'] >= 2, 'CDC did not occupy both ordinary workers')
        stop()
        assert pending() == 0
        start()
        fixture.read('convert', data + b'convert')
        held.unlink()
        wait(lambda: not database.query('SELECT r.id FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s', bucket, 'convert' if database.type == 'postgres' else b'convert'), 'conversion did not recover', seconds=90)
        print('PASS shutdown persists raw independently of a paused conversion and restart resumes CDC', flush=True)

        configure(cdc_enabled=False, pack_enabled=False, pack_range_enabled=False, write_processing='inline')
        for seconds in (2, 0):
            settings(f'{seconds}s')
            held = arm('raw-upload-before-process')
            key = f'deadline-{seconds}'
            fixture.put(key, data + key.encode())
            wait(held.with_suffix('.hit').exists, 'deadline worker did not pause')
            begin = log_path.stat().st_size
            started = time.monotonic()
            stop()
            elapsed = time.monotonic() - started
            assert max(0, seconds - .2) <= elapsed < seconds + 4, elapsed
            assert pending() > 0
            report = log_path.read_text()[begin:]
            assert 'shutdown flush incomplete' in report and 'oldest_at' in report and 'bytes' in report
            start()
            fixture.read(key, data + key.encode())
            held.unlink()
            fixture.cli('cache', 'flush', '--wait', '--timeout', '20s')
        settings('30s')
        print('PASS configured deadlines preserve pending sources, log backlog and recover on restart', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
