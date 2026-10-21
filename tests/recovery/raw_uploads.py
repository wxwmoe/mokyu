"""Exercise acknowledged raw upload-cache recovery against an isolated S3 backend."""
import base64
import datetime
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'raw-uploads.log').open('a')
    process = None

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect raw-uploads.log'
            try:
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                      timeout=5).returncode == 0
            except subprocess.TimeoutExpired:
                return False

        wait(ready, 'service did not recover', seconds=45)

    try:
        start()
        fixture = Fixture()
        assert fixture.backend, 'requires a disposable S3 backend'
        database = Database()
        storage = uuid.UUID(fixture.store['id'])
        bucket = uuid.UUID(fixture.bucket_id)
        root = Path(database.config['instance']['data_path']) / 'raw'

        def configure(**changes):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(changes)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def row(key):
            return database.query(f"""SELECT r.id,r.{database.raw_location} AS file_uuid,r.pending_upload
                FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id
                JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s""",
                bucket, key if database.type == 'postgres' else key.encode())[0]

        def path(key):
            value = row(key)['file_uuid']
            identity = uuid.UUID(str(value)) if database.type == 'postgres' else uuid.UUID(bytes=bytes(value))
            return root / identity.hex[-2:] / identity.hex

        def queued():
            return database.query('SELECT COUNT(*) AS n FROM raw_uploads u JOIN raw_sources r '
                                  'ON r.id=u.source_id WHERE r.storage_id=%s', storage)[0]['n']

        def drain():
            wait(lambda: queued() == 0, 'raw uploads did not drain', seconds=150)

        configure(cdc_enabled=False, deduplication_enabled=True, pack_range_enabled=False,
                  write_processing='inline', upload_cache=False)
        data = bytes(range(256)) * (7 * 1024 * 1024 // 256) + b'cached tail'
        fixture.put('canonical', data)
        baseline = set(fixture.keys())
        configure(upload_cache=True)
        marker = fixture.arm('raw-upload-before-process')
        fixture.put('cached', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'upload worker did not pause')
        assert row('cached')['pending_upload'] and queued() == 1
        assert set(fixture.keys()) == baseline, 'acknowledgement waited for or sent a final PUT'
        assert path('cached').read_bytes() == data
        assert path('cached').stat().st_mode & 0o777 == 0o644
        fixture.read('cached', data)
        assert fixture.client.head_object(Bucket=fixture.name, Key='cached')['ContentLength'] == len(data)
        with fixture.client.get_object(Bucket=fixture.name, Key='cached', Range='bytes=13-1037')['Body'] as body:
            assert body.read() == data[13:1038]
        print('PASS acknowledged raw files use their backend UUID and serve GET, HEAD and Range before S3 upload', flush=True)

        stop(crash=True)
        start()
        fixture.read('cached', data)
        configure(upload_cache=False, cache_enabled=False)
        fixture.read('cached', data)
        marker.unlink()
        drain()
        assert row('cached')['id'] == row('canonical')['id']
        assert set(fixture.keys()) == baseline
        print('PASS restart and disabled caches retain the sole copy and deduplicate without another final PUT', flush=True)

        configure(upload_cache=True, write_processing='background')
        marker = fixture.arm('raw-upload-before-process')
        fixture.put('background', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'background worker did not pause')
        assert row('background')['pending_upload'] and set(fixture.keys()) == baseline
        fixture.read('background', data)
        marker.unlink()
        drain()
        assert row('background')['id'] == row('canonical')['id']
        assert set(fixture.keys()) == baseline
        print('PASS background deduplication queues locally and reuses the final source without another PUT', flush=True)

        marker = fixture.arm('raw-upload-before-process')
        different = b'new durable copy' + data
        fixture.put('original', different)
        wait(lambda: marker.with_suffix('.hit').exists(), 'copy test did not pause')
        fixture.client.copy_object(Bucket=fixture.name, Key='copy',
                                   CopySource={'Bucket': fixture.name, 'Key': 'original'})
        fixture.client.delete_object(Bucket=fixture.name, Key='original')
        fixture.read('copy', different)
        stop(crash=True)
        start()
        fixture.read('copy', different)
        marker.unlink()
        drain()
        fixture.read('copy', different)
        assert not row('copy')['pending_upload']
        print('PASS deleting the original object does not abandon a copied value across restart', flush=True)

        configure(write_processing='inline', upload_cache_size='1KiB')
        baseline = set(fixture.keys())
        fixture.put('fallback', data)
        assert not row('fallback')['pending_upload']
        assert row('fallback')['id'] == row('canonical')['id']
        assert set(fixture.keys()) == baseline
        fixture.read('fallback', data)
        configure(upload_cache_size=None)
        rejected(lambda: fixture.client.put_object(Bucket=fixture.name, Key='canonical', Body=data,
                                                   ContentMD5=base64.b64encode(bytes(16)).decode()))
        fixture.read('canonical', data)
        print('PASS a full upload quota falls back to synchronous pre-upload deduplication and preserves checksum checks', flush=True)

        marker = fixture.arm('raw-cache-durable')
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(fixture.put, 'canonical', b'unacknowledged durable candidate')
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or request.done(),
                     'cache publication did not pause')
                assert not request.done(), request.exception()
                stop(crash=True)
            finally:
                marker.unlink(missing_ok=True)
            try:
                request.result(timeout=30)
            except (BotoCoreError, ClientError):
                pass
            else:
                raise AssertionError('crashed request unexpectedly returned success')
        start()
        fixture.read('canonical', data)
        assert queued() == 0
        print('PASS a crash between durable file creation and metadata staging cannot replace the old object', flush=True)

        marker = fixture.arm('raw-upload-before-commit')
        incoming = b'completed remote write' * 2048
        fixture.put('commit-crash', incoming)
        wait(lambda: marker.with_suffix('.hit').exists(), 'remote completion did not pause')
        assert row('commit-crash')['pending_upload']
        stop(crash=True)
        marker.unlink()
        start()
        fixture.read('commit-crash', incoming)
        drain()
        assert not row('commit-crash')['pending_upload']
        fixture.read('commit-crash', incoming)
        print('PASS remote completion before metadata commit retries safely after a crash', flush=True)

        marker = fixture.arm('raw-upload-before-process')
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='multipart')['UploadId']
        pieces = [os.urandom(5 * 1024 * 1024 + 37), b'multipart tail']
        parts = []
        for number, piece in enumerate(pieces, 1):
            result = fixture.client.upload_part(Bucket=fixture.name, Key='multipart', UploadId=upload,
                                                PartNumber=number, Body=piece)
            parts.append({'PartNumber': number, 'ETag': result['ETag']})
        baseline = set(fixture.keys())
        fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=upload,
                                                  MultipartUpload={'Parts': parts})
        wait(lambda: marker.with_suffix('.hit').exists(), 'multipart cache did not pause')
        assert row('multipart')['pending_upload'] and set(fixture.keys()) == baseline
        fixture.read('multipart', b''.join(pieces))
        stop(crash=True)
        start()
        fixture.read('multipart', b''.join(pieces))
        marker.unlink()
        drain()
        fixture.read('multipart', b''.join(pieces))
        print('PASS Multipart completion is readable from durable raw cache across restart and final S3 upload', flush=True)

        marker = fixture.arm('raw-upload-before-process')
        fixture.put('flush-old', b'flush before cutoff')
        wait(lambda: marker.with_suffix('.hit').exists(), 'flush test did not pause')
        fixture.put('flush-later', b'flush after cutoff')
        later = row('flush-later')['id']
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        database.query('UPDATE raw_sources SET created_at=%s WHERE id=%s',
                       int(future.timestamp() * 1000000) if database.type == 'sqlite' else future, later)
        rejected(lambda: fixture.client.put_object(Bucket=fixture.name, Key='canonical',
                                                   Body=b'unpublished conditional data', IfNoneMatch='*'))
        fixture.cli('maintenance', 'enable')
        task = fixture.cli('cache', 'flush')['task_id']
        marker.unlink()
        wait(lambda: fixture.cli('task', 'show', task)['state'] in ('completed', 'failed'),
             'bounded maintenance flush did not finish', seconds=150)
        assert fixture.cli('task', 'show', task)['state'] == 'completed', fixture.cli('task', 'show', task)
        assert not row('flush-old')['pending_upload'] and row('flush-later')['pending_upload']
        assert queued() == 1
        if database.type == 'postgres':
            log_start = (faults / 'raw-uploads.log').stat().st_size
            result = subprocess.run(command + ['cli', 'cluster', 'mode', 'cluster'],
                                    capture_output=True, text=True, timeout=30)
            assert result.returncode, result.stdout
            wait(lambda: b'flush pending uploads' in (faults / 'raw-uploads.log').read_bytes()[log_start:],
                 'mode transition did not reject the pending source')
            assert fixture.cli('status')['deployment_mode'] == 'single'
        fixture.read('canonical', data)
        fixture.read('flush-old', b'flush before cutoff')
        fixture.read('flush-later', b'flush after cutoff')
        fixture.cli('maintenance', 'disable')
        drain()
        print('PASS maintenance flush handles rejected publication and stops at its creation cutoff', flush=True)

        marker = fixture.arm('raw-upload-before-process')
        corrupt = b'pending verification' * 8192
        fixture.put('corrupt', corrupt)
        wait(lambda: marker.with_suffix('.hit').exists(), 'integrity test did not pause')
        physical = path('corrupt')
        baseline = set(fixture.keys())
        with physical.open('r+b') as file:
            file.write(b'?')
        rejected(lambda: fixture.read('corrupt', corrupt))
        marker.unlink()
        wait(lambda: database.query('SELECT attempts FROM raw_uploads WHERE source_id=%s', row('corrupt')['id'])[0]['attempts'] > 0,
             'corrupt cache was not rejected by the worker')
        assert row('corrupt')['pending_upload'] and set(fixture.keys()) == baseline
        physical.write_bytes(corrupt)
        drain()
        fixture.read('corrupt', corrupt)
        print('PASS corrupt acknowledged cache cannot publish unchecked bytes or be silently evicted', flush=True)

        marker = fixture.arm('raw-upload-before-process')
        fixture.put('missing', b'unique missing cache')
        wait(lambda: marker.with_suffix('.hit').exists(), 'missing-cache test did not pause')
        physical = path('missing')
        saved = physical.with_suffix('.test-backup')
        stop(crash=True)
        physical.rename(saved)
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        wait(lambda: process.poll() is not None, 'missing unique cache did not stop startup')
        assert process.returncode and queued() == 1
        saved.rename(physical)
        start()
        fixture.read('missing', b'unique missing cache')
        marker.unlink()
        drain()
        print('PASS a missing unique cache stops startup and retains the queue for recovery', flush=True)

    finally:
        for name in ('raw-upload-before-process', 'raw-upload-before-commit', 'raw-cache-durable'):
            (faults / name).unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
