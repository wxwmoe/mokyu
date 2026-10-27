"""Verify raw reuse before final S3 upload and bounded request workspace recovery.

Owns an isolated, initialized service and disposable remote backend, using the
source_quarantine fixture variables. Every acknowledged value is read back.
"""
import base64
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
    log = (faults / 'raw-receiving.log').open('a')
    process = None

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect raw-receiving.log'
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
        root = Path(database.config['instance']['data_path']) / 'pending' / 'raw'

        def configure(**changes):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(changes)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def sources():
            return database.query('SELECT COUNT(*) AS n FROM raw_sources WHERE storage_id=%s', storage)[0]['n']

        configure(cdc_enabled=False, deduplication_enabled=True, pack_range_enabled=False,
                  write_processing='inline', upload_cache=False)
        data = bytes(range(256)) * (7 * 1024 * 1024 // 256) + b'last bytes'
        fixture.put('canonical', data)
        baseline = set(fixture.keys())
        count = sources()
        puts = fixture.cli('status')['backend_puts']
        fixture.put('duplicate', data)
        fixture.read('duplicate', data)
        assert set(fixture.keys()) == baseline and sources() == count
        fixture.client.copy_object(Bucket=fixture.name, Key='copy',
                                   CopySource={'Bucket': fixture.name, 'Key': 'canonical'})
        fixture.read('copy', data)
        assert set(fixture.keys()) == baseline and sources() == count
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='multipart')['UploadId']
        manifest = []
        for number, part in enumerate((data[:5 * 1024 * 1024], data[5 * 1024 * 1024:]), start=1):
            reply = fixture.client.upload_part(Bucket=fixture.name, Key='multipart', UploadId=upload,
                                               PartNumber=number, Body=part)
            manifest.append(dict(PartNumber=number, ETag=reply['ETag']))
        fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=upload,
                                                 MultipartUpload={'Parts': manifest})
        fixture.read('multipart', data)
        assert set(fixture.keys()) == baseline and sources() == count
        assert fixture.cli('status')['backend_puts'] == puts, 'reuse sent a final S3 PUT'
        wait(lambda: not list(root.rglob('*.partial')), 'successful request workspace was retained')
        print('PASS PUT, Copy and Multipart reuse healthy raw content without another final S3 object', flush=True)

        fixture.put('empty', b'')
        baseline = set(fixture.keys())
        count = sources()
        fixture.put('empty-copy', b'')
        fixture.read('empty-copy', b'')
        assert set(fixture.keys()) == baseline and sources() == count
        rejected(lambda: fixture.client.put_object(Bucket=fixture.name, Key='canonical', Body=data,
                                                   ContentMD5=base64.b64encode(bytes(16)).decode()))
        rejected(lambda: fixture.client.put_object(Bucket=fixture.name, Key='canonical', Body=data,
                                                   IfNoneMatch='*'))
        fixture.read('canonical', data)
        assert set(fixture.keys()) == baseline
        print('PASS empty-object reuse, protocol checksum and conditional publication retain their boundaries', flush=True)

        configure(cache_soft_limit='1MiB')
        try:
            fixture.put('canonical', data)
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 503, error.response
            assert error.response['Error']['Code'] == 'SlowDown', error.response
        else:
            raise AssertionError('raw receive bypassed its workspace budget')
        fixture.read('canonical', data)
        assert set(fixture.keys()) == baseline
        boundary = data[:5 * 1024 * 1024]
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='boundary')['UploadId']
        reply = fixture.client.upload_part(Bucket=fixture.name, Key='boundary', UploadId=upload,
                                           PartNumber=1, Body=boundary)
        fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='boundary', UploadId=upload,
            MultipartUpload={'Parts': [dict(PartNumber=1, ETag=reply['ETag'])]})
        fixture.client.copy_object(Bucket=fixture.name, Key='boundary-copy',
                                   CopySource={'Bucket': fixture.name, 'Key': 'boundary'})
        fixture.read('boundary', boundary)
        fixture.read('boundary-copy', boundary)
        baseline = set(fixture.keys())
        print('PASS exact 5 MiB Multipart and Copy replay existing sources without another request file', flush=True)
        configure(cache_soft_limit=None)
        wait(lambda: not list(root.rglob('*.partial')), 'failed request workspace was retained')
        print('PASS exhausted disk workspace returns SlowDown without publishing or uploading a duplicate', flush=True)

        for point in ('raw-before-reuse', 'raw-reused'):
            marker = fixture.arm(point)
            with ThreadPoolExecutor(max_workers=1) as pool:
                request = pool.submit(fixture.put, 'canonical', data)
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or request.done(), point + ' was not reached')
                    assert not request.done(), request.exception()
                    assert list(root.rglob('*.partial')), 'large request did not stage to disk'
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
            assert not list(root.rglob('*.partial')), 'restart retained unacknowledged request workspace'
            assert set(fixture.keys()) == baseline
        print('PASS crashes before and after reuse preserve the old object and clean unacknowledged workspace', flush=True)

        different = b'!' + data[1:]
        marker = fixture.arm('raw-before-reuse')
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(fixture.put, 'canonical', different)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or request.done(), 'workspace verification did not pause')
                assert not request.done(), request.exception()
                paths = list(root.rglob('*.partial'))
                assert len(paths) == 1, paths
                with paths[0].open('r+b') as file:
                    file.write(b'?')
                marker.unlink()
                rejected(lambda: request.result(timeout=90))
            finally:
                marker.unlink(missing_ok=True)
        fixture.read('canonical', data)
        assert set(fixture.keys()) == baseline
        wait(lambda: not list(root.rglob('*.partial')), 'corrupt workspace was not released')
        print('PASS changed request workspace cannot publish a final S3 object or replace existing data', flush=True)

        shared = b'concurrent first writer' * 4096
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda key: fixture.put(key, shared), ('race-a', 'race-b')))
        rows = database.query("""SELECT e.source_id FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id
            WHERE o.bucket_id=%s AND o.object_key IN (%s,%s)""", uuid.UUID(fixture.bucket_id),
            'race-a' if database.type == 'postgres' else b'race-a',
            'race-b' if database.type == 'postgres' else b'race-b')
        assert len(rows) == 2 and rows[0]['source_id'] == rows[1]['source_id'], rows
        fixture.read('race-a', shared)
        fixture.read('race-b', shared)
        print('PASS concurrent first writers preserve one shared content identity', flush=True)

        incoming = b'quarantine race' * 8192
        fixture.put('bad-candidate', incoming)
        old = database.query(f"""SELECT r.id,r.{database.raw_location} AS file_uuid FROM objects o
            JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s""", uuid.UUID(fixture.bucket_id),
            'bad-candidate' if database.type == 'postgres' else b'bad-candidate')[0]
        identity = uuid.UUID(str(old['file_uuid'])) if database.type == 'postgres' else uuid.UUID(bytes=bytes(old['file_uuid']))
        physical = fixture.prefix + 'raw/' + identity.hex[-2:] + '/' + identity.hex
        marker = fixture.arm('raw-candidate-found')
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(fixture.put, 'new-reference', incoming)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or request.done(), 'candidate lookup did not pause')
                assert not request.done(), request.exception()
                fixture.bytes(physical, b'!' + incoming[1:])
                scope = dict(storage_id=str(storage), source='raw', id=str(old['id']))
                fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
                marker.unlink()
                request.result(timeout=90)
            finally:
                marker.unlink(missing_ok=True)
        fixture.read('new-reference', incoming)
        rejected(lambda: fixture.read('bad-candidate', incoming))
        print('PASS transactional revalidation rejects a candidate quarantined after the fast lookup', flush=True)


    finally:
        for name in ('raw-before-reuse', 'raw-reused', 'raw-candidate-found'):
            (faults / name).unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
