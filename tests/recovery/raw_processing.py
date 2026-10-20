"""Durable raw background deduplication, restart, pinned readers and bad sources."""
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    database = Database()
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    process = None
    log = (faults / 'raw-processing.log').open('a')

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect raw-processing.log'
            return subprocess.run(command + ['cli', 'status', '--ready'],
                                  capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not start', seconds=60)

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def identity(value):
        return value if isinstance(value, uuid.UUID) else uuid.UUID(bytes=bytes(value))

    try:
        start()
        fixture = Fixture()
        bucket = uuid.UUID(fixture.bucket_id)

        # Let the raw worker enter its idle wait before exercising repeated task wakes.
        time.sleep(1.2)
        for _ in range(8):
            job = fixture.cli('maintenance', 'run', 'cleanup')['task_id']

            def completed():
                task = fixture.api('/api/v1/tasks/' + job)
                assert task['status'] not in ('failed', 'paused'), task
                return task['status'] == 'completed'

            wait(completed, 'raw worker consumed the task notification', seconds=5)
            time.sleep(.1)
        print('PASS idle raw worker does not delay maintenance until the scheduler fallback', flush=True)

        def configure(succeeds=True, **values):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(values)
            result = fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile, succeeds=succeeds)
            if not succeeds:
                response = fixture.session.put(fixture.web + '/api/v1/storages/' + fixture.name + '/profile',
                    headers=fixture.headers, json=profile, timeout=30)
                assert response.status_code == 400, response.text
                assert fixture.cli('storage', 'profile', 'show', fixture.name)['revision'] == profile['revision']
            return result

        def source(key):
            rows = database.query('''SELECT r.id,r.content_hash,s.id AS stream_id,s.etag,s.metadata,
                s.public_read,s.created_at,s.touched_at,s.payload_revision
                FROM objects o JOIN streams s ON s.id=o.stream_id JOIN raw_ranges e ON e.stream_id=s.id
                JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s''',
                bucket, key if database.type == 'postgres' else key.encode())
            assert len(rows) == 1, rows
            return rows[0]

        def unchanged(before, after):
            for field in ('stream_id', 'etag', 'metadata', 'public_read', 'created_at', 'touched_at'):
                assert before[field] == after[field], field

        def drained():
            return not database.query('''SELECT w.stream_id FROM raw_processing w JOIN streams s
                ON s.id=w.stream_id WHERE s.bucket_id=%s''', bucket)

        configure(write_processing='unknown', succeeds=False)
        configure(write_processing='background', succeeds=False)
        configure(cdc_enabled=False, pack_range_enabled=False, deduplication_enabled=True,
                  write_processing='background')
        publication = fixture.arm('source-before-publish')
        with ThreadPoolExecutor(max_workers=1) as pool:
            upload = pool.submit(fixture.put, 'publishing', b'publication barrier')
            try:
                wait(lambda: publication.with_suffix('.hit').exists() or upload.done(), 'publication did not pause')
                assert not upload.done(), upload.exception()
                time.sleep(2.2)
                pending = database.query('''SELECT w.token FROM raw_processing w JOIN streams s
                    ON s.id=w.stream_id WHERE s.bucket_id=%s''', bucket)
                assert len(pending) == 1 and pending[0]['token'] is None, pending
            finally:
                publication.unlink(missing_ok=True)
            upload.result(timeout=30)
        wait(drained, 'work was lost before object publication')
        assert source('publishing')['content_hash'] is not None
        print('PASS unpublished ready streams retain their background work until object publication', flush=True)
        data = b'background raw content' * 4096
        marker = fixture.arm('raw-processing-before-hash')
        fixture.put('first', data)
        fixture.put('second', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'background receiver did not pause')
        first, second = source('first'), source('second')
        assert first['id'] != second['id'] and first['content_hash'] is None
        fixture.read('first', data)
        fixture.read('second', data)
        configure(deduplication_enabled=False)
        stop(crash=True)
        marker.unlink()
        start()
        wait(drained, 'recorded work did not resume after restart')
        assert source('first')['id'] == source('second')['id']
        unchanged(first, source('first'))
        unchanged(second, source('second'))
        fixture.put('disabled', data)
        assert source('disabled')['id'] != source('first')['id']
        print('PASS durable ACK precedes deduplication, restart resumes the captured policy and preserves logical metadata', flush=True)

        configure(deduplication_enabled=True)
        marker = fixture.arm('raw-processing-before-switch')
        fixture.put('reading', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'switch did not pause')
        before = source('reading')
        reader_marker = fixture.arm('read-body')
        with ThreadPoolExecutor(max_workers=1) as executor:
            reader = executor.submit(fixture.read, 'reading', data)
            try:
                wait(lambda: reader_marker.with_suffix('.hit').exists() or reader.done(), 'reader did not pin the original file')
                assert not reader.done(), reader.exception()
                marker.unlink()
                wait(lambda: database.query('SELECT token,content_hash FROM raw_processing WHERE stream_id=%s', identity(before['stream_id']))[0]['token'] is None,
                     'busy reader did not postpone the switch')
                assert source('reading')['id'] == before['id']
            finally:
                marker.unlink(missing_ok=True)
                reader_marker.unlink(missing_ok=True)
            reader.result(timeout=30)
        wait(drained, 'deduplication did not resume after the reader released its pin')
        assert source('reading')['id'] == source('first')['id']
        unchanged(before, source('reading'))
        print('PASS pinned reads postpone switching without blocking responses or changing the object version', flush=True)

        marker = fixture.arm('raw-processing-before-switch')
        fixture.put('restart-switch', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'verified switch did not pause')
        stop(crash=True)
        marker.unlink()
        start()
        wait(drained, 'verified hash did not recover')
        fixture.read('restart-switch', data)
        assert source('restart-switch')['id'] == source('first')['id']
        print('PASS restart after hashing reuses the durable work record', flush=True)

        large = b'x' * (5 * 1024 * 1024) + b'multipart suffix'
        fixture.put('whole', large)
        wait(drained, 'whole-file reference did not finish')
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='multipart')['UploadId']
        parts = []
        for number, part in enumerate([large[:5 * 1024 * 1024], large[5 * 1024 * 1024:]], start=1):
            reply = fixture.client.upload_part(Bucket=fixture.name, Key='multipart', UploadId=upload, PartNumber=number, Body=part)
            parts.append(dict(PartNumber=number, ETag=reply['ETag']))
        reply = fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=upload,
                                                         MultipartUpload={'Parts': parts})
        assert reply['ETag'].endswith('-2"')
        wait(drained, 'Multipart background work did not finish')
        assert source('whole')['id'] == source('multipart')['id']
        fixture.read('multipart', large)
        fixture.client.copy_object(Bucket=fixture.name, Key='copy', CopySource={'Bucket': fixture.name, 'Key': 'whole'})
        wait(drained, 'CopyObject background work did not finish')
        assert source('copy')['id'] == source('whole')['id']
        print('PASS Multipart and CopyObject share complete-content identity in the background', flush=True)

        fixture.put('empty-a', b'')
        fixture.put('empty-b', b'')
        wait(drained, 'empty-object processing did not finish')
        assert source('empty-a')['id'] == source('empty-b')['id']
        fixture.read('empty-b', b'')
        original = source('first')
        original_id = identity(original['id'])
        relative = 'raw/' + original_id.hex[:2] + '/' + original_id.hex
        fixture.bytes(fixture.prefix + relative if fixture.backend else str(fixture.root / relative), b'!' + data[1:])
        scope = dict(storage_id=fixture.store['id'], source='raw', id=str(original_id))
        fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
        fixture.put('healthy-replacement', data)
        wait(drained, 'quarantined candidate prevented healthy independent publication')
        assert source('healthy-replacement')['id'] != original['id']
        fixture.read('healthy-replacement', data)
        rejected(lambda: fixture.read('first', data))
        print('PASS empty objects share safely and quarantined candidates never gain new references', flush=True)

        marker = fixture.arm('raw-processing-before-hash')
        fixture.put('deleted', b'deleted before completion')
        wait(lambda: marker.with_suffix('.hit').exists(), 'deleted work did not pause')
        fixture.client.delete_object(Bucket=fixture.name, Key='deleted')
        marker.unlink()
        wait(drained, 'deleted work was not canceled')
        marker = fixture.arm('raw-processing-before-hash')
        fixture.put('corrupt', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'corrupt work did not pause')
        bad = source('corrupt')
        ident = identity(bad['id'])
        relative = 'raw/' + ident.hex[:2] + '/' + ident.hex
        path = fixture.prefix + relative if fixture.backend else str(fixture.root / relative)
        fixture.bytes(path, b'!' + data[1:])
        marker.unlink()
        wait(lambda: database.query('SELECT last_error FROM raw_processing WHERE stream_id=%s', identity(bad['stream_id']))[0]['last_error'],
             'corruption did not record a failure')
        assert source('corrupt')['id'] == bad['id'] and source('corrupt')['content_hash'] is None
        rejected(lambda: fixture.read('corrupt', data))
        fixture.read('healthy-replacement', data)
        status = fixture.cli('cache', 'status')['raw_processing']
        assert status['failed'] >= 1
        fixture.client.delete_object(Bucket=fixture.name, Key='corrupt')
        print('PASS deleted work is canceled and corrupted bytes never create shared references', flush=True)
    finally:
        for name in ('raw-processing-before-hash', 'raw-processing-before-switch', 'read-body', 'source-before-publish'):
            (faults / name).unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
