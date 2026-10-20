"""Published PUT/Copy/Complete must not hold the read/GC barrier while sealing."""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    database = Database()
    baseline = os.environ.get('MOKYU_TEST_EXPECT_PUBLISH_BLOCKED') == '1'
    point = 'object-published' if baseline else 'object-before-seal'
    data = b'publication coordination\n' * 2048
    for raw in (False, True):
        target = Fixture()
        profile = target.cli('storage', 'profile', 'show', target.name)
        profile.update(upload_retention='1s', unreferenced_grace='1s', min_storage_duration='0s')
        profile['cdc_enabled'] = not raw
        target.cli('storage', 'profile', 'set', target.name, '-', value=profile)
        target.put('source', data)
        for operation in ('put', 'copy', 'complete'):
            key = f'{raw}-{operation}'
            if operation == 'put':
                request = lambda: target.put(key, data)
            elif operation == 'copy':
                request = lambda: target.client.copy_object(Bucket=target.name, Key=key,
                    CopySource={'Bucket': target.name, 'Key': 'source'})
            else:
                upload = target.client.create_multipart_upload(Bucket=target.name, Key=key)['UploadId']
                part = target.client.upload_part(Bucket=target.name, Key=key,
                    UploadId=upload, PartNumber=1, Body=data)
                request = lambda: target.client.complete_multipart_upload(Bucket=target.name,
                    Key=key, UploadId=upload, MultipartUpload={'Parts': [dict(PartNumber=1, ETag=part['ETag'])]})

            def read(key):
                start = time.monotonic()
                target.read(key, data)
                return (time.monotonic() - start) * 1000

            marker = target.arm(point)
            with ThreadPoolExecutor(max_workers=3) as pool:
                writer = pool.submit(request)
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or writer.done(),
                         'write did not reach publication boundary')
                    assert not writer.done(), writer.exception()
                    readers = [pool.submit(read, item) for item in ('source', key)]
                    if baseline:
                        time.sleep(.25)
                        assert not any(item.done() for item in readers), 'baseline did not reproduce the barrier'
                        marker.unlink(missing_ok=True)
                    elapsed = [item.result(timeout=3) for item in readers]
                    if not baseline:
                        assert not writer.done(), 'ACK bypassed pending sealing'
                        stream = database.query('SELECT s.id FROM streams s JOIN buckets b ON b.id=s.bucket_id '
                            'WHERE b.name=%s AND s.object_key=%s', target.name,
                            key if database.type == 'postgres' else key.encode())[0]['id']
                        target.client.delete_object(Bucket=target.name, Key=key)
                        time.sleep(1.1)
                        target.finished(target.cli('maintenance', 'run', 'gc'))
                        assert database.query('SELECT id FROM streams WHERE id=%s', stream), 'GC crossed the writer pin'
                finally:
                    marker.unlink(missing_ok=True)
                writer.result(timeout=30)
            if not baseline:
                def reclaimed():
                    target.finished(target.cli('maintenance', 'run', 'gc'))
                    return not database.query('SELECT id FROM streams WHERE id=%s', stream)
                wait(reclaimed, 'released writer pin leaked its stream', seconds=45)
            print(json.dumps(dict(layout='raw' if raw else 'chunks', operation=operation,
                                  baseline=baseline, read_ms=elapsed)), flush=True)
    print('PASS published reads, pending ACK and GC writer pins across PUT/Copy/Complete', flush=True)


if __name__ == '__main__':
    main()
