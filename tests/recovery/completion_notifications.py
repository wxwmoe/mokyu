"""Object notices wake duplicate Complete requests without proving completion.

Requires two API data nodes and the source_quarantine fault fixture.
"""
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_PEER_ENDPOINT'],
                        region_name='us-east-1', config=fixture.client.meta.config,
                        aws_access_key_id=fixture.credentials['access_key'],
                        aws_secret_access_key=fixture.credentials['secret_key'])
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    peer_faults = Path(os.environ['MOKYU_TEST_PEER_FAULT_DIR'])
    controls = [Path(path) for path in os.environ['MOKYU_TEST_CONTROL_FAULT_DIRS'].split(',')]
    markers = []

    def arm(root, name):
        marker = root / name
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        markers.append(marker)
        return marker

    def settled():
        return database.query('SELECT count(*) AS n FROM notification_targets WHERE applied_at IS NULL AND retired_at IS NULL')[0]['n'] == 0

    def state(args):
        return database.query('SELECT state,output_stream FROM uploads WHERE id=%s', uuid.UUID(args['UploadId']))[0]

    try:
        for mode in ('notice', 'early', 'missing', 'superseded', 'chunks'):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(cdc_enabled=mode == 'chunks', deduplication_enabled=False,
                           upload_cache=False, pack_enabled=False, pack_range_enabled=False,
                           compression_enabled=False)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
            key = 'complete-' + mode
            data = os.urandom(192 * 1024 + 13)
            upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key=key)['UploadId']
            part = fixture.client.upload_part(Bucket=fixture.name, Key=key, UploadId=upload, PartNumber=1, Body=data)
            args = dict(Bucket=fixture.name, Key=key, UploadId=upload,
                        MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
            wait(settled, 'fixture notifications did not settle')
            owner = arm(faults, 'node-completion-assembling')
            waiting = arm(peer_faults, 'node-completion-waiting')
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(fixture.client.complete_multipart_upload, **args)
                wait(owner.with_suffix('.hit').exists, 'owner did not pause')
                duplicate = pool.submit(peer.complete_multipart_upload, **args)
                try:
                    wait(waiting.with_suffix('.hit').exists, 'duplicate did not check status')
                    if mode == 'superseded':
                        fixture.put(key, b'another operation on the same key')
                        wait(settled, 'same-key notice did not apply')
                        waiting.unlink()
                        # The notice predates polling the enabled waiter; it must still wake it.
                        time.sleep(.3)
                        assert not duplicate.done() and state(args)['state'] == 'completing'
                    if mode == 'missing':
                        for root in controls:
                            arm(root, 'notifications-claimed')
                    if mode != 'early':
                        waiting.unlink(missing_ok=True)
                        time.sleep(.15)
                    released = time.monotonic()
                    owner.unlink()
                    if mode == 'superseded':
                        for future in (first, duplicate):
                            try:
                                future.result(timeout=10)
                                raise AssertionError('superseded completion reported success')
                            except ClientError as error:
                                assert error.response['Error']['Code'] in ('ServiceUnavailable', 'InternalError', 'OperationAborted'), error.response
                        assert state(args)['state'] != 'completed'
                        fixture.read(key, b'another operation on the same key')
                        print('PASS same-key notice cannot publish or confirm a superseded completion', flush=True)
                        continue
                    result = first.result(timeout=20)
                    if mode == 'early':
                        wait(settled, 'publication notice did not apply before waiter resumed')
                        released = time.monotonic()
                        waiting.unlink()
                    reply = duplicate.result(timeout=10)
                    elapsed = time.monotonic() - released
                    assert reply['ETag'] == result['ETag']
                    if mode == 'missing':
                        assert 1.5 < elapsed < 4.5, elapsed
                    else:
                        assert elapsed < 1.5, (mode, elapsed)
                    completed = state(args)
                    assert completed['state'] == 'completed'
                    fixture.read(key, data)
                    assert peer.complete_multipart_upload(**args)['ETag'] == reply['ETag']
                    assert state(args) == completed, 'replay created another output'
                    print(f'PASS {mode}: confirmed completion in {elapsed:.3f}s', flush=True)
                finally:
                    for marker in markers:
                        marker.unlink(missing_ok=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
