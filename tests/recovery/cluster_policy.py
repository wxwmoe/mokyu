"""Verify per-backend Multipart expiry with two real data nodes and native S3 MPU.

Use the raw_cluster fixture variables; cleanup is triggered through maintenance.
"""
import os
import uuid

import boto3

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    database = Database()
    assert database.type == 'postgres'
    first, second = Fixture(), Fixture()
    uploads = []
    for fixture, expiry in [(first, '24h'), (second, '7d')]:
        assert fixture.backend is not None
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        assert profile['min_free_space'] is None
        assert 'write_durability' not in profile
        for durability in ['durable', 'buffered']:
            fixture.cli('storage', 'profile', 'set', fixture.name, '-',
                        value={**profile, 'write_durability': durability}, succeeds=False)
        assert fixture.cli('storage', 'profile', 'show', fixture.name)['revision'] == profile['revision']
        profile.update(cdc_enabled=False, pack_range_enabled=False, multipart_idle_timeout=expiry)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_PEER_ENDPOINT'], region_name='us-east-1',
            aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key=fixture.credentials['secret_key'],
            config=fixture.client.meta.config)
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='expiry')['UploadId']
        first_part = fixture.client.upload_part(Bucket=fixture.name, Key='expiry', UploadId=upload, PartNumber=1, Body=b'a' * 5 * 1024 * 1024)
        before = database.query('SELECT touched_at FROM uploads WHERE id=%s', uuid.UUID(upload))[0]['touched_at']
        last_part = peer.upload_part(Bucket=fixture.name, Key='expiry', UploadId=upload, PartNumber=2, Body=b'b')
        touched = database.query('SELECT touched_at FROM uploads WHERE id=%s', uuid.UUID(upload))[0]['touched_at']
        assert touched > before
        peer.list_parts(Bucket=fixture.name, Key='expiry', UploadId=upload)
        assert database.query('SELECT touched_at FROM uploads WHERE id=%s', uuid.UUID(upload))[0]['touched_at'] == touched
        sources = database.query('''SELECT f.staging_storage_id,f.file_id,p.remote_etag FROM parts p JOIN extents e ON e.stream_id=p.stream_id
            JOIN fragments f ON f.id=e.fragment_id WHERE p.upload_id=%s''', uuid.UUID(upload))
        assert sources and all(row['staging_storage_id'] is None and row['file_id'] and row['remote_etag'] for row in sources)
        uploads.append((fixture, peer, upload, [dict(PartNumber=1, ETag=first_part['ETag']), dict(PartNumber=2, ETag=last_part['ETag'])]))

    for _, _, upload, _ in uploads:
        database.query("UPDATE uploads SET touched_at=now()-interval '2 days' WHERE id=%s", uuid.UUID(upload))
        # Advance the same simulated idle period for already-finished part leases.
        database.query("UPDATE stream_leases SET expires_at=expires_at-interval '2 days' WHERE stream_id IN (SELECT write_token FROM parts WHERE upload_id=%s)", uuid.UUID(upload))
    first.finished(first.cli('maintenance', 'run', 'gc'))
    wait(lambda: database.query('SELECT state FROM uploads WHERE id=%s', uuid.UUID(uploads[0][2]))[0]['state'] == 'aborted',
         'short backend policy did not expire native Multipart')
    assert database.query('SELECT state FROM uploads WHERE id=%s', uuid.UUID(uploads[1][2]))[0]['state'] == 'active'
    fixture, peer, upload, manifest = uploads[0]
    rejected(lambda: peer.complete_multipart_upload(Bucket=fixture.name, Key='expiry', UploadId=upload, MultipartUpload={'Parts': manifest}))
    fixture, peer, upload, manifest = uploads[1]
    peer.complete_multipart_upload(Bucket=fixture.name, Key='expiry', UploadId=upload, MultipartUpload={'Parts': manifest})
    fixture.read('expiry', b'a' * 5 * 1024 * 1024 + b'b')
    print('PASS native Multipart uses each backend expiry, renews across nodes and retains the longer-lived upload', flush=True)


if __name__ == '__main__':
    main()
