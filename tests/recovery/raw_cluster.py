"""Two real data nodes share raw staging, deduplication and protected reads.

Uses the source_quarantine fixture on control, a remote backend, and
MOKYU_TEST_PEER_ENDPOINT for the second data node. Fault markers target the
first data node. Configure read_idle_timeout=30s and a disposable primary DB.
"""
import argparse
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.config import Config

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cache', action='store_true',
                        help='enable raw file cache; requires two MOKYU_TEST_CACHE_DIRS and sufficient node cache budgets')
    args = parser.parse_args()
    fixture = Fixture()
    assert fixture.backend is not None
    database = Database()
    assert database.type == 'postgres'
    fixture.client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
        aws_access_key_id=fixture.credentials['access_key'],
        aws_secret_access_key=fixture.credentials['secret_key'],
        config=fixture.client.meta.config.merge(Config(read_timeout=180)))
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_PEER_ENDPOINT'], region_name='us-east-1',
        aws_access_key_id=fixture.credentials['access_key'],
        aws_secret_access_key=fixture.credentials['secret_key'], config=fixture.client.meta.config)
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, deduplication_enabled=True, pack_range_enabled=False,
                   cache_enabled=args.cache, unreferenced_grace='1s', upload_retention='1s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def mapping(key):
        rows = database.query('''SELECT o.stream_id,r.id FROM objects o
            JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(fixture.bucket_id), key)
        assert len(rows) == 1, rows
        return rows[0]

    boundary = 16 * 1024 * 1024
    data = bytes(range(256)) * (boundary // 256 + 100)
    upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='multipart')['UploadId']
    manifest = []
    for client, number, part in [(fixture.client, 1, data[:boundary]), (peer, 2, data[boundary:])]:
        result = client.upload_part(Bucket=fixture.name, Key='multipart', UploadId=upload, PartNumber=number, Body=part)
        manifest.append(dict(PartNumber=number, ETag=result['ETag']))
    rows = database.query('''SELECT f.staging_storage_id,f.staging_object_key FROM parts p
        JOIN extents e ON e.stream_id=p.stream_id JOIN fragments f ON f.id=e.fragment_id
        WHERE p.upload_id=%s''', uuid.UUID(upload))
    assert rows and all(row['staging_storage_id'] and row['staging_object_key'].startswith('__mokyu/multipart/fragment/') for row in rows), rows
    assert not database.query('SELECT id FROM raw_sources WHERE storage_id=%s', uuid.UUID(fixture.store['id']))
    peer.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=upload,
                                   MultipartUpload={'Parts': manifest})
    fixture.read('multipart', data)
    identity = mapping('multipart')['id'].hex
    if args.cache:
        directories = [Path(value) for value in os.environ['MOKYU_TEST_CACHE_DIRS'].split(',')]
        assert len(directories) == 2
        cached = directories[0] / identity[-2:] / identity
        inode = cached.stat().st_ino
    fixture.put('whole', data)
    assert mapping('whole')['id'] == mapping('multipart')['id']
    fixture.read('whole', data)
    with peer.get_object(Bucket=fixture.name, Key='whole')['Body'] as body:
        assert body.read() == data
    if args.cache:
        assert cached.stat().st_ino == inode
        for directory in directories:
            assert (directory / identity[-2:] / identity).stat().st_size == len(data)

    concurrent = b'concurrent raw cluster identity' * 1024
    with ThreadPoolExecutor(max_workers=2) as pool:
        writes = [pool.submit(client.put_object, Bucket=fixture.name, Key=key, Body=concurrent)
                  for client, key in [(fixture.client, 'a'), (peer, 'b')]]
        for result in writes:
            result.result(timeout=60)
    assert mapping('a')['id'] == mapping('b')['id']
    print('PASS raw Multipart uses shared fragments and completes, reads and deduplicates across nodes', flush=True)

    original = b'protected raw reader' * 4096
    fixture.put('retiring', original)
    old = mapping('retiring')
    identity = uuid.UUID(str(old['id']))
    physical = fixture.prefix + 'raw/' + identity.hex[-2:] + '/' + identity.hex
    if args.cache:
        fixture.read('retiring', original)
    marker = fixture.arm('raw-cache-hit' if args.cache else 'raw-before-verify')
    with ThreadPoolExecutor(max_workers=1) as pool:
        reader = pool.submit(fixture.read, 'retiring', original)
        try:
            wait(lambda: marker.with_suffix('.hit').exists() or reader.done(), 'raw read did not enter verification')
            assert not reader.done(), reader.exception()
            initial = database.query("SELECT expires_at FROM stream_leases WHERE stream_id=%s AND lease_type='read'", old['stream_id'])
            assert len(initial) == 1, initial
            peer.put_object(Bucket=fixture.name, Key='retiring', Body=b'replacement')
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))

            def renewed_past_original_deadline():
                return database.query('''SELECT expires_at FROM stream_leases
                    WHERE stream_id=%s AND lease_type='read' AND expires_at>clock_timestamp()
                    AND clock_timestamp()>%s''', old['stream_id'], initial[0]['expires_at'])

            wait(renewed_past_original_deadline, 'slow raw verification lost its shared read lease', seconds=80)
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            assert fixture.bytes(physical) == original
        finally:
            marker.unlink(missing_ok=True)
        reader.result(timeout=30)
    fixture.read('retiring', b'replacement')

    def collected():
        fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        return physical not in fixture.keys()

    wait(collected, 'old raw source was not reclaimed after its reader finished', seconds=60)
    print('PASS slow raw verification renews its lease and cross-node GC waits for the reader', flush=True)


if __name__ == '__main__':
    main()
