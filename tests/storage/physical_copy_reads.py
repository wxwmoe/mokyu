"""Bound pack downloads while copying across stores with all read caches disabled."""
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import uuid

from tests.recovery.source_repair import Database, location
from tests.storage.source_quarantine import Fixture, rejected, wait


def run(*, bounded=True):
    fixture = Fixture()
    assert fixture.backend, 'requires an isolated S3 namespace'
    backend = json.loads(Path(os.environ['MOKYU_TEST_BACKEND']).read_text())
    database = Database()
    source = fixture.cli('storage', 'profile', 'show', fixture.name)
    source.update(cache_enabled=False, upload_cache=False, pack_range_enabled=False,
                  cdc_enabled=True, compression_enabled=False, min_storage_duration='0s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=source)
    key = fixture.cli('key', 'create', fixture.name + '-key', 'block', '-a', 'aes-256-gcm')
    fixture.cli('storage', 'key', 'set', fixture.name, key['name'])
    data = bytes(value & 63 for value in os.urandom(6 * 1024 * 1024 + 17))
    fixture.put('packed', data)
    source = fixture.cli('storage', 'profile', 'show', fixture.name)
    source.update(pack_enabled=True, compression_enabled=True, pack_max_size='16MiB')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=source)
    fixture.finished(fixture.cli('pack', 'run'))
    packs = database.query("SELECT location_id,member_count,stored_size FROM packs WHERE storage_id=%s AND state='ready'",
                           uuid.UUID(fixture.store['id']))
    assert len(packs) == 1 and packs[0]['member_count'] > 1, packs
    packed_size = packs[0]['stored_size']
    source = fixture.cli('storage', 'profile', 'show', fixture.name)
    source.update(pack_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=source)
    wait(lambda: fixture.cli('status')['cache_jobs_active'] == 0, 'cache jobs did not drain')

    measurements = []
    for raw in (False, True):
        name = fixture.name + ('-raw' if raw else '-chunks')
        # Distinct stores may share a disposable test service, but never a namespace.
        target = fixture.cli('storage', 'create', name, 's3', '--init', '--endpoint', backend['endpoint'],
                             '--bucket', backend['bucket'], '--region', backend['region'], '--prefix', name,
                             '--access', backend['access_key'], '--secret', backend['secret_key'])
        profile = fixture.cli('storage', 'profile', 'show', name)
        profile.update(cdc_enabled=not raw, cache_enabled=False, upload_cache=False,
                       deduplication_enabled=False, compression_enabled=False, pack_enabled=False,
                       pack_range_enabled=False)
        fixture.cli('storage', 'profile', 'set', name, '-', value=profile)
        bucket = fixture.cli('bucket', 'create', name, '--storage', target['id'])
        fixture.cli('cred', 'grant', fixture.credentials['access_key'], name)
        before = fixture.cli('status')
        fixture.client.copy_object(Bucket=name, Key='copy', CopySource={'Bucket': fixture.name, 'Key': 'packed'})
        after = fixture.cli('status')
        reads = after['backend_gets'] - before['backend_gets']
        downloaded = after['backend_read_bytes'] - before['backend_read_bytes']
        # Remote raw output hashes its input before replaying the immutable body.
        passes = 2 if raw else 1
        measurements.append(dict(payload='raw' if raw else 'chunks', members=packs[0]['member_count'],
                                 pack_bytes=packed_size, reads=reads, downloaded=downloaded, passes=passes))
        with fixture.client.get_object(Bucket=name, Key='copy')['Body'] as response:
            assert response.read() == data
        fixture.read('packed', data)
        if bounded:
            assert reads == passes, measurements[-1]
            assert downloaded <= packed_size * passes, measurements[-1]
        rows = database.query("SELECT s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=%s AND o.object_key=%s",
                              uuid.UUID(bucket['id']), 'copy' if database.type == 'postgres' else b'copy')
        assert rows == [dict(payload_type='raw' if raw else 'chunks')], rows
    # The hash pass must release its pack before replay revalidates the source.
    previous = b'previous durable target'
    fixture.client.put_object(Bucket=name, Key='copy', Body=previous)
    path = location(fixture, packs[0]['location_id'], packed=True)
    original = fixture.bytes(path)
    marker = fixture.arm('raw-before-replay')
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fixture.client.copy_object, Bucket=name, Key='copy',
                             CopySource={'Bucket': fixture.name, 'Key': 'packed'})
        try:
            wait(lambda: marker.with_suffix('.hit').exists() or future.done(), 'copy did not reach replay')
            assert not future.done(), future.exception()
            fixture.bytes(path, bytes([original[0] ^ 1]) + original[1:])
            marker.unlink()
            rejected(lambda: future.result(timeout=90))
        finally:
            marker.unlink(missing_ok=True)
            fixture.bytes(path, original)
    with fixture.client.get_object(Bucket=name, Key='copy')['Body'] as response:
        assert response.read() == previous
    fixture.client.copy_object(Bucket=name, Key='copy', CopySource={'Bucket': fixture.name, 'Key': 'packed'})
    with fixture.client.get_object(Bucket=name, Key='copy')['Body'] as response:
        assert response.read() == data
    return measurements


if __name__ == '__main__':
    print(json.dumps(run()))
