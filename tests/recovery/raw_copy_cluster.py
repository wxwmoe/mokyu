"""Node S3 Copy verifies durable raw files before sharing bounded references.

Uses source_quarantine on a disposable PostgreSQL control and a real data/node
S3 endpoint. MOKYU_TEST_CACHE_DIRS identifies its raw cache directory. The
fault-injection control and node share MOKYU_TEST_FAULT_DIR.
"""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def run():
    fixture = Fixture()
    database = Database()
    assert fixture.backend is not None and database.type == 'postgres'
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def mapping(key):
        return database.query('''SELECT o.stream_id,e.source_id,e.offset_bytes,e.length,e.source_offset
            FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id
            WHERE o.bucket_id=%s AND o.object_key=%s ORDER BY e.offset_bytes''',
            uuid.UUID(fixture.bucket_id), key)

    def copy(source, target, **kwargs):
        return fixture.client.copy_object(Bucket=fixture.name, Key=target,
            CopySource={'Bucket': fixture.name, 'Key': source}, **kwargs)

    data = bytes(range(256)) * 1200
    etag = fixture.put('source', data)['ETag']
    original = mapping('source')[0]
    physical = set(fixture.keys())
    result = copy('source', 'copy', MetadataDirective='REPLACE', Metadata={'copied': 'yes'})
    assert result['CopyObjectResult']['ETag'] == etag
    assert mapping('copy')[0]['source_id'] == original['source_id']
    assert set(fixture.keys()) == physical
    fixture.read('copy', data)
    assert fixture.client.head_object(Bucket=fixture.name, Key='copy')['Metadata'] == {'copied': 'yes'}

    database.query('''WITH removed AS (
        DELETE FROM raw_ranges WHERE stream_id=%s RETURNING source_id
    ), inserted AS (
        INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset)
        SELECT %s,n*1024,1024,%s,n*1024 FROM generate_series(0,299) n
        CROSS JOIN (SELECT count(*) FROM removed) deleted RETURNING 1
    ) UPDATE streams SET payload_revision=payload_revision+1
        WHERE id=%s AND (SELECT count(*) FROM inserted)=300''',
        original['stream_id'], original['stream_id'], original['source_id'], original['stream_id'])
    copy('source', 'paged')
    rows = mapping('paged')
    assert len(rows) == 300 and {row['source_id'] for row in rows} == {original['source_id']}
    assert [(row['offset_bytes'], row['length'], row['source_offset']) for row in rows] == [
        (i * 1024, 1024, i * 1024) for i in range(300)]
    assert set(fixture.keys()) == physical
    fixture.read('paged', data)
    print('PASS node raw Copy shares sources and preserves metadata, ETag and paged mappings', flush=True)

    fixture.put('empty', b'')
    copy('empty', 'empty-copy')
    fixture.read('empty-copy', b'')
    fixture.put('target', b'previous target')
    old = mapping('target')[0]['stream_id']
    rejected(lambda: copy('source', 'target', IfNoneMatch='*'))
    assert mapping('target')[0]['stream_id'] == old

    marker = fixture.arm('node-raw-before-verify')
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(lambda: rejected(lambda: copy('source', 'target')))
        try:
            wait(lambda: marker.with_suffix('.hit').exists() or pending.done(), 'raw copy verification not reached')
            assert not pending.done(), pending.exception()
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile['cache_enabled'] = True
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        finally:
            marker.unlink(missing_ok=True)
        pending.result(timeout=60)
    assert mapping('target')[0]['stream_id'] == old
    fixture.read('target', b'previous target')
    print('PASS node raw Copy fences target profile changes and preserves failed overwrite targets', flush=True)

    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile['read_integrity'] = 'inline'
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    warm = b'healthy cached raw source' * 4096
    fixture.put('warm', warm)
    source = mapping('warm')[0]
    identity = source['source_id'].hex
    cache = Path(os.environ['MOKYU_TEST_CACHE_DIRS'].split(',')[0]) / identity[-2:] / identity
    fixture.read('warm', warm)
    wait(cache.is_file, 'raw cache was not filled')
    assert cache.read_bytes() == warm
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile['read_integrity'] = 'off'
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    path = fixture.prefix + 'raw/' + identity[-2:] + '/' + identity
    encoded = fixture.bytes(path)
    fixture.bytes(path, bytes([encoded[0] ^ 1]) + encoded[1:])
    try:
        fixture.read('warm', warm)
        rejected(lambda: copy('warm', 'target'))
        assert mapping('target')[0]['stream_id'] == old
        fixture.read('target', b'previous target')
        wait(lambda: database.query('SELECT quarantine_code FROM raw_sources WHERE id=%s',
             source['source_id'])[0]['quarantine_code'], 'damaged backend was not quarantined')
    finally:
        fixture.bytes(path, encoded)
    print('PASS healthy L2 cannot hide backend corruption during node raw reference Copy', flush=True)
    return fixture, database


if __name__ == '__main__':
    run()
