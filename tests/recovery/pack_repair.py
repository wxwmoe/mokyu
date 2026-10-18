"""Recover one member at a time while preserving the quarantined pack manifest."""
import os
import uuid

from tests.recovery.source_repair import Database, location
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    fixture, database = Fixture(), Database()
    assert fixture.backend, 'requires an isolated S3 pack backend'
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(pack_enabled=True, pack_interval='24h', pack_repack_interval='24h', unreferenced_grace='1s',
        min_storage_duration='0s', deleted_chunk_retention='1h')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    raw = os.urandom(9 * 1024 * 1024 + 17)
    fixture.put('original', raw)
    fixture.finished(fixture.cli('pack', 'run'))
    packs = database.query("SELECT id,location_id FROM packs WHERE storage_id=%s AND state='ready' ORDER BY id", uuid.UUID(fixture.store['id']))
    assert packs, 'expected a real pack'
    pack = packs[0]
    manifest = database.query('SELECT ordinal,chunk_id,offset_bytes FROM pack_members WHERE pack_id=%s ORDER BY ordinal', pack['id'])
    assert len(manifest) >= 2
    stream = database.query('SELECT stream_id FROM objects WHERE bucket_id=%s', uuid.UUID(fixture.bucket_id))[0]['stream_id']
    if not isinstance(stream, uuid.UUID):
        stream = uuid.UUID(bytes=stream) if isinstance(stream, bytes) else uuid.UUID(str(stream))
    extents = database.query('SELECT offset_bytes,chunk_id,length FROM extents WHERE stream_id=%s ORDER BY offset_bytes', stream)
    physical = location(fixture, pack['location_id'], packed=True)
    encoded = fixture.bytes(physical)
    fixture.bytes(physical, bytes([encoded[0] ^ 1]) + encoded[1:])
    scope = dict(storage_id=fixture.store['id'], source='pack', id=str(pack['id']))
    fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
    for index, member in enumerate(manifest):
        extent = next(row for row in extents if row['chunk_id'] == member['chunk_id'])
        start = extent['offset_bytes']
        member_raw = raw[start:start + extent['length']]
        fixture.put('member-' + str(index), member_raw)
        fixture.read('member-' + str(index), member_raw)
        repaired = database.query('SELECT current_encoding_id,pack_id FROM chunks WHERE id=%s', member['chunk_id'])[0]
        assert repaired['current_encoding_id'] and repaired['pack_id'] is None
        assert database.query('SELECT ordinal,chunk_id,offset_bytes FROM pack_members WHERE pack_id=%s ORDER BY ordinal', pack['id']) == manifest
        remaining = database.query("SELECT id FROM chunks WHERE pack_id=%s AND state='ready'", pack['id'])
        assert len(remaining) == len(manifest) - index - 1
        if index == 0:
            rejected(lambda: fixture.client.copy_object(Bucket=fixture.name,Key='partial-copy',CopySource={'Bucket':fixture.name,'Key':'original'}))
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            assert database.query('SELECT state FROM packs WHERE id=%s', pack['id'])[0]['state'] == 'ready'
    fixture.read('original', raw)
    fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
    assert database.query('SELECT state FROM packs WHERE id=%s', pack['id'])[0]['state'] in ('retired','deleting','deleted')
    assert database.query('SELECT ordinal,chunk_id,offset_bytes FROM pack_members WHERE pack_id=%s ORDER BY ordinal', pack['id']) == manifest

    def deleted():
        fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        return database.query('SELECT state FROM packs WHERE id=%s', pack['id'])[0]['state'] == 'deleted'

    wait(deleted, 'unused old pack did not complete physical deletion')
    assert physical not in fixture.keys()
    fixture.read('original', raw)
    print('PASS incremental member recovery, unchanged manifest and full read after old pack deletion', flush=True)


if __name__ == '__main__':
    main()
