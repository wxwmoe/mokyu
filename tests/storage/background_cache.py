"""Verified maintenance bytes refill the same cache used by foreground reads."""
import os
import json
import subprocess
import tomllib
import uuid
from pathlib import Path

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def identity(value):
    return uuid.UUID(bytes=bytes(value)) if isinstance(value, (bytes, bytearray, memoryview)) else uuid.UUID(str(value))


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    database = Database()
    node = os.environ.get('MOKYU_TEST_NODE_CONFIG')
    config = tomllib.loads(Path(node).read_text()) if node else database.config
    root = Path(config['instance']['data_path'])
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', node or os.environ['MOKYU_TEST_CONFIG']]
    log = (Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'background-cache.log').open('a')
    process = None

    def start():
        nonlocal process
        process = subprocess.Popen(command + (['node'] if node else []), stdout=log, stderr=subprocess.STDOUT)
        def ready():
            assert process.poll() is None, 'service exited; inspect background-cache.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not start', seconds=180)

    def stop():
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=40)

    try:
        start()
        fixture = Fixture()
        assert fixture.backend, 'requires an isolated S3 backend'
        storage = uuid.UUID(fixture.store['id'])
        bucket = uuid.UUID(fixture.bucket_id)
        logical = 'c.id' if database.type == 'postgres' else 'c.uuid'

        def configure(**values):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value={**profile, **values})

        def status():
            if not node:
                return fixture.cli('status')
            reply = subprocess.run(command + ['cli', 'status'], capture_output=True, text=True, timeout=10)
            assert reply.returncode == 0, reply.stderr
            value = json.loads(reply.stdout)
            assert value['db_pool_size'] == 0
            return dict(cache_jobs_active=value['cache']['fills'], backend_gets=value['io']['backend']['read']['completed'],
                        backend_read_bytes=value['io']['backend']['read']['bytes'], cache_hit_bytes=value['cache']['hits'],
                        local_bytes=[value['cache']['bytes']])

        def idle():
            wait(lambda: status()['cache_jobs_active'] == 0, 'optional fills did not drain')

        def empty():
            idle()
            stop()
            for area in ['chunks', 'raw']:
                for path in (root / 'cache' / area).glob('*/*'):
                    if path.is_file():
                        path.unlink()
            start()

        def chunk_hit(key, body, owner=None):
            owner = owner or fixture
            idle()
            rows = database.query(f'''SELECT {logical} AS file_uuid,c.current_encoding_id,c.pack_id,e.offset_bytes,e.length
                FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id
                WHERE o.bucket_id=%s AND o.object_key=%s ORDER BY e.offset_bytes''',
                uuid.UUID(owner.bucket_id), key if database.type == 'postgres' else key.encode())
            matches = []
            for row in rows:
                value = row['file_uuid']
                if row['pack_id'] is None:
                    value = database.query('SELECT location_id FROM chunk_encodings WHERE encoding_id=%s', row['current_encoding_id'])[0]['location_id']
                value = identity(value).hex
                if any((root / 'cache/chunks' / value[-2:] / (value + suffix)).exists() for suffix in ['.raw', '.zst']):
                    matches.append(row)
            assert matches, ('no published chunk cache', key, status()['local_bytes'])
            row = matches[0]
            start = int(row['offset_bytes'])
            end = start + int(row['length']) - 1
            before = status()
            with owner.client.get_object(Bucket=owner.name, Key=key, Range=f'bytes={start}-{end}')['Body'] as response:
                assert response.read() == body[start:end+1]
            after = status()
            assert after['backend_gets'] == before['backend_gets'], (key, before['backend_gets'], after['backend_gets'])
            assert after['cache_hit_bytes'] > before['cache_hit_bytes']

        def raw_path(key, owner=None):
            owner = owner or fixture
            row, = database.query(f'''SELECT r.{database.raw_location} AS file_uuid FROM objects o
                JOIN raw_ranges x ON x.stream_id=o.stream_id JOIN raw_sources r ON r.id=x.source_id
                WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(owner.bucket_id), key if database.type == 'postgres' else key.encode())
            value = identity(row['file_uuid']).hex
            return root / 'cache/raw' / value[-2:] / value

        def raw_hit(key, body):
            idle()
            assert raw_path(key).read_bytes() == body
            before = status()['backend_gets']
            fixture.read(key, body)
            assert status()['backend_gets'] == before

        configure(cdc_enabled=True, cache_enabled=True, pack_enabled=False, read_integrity='off', min_storage_duration='0s')
        small = os.urandom(128 * 1024 + 13)
        fixture.put('encoded', small)
        chunk_hit('encoded', small)
        fixture.cold_put('checked', small + b'checked')
        for mode in ['metadata', 'head']:
            before = status()['backend_read_bytes']
            fixture.finished(fixture.cli('integrity', 'check', '--mode', mode, '--bucket', fixture.name, '--key', 'checked'))
            marker = len(fixture.bytes(fixture.prefix + 'meta.json')) if node and mode == 'head' else 0
            assert status()['backend_read_bytes'] - before == marker, 'metadata/head downloaded a payload'
        fixture.finished(fixture.cli('integrity', 'check', '--mode', 'full', '--bucket', fixture.name, '--key', 'checked'))
        chunk_hit('checked', small + b'checked')
        print('PASS independent encoding and full inspection refill; metadata/head add no body reads', flush=True)

        body = bytes(value & 63 for value in os.urandom(6 * 1024 * 1024 + 17))
        fixture.cold_put('packed', body)
        configure(pack_enabled=True, compression_enabled=True, pack_range_enabled=False, pack_max_size='16MiB')
        fixture.finished(fixture.cli('pack', 'run'))
        chunk_hit('packed', body)
        empty()
        fixture.finished(fixture.cli('integrity', 'check', '--mode', 'full', '--bucket', fixture.name, '--key', 'packed'))
        chunk_hit('packed', body)
        empty()
        key = fixture.name + '-key'
        fixture.cli('key', 'create', key, 'block', '-a', 'aes-256-gcm')
        preview = fixture.cli('storage', 'rewrite', fixture.name, 'preview', '--key', key)
        fixture.finished(fixture.cli('storage', 'rewrite', fixture.name, 'start', '--key', key, '--confirm', preview['confirmation']))
        chunk_hit('packed', body)
        empty()
        configure(pack_enabled=False)
        packs = database.query("SELECT id FROM packs WHERE storage_id=%s AND state='ready'", storage)
        assert packs
        for pack in packs:
            fixture.finished(fixture.cli('pack', 'unpack', str(pack['id']), '--execute'))
        chunk_hit('packed', body)
        print('PASS pack, full pack inspection, encrypted rewrite and unpack feed foreground L2', flush=True)

        configure(cdc_enabled=False, deduplication_enabled=False, compression_enabled=False)
        raw = os.urandom(1024 * 1024 + 19)
        fixture.cold_put('raw-check', raw)
        before = status()
        fixture.finished(fixture.cli('integrity', 'check', '--mode', 'full', '--bucket', fixture.name, '--key', 'raw-check'))
        idle()
        after = status()
        assert after['backend_gets'] - before['backend_gets'] == (3 if node else 2)
        extra = len(fixture.bytes(fixture.prefix + 'meta.json')) if node else 0
        assert after['backend_read_bytes'] - before['backend_read_bytes'] == len(raw) + extra
        raw_hit('raw-check', raw)
        configure(deduplication_enabled=True, write_processing='inline', cache_enabled=False)
        fixture.put('raw-hash', raw + b'hash')
        configure(write_processing='background', cache_enabled=True)
        due = '0' if database.type == 'sqlite' else 'CURRENT_TIMESTAMP'
        database.query(f'''INSERT INTO raw_processing(stream_id,source_id,payload_revision,next_attempt_at)
            SELECT o.stream_id,x.source_id,s.payload_revision,{due} FROM objects o JOIN streams s ON s.id=o.stream_id
            JOIN raw_ranges x ON x.stream_id=o.stream_id WHERE o.bucket_id=%s AND o.object_key=%s''',
            bucket, 'raw-hash' if database.type == 'postgres' else b'raw-hash')
        wait(lambda: raw_path('raw-hash').exists(), 'hashing did not refill raw')
        raw_hit('raw-hash', raw + b'hash')
        print('PASS full raw check and mandatory digest processing reuse the backend stream', flush=True)

        configure(cdc_enabled=True, cache_enabled=bool(node), deduplication_enabled=False)
        marker = fixture.arm('node-conversion-before-read' if node else 'cdc-before-read')
        fixture.put('converted', small + b'converted')
        wait(marker.with_suffix('.hit').exists, 'CDC did not pause')
        if not node:
            configure(cache_enabled=True)
        marker.unlink()
        wait(lambda: database.query('''SELECT e.chunk_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, 'converted' if database.type == 'postgres' else b'converted'), 'CDC did not publish')
        chunk_hit('converted', small + b'converted')
        print('PASS remote CDC output refills after metadata confirmation with read integrity disabled', flush=True)

        target = Fixture()
        configure(cdc_enabled=False, write_processing='inline', deduplication_enabled=False)
        fixture.cold_put('migration-source', raw + b'migration')
        source, = database.query('SELECT stream_id FROM objects WHERE bucket_id=%s AND object_key=%s',
            bucket, 'migration-source' if database.type == 'postgres' else b'migration-source')
        for chunks in [False, True]:
            profile = target.cli('storage', 'profile', 'show', target.name)
            target.cli('storage', 'profile', 'set', target.name, '-', value={**profile,
                'cdc_enabled': chunks, 'pack_range_enabled': False, 'deduplication_enabled': False, 'cache_enabled': True})
            name = 'migration-chunks' if chunks else 'migration-raw'
            queued = fixture.api('/api/v1/media/actions', dict(bucket=fixture.bucket_id, action='copy',
                target_bucket=target.bucket_id, confirm_storage_migration=True,
                objects=[dict(client_id=str(uuid.uuid4()), key='migration-source', version=str(source['stream_id']), target_key=name)]))['results'][0]
            assert queued['status'] == 202, queued
            fixture.finished(queued)
            idle()
            if chunks:
                chunk_hit(name, raw + b'migration', target)
            else:
                assert raw_path(name, target).read_bytes() == raw + b'migration'
                before = status()['backend_gets']
                target.read(name, raw + b'migration')
                assert status()['backend_gets'] == before
            assert raw_path('migration-source').read_bytes() == raw + b'migration'
        print('PASS storage migration reuses verified source bytes and fills raw/chunk destinations after commit', flush=True)
    finally:
        for path in Path(os.environ['MOKYU_TEST_FAULT_DIR']).glob('*'):
            if path.is_file() and path.suffix not in ['.log']:
                path.unlink()
        stop()
        log.close()


if __name__ == '__main__':
    main()
