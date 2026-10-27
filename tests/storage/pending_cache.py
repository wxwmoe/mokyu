"""Pending publication, inode reuse and crash recovery across single-mode databases.

Uses source_quarantine's isolated S3 fixture and owns the service process.
MOKYU_TEST_CACHE_CROSS_DEVICE=1 requires cache and pending on distinct filesystems.
"""
import os
import subprocess
import uuid
from pathlib import Path

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    database = Database()
    root = Path(database.config['instance']['data_path'])
    cross = os.environ.get('MOKYU_TEST_CACHE_CROSS_DEVICE') == '1'
    process = None
    log = (faults / 'pending-cache.log').open('a')

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect pending-cache.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                  timeout=5).returncode == 0
        wait(ready, 'service did not recover')

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def identity(value):
        return uuid.UUID(bytes=bytes(value)) if isinstance(value, (bytes, bytearray, memoryview)) else uuid.UUID(str(value))

    def path(namespace, area, value, extension=''):
        value = identity(value).hex
        return root / namespace / area / value[-2:] / (value + extension)

    def inode(file):
        value = file.stat()
        return value.st_dev, value.st_ino

    try:
        start()
        fixture = Fixture()
        assert fixture.backend, 'requires a disposable S3 backend'
        bucket = uuid.UUID(fixture.bucket_id)
        storage = uuid.UUID(fixture.store['id'])
        assert (root / 'cache').stat().st_dev != (root / 'pending').stat().st_dev if cross else True

        def configure(**changes):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(changes)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def key(value):
            return value if database.type == 'postgres' else value.encode()

        def raw_id(name):
            row, = database.query(f'''SELECT r.{database.raw_location} AS file_uuid FROM objects o
                JOIN raw_ranges x ON x.stream_id=o.stream_id JOIN raw_sources r ON r.id=x.source_id
                WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, key(name))
            return identity(row['file_uuid'])

        configure(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False,
                  upload_cache=True, cache_enabled=True, write_processing='inline')
        data = os.urandom(256 * 1024 + 11)
        marker = fixture.arm('raw-upload-before-process')
        fixture.put('raw', data)
        wait(marker.with_suffix('.hit').exists, 'raw upload did not pause')
        source = path('pending', 'raw', raw_id('raw'))
        destination = path('cache', 'raw', raw_id('raw'))
        before = inode(source)
        assert source.read_bytes() == data and not destination.exists()
        fixture.read('raw', data)
        stop(crash=True)
        start()
        fixture.read('raw', data)
        assert inode(source) == before
        marker.unlink()
        wait(lambda: not source.exists(), 'raw pending file was not released')
        if cross:
            assert not destination.exists(), 'cross-device promotion copied data'
        else:
            assert inode(destination) == before and destination.read_bytes() == data
        fixture.read('raw', data)
        print('PASS raw restart, owner protection and inode handoff', flush=True)

        configure(cache_enabled=False)
        marker = fixture.arm('raw-upload-before-process')
        fixture.put('disabled', data)
        wait(marker.with_suffix('.hit').exists, 'disabled-cache upload did not pause')
        source = path('pending', 'raw', raw_id('disabled'))
        destination = path('cache', 'raw', raw_id('disabled'))
        assert source.read_bytes() == data
        marker.unlink()
        wait(lambda: not source.exists(), 'disabled-cache pending file was retained')
        assert not destination.exists()
        fixture.read('disabled', data)
        print('PASS disabled optional cache preserves pending until final publication', flush=True)

        configure(cache_enabled=True, cdc_enabled=True, pack_enabled=False)
        for compressed in (False, True):
            configure(compression_enabled=compressed)
            marker = fixture.arm('upload-before-work')
            name = 'zstd' if compressed else 'chunk'
            data = b'compressed pending candidate' * 4096 if compressed else os.urandom(128 * 1024)
            fixture.put(name, data)
            wait(marker.with_suffix('.hit').exists, 'chunk upload did not pause')
            column = 'u.staging_object_key' if database.type == 'postgres' else 'u.cache_key'
            rows = database.query(f'''SELECT c.id,{column} AS cache_key FROM objects o
                JOIN extents x ON x.stream_id=o.stream_id JOIN chunks c ON c.id=x.chunk_id
                JOIN pending_uploads u ON u.chunk_id=c.id WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, key(name))
            assert rows
            candidates = []
            for row in rows:
                value, extension = row['cache_key'].split('.')
                source = path('pending', 'chunks', value, '.' + extension)
                assert extension == 'raw', 'new chunks must retain plaintext before background encoding'
                candidates.append((row['id'], source, inode(source), '.' + extension))
            fixture.read(name, data)
            marker.unlink()
            for chunk, source, before, extension in candidates:
                wait(lambda: not source.exists(), 'chunk pending file was not released')
                rows = database.query('SELECT location_id FROM chunk_encodings WHERE chunk_id=%s AND state=\'ready\'', chunk)
                assert len(rows) == 1
                destination = path('cache', 'chunks', rows[0]['location_id'], extension)
                if cross:
                    assert not destination.exists(), 'cross-device promotion copied a chunk'
                else:
                    assert inode(destination) == before
            fixture.read(name, data)
        print('PASS pending chunks retain physical identities and inodes across compression settings', flush=True)

        if not cross:
            configure(pack_enabled=True, deduplication_enabled=True, compression_enabled=True)
            packed = b'A' * (4 * 1024 * 1024) + b'B' * (4 * 1024 * 1024) + b'C' * (4 * 1024 * 1024 + 31)
            marker = fixture.arm('upload-before-work')
            fixture.put('packed', packed)
            wait(marker.with_suffix('.hit').exists, 'pack upload did not pause')
            column = 'u.staging_object_key' if database.type == 'postgres' else 'u.cache_key'
            logical = 'c.id' if database.type == 'postgres' else 'c.uuid'

            def pending_chunks(name):
                return database.query(f'''SELECT c.id,{logical} AS file_uuid,{column} AS cache_key FROM objects o
                    JOIN extents x ON x.stream_id=o.stream_id JOIN chunks c ON c.id=x.chunk_id
                    JOIN pending_uploads u ON u.chunk_id=c.id WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, key(name))

            rows = pending_chunks('packed')
            assert len(rows) >= 2
            originals = {}
            for row in rows:
                value, extension = row['cache_key'].split('.')
                source = path('pending', 'chunks', value, '.' + extension)
                originals[row['id']] = source, inode(source), row['file_uuid'], '.' + extension
            marker.unlink()
            wait(lambda: not pending_chunks('packed'), 'pack did not publish')
            for source, before, value, extension in originals.values():
                destination = path('cache', 'chunks', value, extension)
                wait(lambda: not source.exists(), 'pack source was not released')
                assert inode(destination) == before
            fixture.read('packed', packed)

            marker = fixture.arm('upload-before-work')
            fixture.put('packed-again', packed)
            wait(marker.with_suffix('.hit').exists, 'packed reuse did not pause')
            rows = pending_chunks('packed-again')
            assert rows
            originals = {}
            for row in rows:
                value, extension = row['cache_key'].split('.')
                source = path('pending', 'chunks', value, '.' + extension)
                originals[row['id']] = source, inode(source), row['file_uuid'], '.' + extension
            stop(crash=True)
            start()
            fixture.read('packed-again', packed)
            marker.unlink()
            wait(lambda: not pending_chunks('packed-again'), 'packed reuse did not finish')
            for source, before, value, extension in originals.values():
                wait(lambda: not source.exists(), 'packed reuse file was retained')
                assert inode(path('cache', 'chunks', value, extension)) == before
            fixture.read('packed-again', packed)
            print('PASS pack publication and reuse retain final logical identities and inodes', flush=True)

        if not cross:
            configure(cdc_enabled=False, compression_enabled=False, pack_enabled=False)
            for point in ('pending-cache-linked', 'pending-cache-unlinked'):
                marker = fixture.arm(point)
                payload = os.urandom(64 * 1024)
                fixture.put(point, payload)
                wait(marker.with_suffix('.hit').exists, 'handoff did not reach ' + point)
                location = raw_id(point)
                destination = path('cache', 'raw', location)
                before = inode(destination)
                stop(crash=True)
                marker.unlink()
                start()
                assert not path('pending', 'raw', location).exists()
                assert inode(destination) == before and destination.read_bytes() == payload
                fixture.read(point, payload)
            print('PASS crashes around unlink recover one cache file without duplicating disk charges', flush=True)

        wait(lambda: fixture.cli('status')['cache_jobs_active'] == 0, 'cache fills did not finish')
        used = sum(file.stat().st_size for namespace in ('cache', 'pending') for area in ('chunks', 'raw')
                   for file in (root / namespace / area).glob('*/*') if file.is_file())
        assert fixture.cli('status')['local_bytes'][1] == used
        assert not any((root / name).exists() for name in ('raw', 'chunks', 'multipart', 'read-cache'))
        assert not database.query('SELECT source_id FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.storage_id=%s', storage)
        print('PASS canonical layout and exact disk accounting', flush=True)
    finally:
        for marker in faults.glob('*'):
            if marker.is_file() and marker.suffix != '.log':
                marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
