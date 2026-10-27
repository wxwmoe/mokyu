"""Same-stream background raw caching, bounded failures, and recovery on S3."""
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    database = Database()
    root = Path(database.config['instance']['data_path'])
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    log = (faults / 'background-raw.log').open('a')
    process = None

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect background-raw.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not start', seconds=60)

    def stop():
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=40)

    try:
        start()
        fixture = Fixture()
        assert fixture.backend, 'requires an isolated S3 backend'

        def configure(**values):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(values)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def status():
            return fixture.cli('status')

        def idle():
            wait(lambda: status()['read_checks']['active'] == 0 and status()['cache_jobs_active'] == 0,
                 'background raw work did not drain')
            assert status()['read_checks']['used_bytes'] == 0
            wait(lambda: not list((root / 'cache/raw').glob('*/*.tmp')), 'temporary cache file was retained')

        def source(key):
            row, = database.query(f'''SELECT r.{database.raw_location} AS file_uuid FROM objects o
                JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
                WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(fixture.bucket_id),
                key if database.type == 'postgres' else key.encode())
            value = row['file_uuid']
            return value if isinstance(value, uuid.UUID) else uuid.UUID(bytes=bytes(value))

        def cache(key):
            identity = source(key).hex
            return root / 'cache/raw' / identity[-2:] / identity

        configure(cdc_enabled=False, pack_enabled=False, pack_range_enabled=False,
                  deduplication_enabled=False, cache_enabled=True, read_integrity='background')
        body = os.urandom(6 * 1024 * 1024 + 17)
        fixture.cold_put('full', body)
        before = status()
        fixture.read('full', body)
        idle()
        assert cache('full').exists(), status()['read_checks']
        assert cache('full').read_bytes() == body
        after = status()
        assert after['backend_gets'] - before['backend_gets'] == 2, (before['backend_gets'], after['backend_gets'])
        assert after['backend_read_bytes'] - before['backend_read_bytes'] == len(body)
        fixture.read('full', body)
        idle()
        assert status()['backend_gets'] == after['backend_gets']
        with fixture.client.get_object(Bucket=fixture.name, Key='full', Range='bytes=17-511')['Body'] as response:
            assert response.read() == body[17:512]
        idle()
        assert cache('full').read_bytes() == body
        print('PASS complete background raw fills from one download and serves later full/Range reads', flush=True)

        fixture.cold_put('partial', body)
        marker = fixture.arm('read-integrity-check')
        before = status()['backend_read_bytes']
        with fixture.client.get_object(Bucket=fixture.name, Key='partial', Range='bytes=17-511')['Body'] as response:
            assert response.read() == body[17:512]
        assert not cache('partial').exists()
        marker.unlink()
        idle()
        assert status()['backend_read_bytes'] - before == len(body) + 495
        if cache('partial').exists():
            assert cache('partial').read_bytes() == body
        print('PASS a partial response never publishes; the existing full background check may refill verified bytes', flush=True)
        for name, changes in [('off', dict(read_integrity='off')),
                              ('disabled', dict(read_integrity='background', cache_enabled=False)),
                              ('oversized', dict(cache_enabled=True, cache_soft_limit='1MiB'))]:
            configure(**changes)
            fixture.cold_put(name, body)
            fixture.read(name, body)
            idle()
            assert not cache(name).exists(), name
        configure(cache_soft_limit=None)
        print('PASS disabled integrity/cache and oversized files do not publish a raw cache', flush=True)

        small = os.urandom(512 * 1024 + 13)
        fixture.cold_put('slow', small)
        marker = fixture.arm('raw-cache-stream')
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                reading = pool.submit(fixture.read, 'slow', small)
                wait(marker.with_suffix('.hit').exists, 'stream cache writer did not pause')
                reading.result(timeout=5)
                idle()
                assert not cache('slow').exists()
        finally:
            marker.unlink(missing_ok=True)
        fixture.read('slow', small)
        idle()
        assert cache('slow').read_bytes() == small
        print('PASS slow optional cache writes are dropped without blocking the response or losing later admission', flush=True)

        if fault := os.environ.get('MOKYU_TEST_SPACE_FAULT'):
            fixture.cold_put('space', small)
            marker = Path(fault)
            marker.touch()
            try:
                fixture.read('space', small)
                idle()
                assert not cache('space').exists()
            finally:
                marker.unlink(missing_ok=True)
            print('PASS low disk space skips only the optional cache', flush=True)

        fixture.cold_put('cancel', body * 4)
        marker = fixture.arm('read-integrity-check')
        try:
            response = fixture.client.get_object(Bucket=fixture.name, Key='cancel')['Body']
            assert response.read(65536) == body[:65536]
            response.close()
        finally:
            marker.unlink(missing_ok=True)
        idle()
        assert not cache('cancel').exists()

        fixture.cold_put('bad', small)
        identity = source('bad').hex
        physical = fixture.prefix + 'raw/' + identity[-2:] + '/' + identity
        fixture.bytes(physical, b'!' + small[1:])
        before = status()['read_checks']['failed']
        with fixture.client.get_object(Bucket=fixture.name, Key='bad')['Body'] as response:
            assert response.read() == b'!' + small[1:]
        wait(lambda: status()['read_checks']['failed'] > before, 'bad raw digest was not detected')
        idle()
        assert not cache('bad').exists()
        print('PASS cancellation and bad digests leave no published or temporary cache files', flush=True)

        fixture.read('full', body)
        idle()
        assert cache('full').read_bytes() == body
        stop()
        start()
        before = status()['backend_gets']
        fixture.read('full', body)
        idle()
        assert status()['backend_gets'] == before
        assert status()['local_bytes'][1] == sum(path.stat().st_size for path in (root / 'cache').glob('*/*/*') if path.is_file())
        print('PASS restart restores validated caches with exact disk accounting', flush=True)
    finally:
        for path in faults.glob('*'):
            if path.is_file() and path.name in ('raw-cache-stream', 'read-integrity-check', 'space-failure'):
                path.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
