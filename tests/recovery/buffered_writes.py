"""Raw buffered acknowledgement, durable reuse and restart verification."""
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from botocore.exceptions import ClientError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'buffered-writes.log').open('a')
    process = None
    markers = []

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)

        def ready():
            assert process.poll() is None, 'service exited; inspect buffered-writes.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                  timeout=5).returncode == 0
        wait(ready, 'buffered service did not start', seconds=60)

    def configure(fixture, **changes):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(changes)
        return fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(fixture, name):
        marker = fixture.arm(name)
        markers.append(marker)
        return marker

    def source(fixture, key):
        rows = database.query(f'''SELECT r.id,r.{database.raw_location} AS file_uuid,
            r.durability,r.pending_upload,r.quarantine_code FROM objects o
            JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(fixture.bucket_id),
            key if database.type == 'postgres' else key.encode())
        return rows[0] if rows else None

    def file(fixture, key):
        identity = source(fixture, key)['file_uuid']
        identity = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
        root = Path(database.config['instance']['data_path']) / 'pending' if fixture.backend else fixture.root
        return root / 'raw' / identity.hex[-2:] / identity.hex

    try:
        start()
        backend = os.environ.pop('MOKYU_TEST_BACKEND', None)
        fixture, database = Fixture(), Database()
        if backend:
            os.environ['MOKYU_TEST_BACKEND'] = backend
        assert fixture.cli('storage', 'profile', 'show', fixture.name)['write_durability'] == 'durable'
        configure(fixture, cdc_enabled=False, deduplication_enabled=True, pack_range_enabled=False)
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        for value in ['volatile', 'unknown', 'BUFFERED']:
            fixture.cli('storage', 'profile', 'set', fixture.name, '-',
                        value={**profile, 'write_durability': value}, succeeds=False)
        assert fixture.cli('storage', 'profile', 'show', fixture.name)['revision'] == profile['revision']
        legacy = configure(fixture, write_durability='buffered')
        legacy.pop('write_durability')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=legacy)
        assert fixture.cli('storage', 'profile', 'show', fixture.name)['write_durability'] == 'durable'
        durable = arm(fixture, 'raw-file-durable')
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(fixture.put, 'default', b'default durable')
            try:
                wait(durable.with_suffix('.hit').exists, 'durable default skipped its sync boundary')
                assert not request.done()
            finally:
                durable.unlink(missing_ok=True)
            request.result(timeout=20)
        print('PASS durable default and invalid policy validation', flush=True)

        configure(fixture, write_durability='buffered')
        marker = arm(fixture, 'raw-before-writer')
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(fixture.put, 'allocated-buffered', b'allocated before writing')
            try:
                wait(marker.with_suffix('.hit').exists, 'buffered allocation did not pause')
                rows = database.query("SELECT durability FROM raw_sources WHERE storage_id=%s AND state='writing'",
                                      uuid.UUID(fixture.store['id']))
                assert rows == [{'durability': 'buffered'}], rows
                assert not request.done()
            finally:
                marker.unlink(missing_ok=True)
            request.result(timeout=30)
        fixture.read('allocated-buffered', b'allocated before writing')
        wait(lambda: source(fixture, 'allocated-buffered')['durability'] == 'durable',
             'allocated buffered source did not synchronize')
        print('PASS buffered durability is committed before the first file write', flush=True)

        for initial, changed, boundary in [('buffered', 'durable', 'raw-file-durable'),
                                           ('durable', 'buffered', 'raw-file-buffered')]:
            configure(fixture, write_durability=initial, deduplication_enabled=False)
            allocated = arm(fixture, 'raw-before-writer')
            written = arm(fixture, boundary)
            key = 'policy-' + initial
            with ThreadPoolExecutor(max_workers=1) as pool:
                request = pool.submit(fixture.put, key, key.encode())
                try:
                    wait(allocated.with_suffix('.hit').exists, 'policy allocation did not pause')
                    rows = database.query("SELECT durability FROM raw_sources WHERE storage_id=%s AND state='writing'",
                                          uuid.UUID(fixture.store['id']))
                    assert rows == [{'durability': initial}], rows
                    configure(fixture, write_durability=changed)
                    allocated.unlink(missing_ok=True)
                    wait(written.with_suffix('.hit').exists, 'writer ignored the changed durability policy')
                    rows = database.query("SELECT durability FROM raw_sources WHERE storage_id=%s AND state='writing'",
                                          uuid.UUID(fixture.store['id']))
                    assert rows == [{'durability': 'buffered'}], rows
                    assert not request.done()
                finally:
                    allocated.unlink(missing_ok=True)
                    written.unlink(missing_ok=True)
                request.result(timeout=30)
            fixture.read(key, key.encode())
            wait(lambda: source(fixture, key)['durability'] == 'durable', 'changed policy left an unsynced source')
        configure(fixture, write_durability='buffered', deduplication_enabled=True)
        print('PASS policy changes retain conservative durability and required file sync', flush=True)

        marker = arm(fixture, 'buffered-before-sync')
        value = os.urandom(256 * 1024)
        fixture.put('buffered', value)
        wait(marker.with_suffix('.hit').exists, 'buffered sync worker did not pause')
        assert source(fixture, 'buffered')['durability'] == 'buffered'
        fixture.read('buffered', value)
        with fixture.client.get_object(Bucket=fixture.name, Key='buffered', Range='bytes=17-1041')['Body'] as body:
            assert body.read() == value[17:1042]
        assert fixture.client.head_object(Bucket=fixture.name, Key='buffered')['ContentLength'] == len(value)
        print('PASS complete raw is acknowledged and immediately readable before fsync', flush=True)

        configure(fixture, write_durability='durable')
        with ThreadPoolExecutor(max_workers=2) as pool:
            copied = pool.submit(fixture.client.copy_object, Bucket=fixture.name, Key='copied',
                                 CopySource={'Bucket': fixture.name, 'Key': 'buffered'})
            reused = pool.submit(fixture.put, 'reused', value)
            try:
                time.sleep(.3)
                assert not copied.done() and not reused.done(), 'durable reference escaped the sync barrier'
            finally:
                marker.unlink(missing_ok=True)
            copied.result(timeout=30)
            reused.result(timeout=30)
        assert source(fixture, 'buffered')['durability'] == 'durable'
        assert source(fixture, 'reused')['id'] == source(fixture, 'buffered')['id']
        fixture.read('copied', value)
        print('PASS durable copy and inline dedup synchronize the reused buffered source', flush=True)

        configure(fixture, write_durability='buffered', deduplication_enabled=False)
        marker = arm(fixture, 'buffered-before-sync')
        fixture.put('flush', b'flush-me')
        wait(marker.with_suffix('.hit').exists, 'flush source did not pause')
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(fixture.cli, 'cache', 'flush', '--wait', '--timeout', '20s')
            try:
                time.sleep(.3)
                assert not request.done(), 'flush omitted local buffered files'
            finally:
                marker.unlink(missing_ok=True)
            assert request.result(timeout=30)['state'] == 'completed'
        assert source(fixture, 'flush')['durability'] == 'durable'
        print('PASS manual flush includes local buffered files', flush=True)

        configure(fixture, deduplication_enabled=True)
        marker = arm(fixture, 'buffered-before-sync')
        duplicate = os.urandom(64 * 1024)
        fixture.put('candidate', duplicate)
        wait(marker.with_suffix('.hit').exists, 'dedup candidate did not pause')
        configure(fixture, write_durability='durable', write_processing='background')
        processing = arm(fixture, 'raw-processing-before-switch')
        fixture.put('background-reuse', duplicate)
        wait(processing.with_suffix('.hit').exists, 'background dedup did not reach its switch')
        processing.unlink(missing_ok=True)
        time.sleep(.3)
        assert source(fixture, 'background-reuse')['id'] != source(fixture, 'candidate')['id']
        assert source(fixture, 'background-reuse')['durability'] == 'durable'
        marker.unlink(missing_ok=True)
        wait(lambda: source(fixture, 'background-reuse')['id'] == source(fixture, 'candidate')['id'],
             'background dedup did not switch to the synchronized candidate')
        fixture.read('background-reuse', duplicate)
        configure(fixture, write_durability='buffered', write_processing='inline', deduplication_enabled=False)
        print('PASS background dedup keeps the durable source until its buffered candidate is synced', flush=True)

        if fault_file := os.environ.get('MOKYU_TEST_FSYNC_FAULT'):
            assert os.environ.get('LD_PRELOAD'), 'fsync test requires its Linux interposer'
            fault, trace = Path(fault_file), Path(os.environ['MOKYU_TEST_FSYNC_TRACE'])
            try:
                for mode in ['f', 'd']:
                    marker = arm(fixture, 'buffered-before-sync')
                    key = 'sync-error-' + mode
                    fixture.put(key, value)
                    wait(marker.with_suffix('.hit').exists, 'error source did not pause')
                    trace.write_text('')
                    fault.write_text(mode)
                    marker.unlink(missing_ok=True)
                    wait(lambda: mode + ' 1' in trace.read_text().splitlines(), 'sync failure was not injected')
                    assert source(fixture, key)['durability'] == 'buffered'
                    assert source(fixture, key)['quarantine_code'] is None
                    fixture.read(key, value)
                    if mode == 'f':
                        stop(crash=True)
                        process = subprocess.Popen(command, stdout=log, stderr=log)
                        process.wait(timeout=30)
                        assert process.returncode != 0, 'startup accepted an unresolved sync failure'
                        assert source(fixture, key)['quarantine_code'] is None
                    fault.unlink()
                    if mode == 'f':
                        start()
                    wait(lambda: source(fixture, key)['durability'] == 'durable', 'sync did not retry')
                    fixture.read(key, value)
            finally:
                fault.unlink(missing_ok=True)
            print('PASS file and directory sync failures retain obligations, retry and prevent unsafe startup', flush=True)

        configure(fixture, cdc_enabled=True, write_processing='background')
        sync = arm(fixture, 'buffered-before-sync')
        conversion = arm(fixture, 'cdc-before-switch')
        fixture.put('conversion', value)
        wait(conversion.with_suffix('.hit').exists, 'CDC did not prepare its replacement')
        sync.unlink(missing_ok=True)
        wait(lambda: source(fixture, 'conversion')['durability'] == 'durable', 'CDC source did not sync')
        conversion.unlink(missing_ok=True)
        wait(lambda: not source(fixture, 'conversion'), 'CDC did not switch after source sync')
        fixture.read('conversion', value)
        configure(fixture, cdc_enabled=False, write_processing='inline')
        print('PASS syncing an accepted source does not invalidate the CDC snapshot', flush=True)

        marker = arm(fixture, 'buffered-before-sync')
        fixture.put('retry-gate', value)
        wait(marker.with_suffix('.hit').exists, 'retry source did not pause')
        fixture.put('retry-first', value)
        damaged = file(fixture, 'retry-first')
        damaged.write_bytes(b'truncated')
        fixture.put('retry-later', value)
        marker.unlink(missing_ok=True)
        wait(lambda: source(fixture, 'retry-later')['durability'] == 'durable', 'one failed file blocked later syncs')
        assert source(fixture, 'retry-first')['durability'] == 'buffered'
        damaged.write_bytes(value)
        wait(lambda: source(fixture, 'retry-first')['durability'] == 'durable', 'failed source was not retried')
        fixture.read('retry-first', value)
        fixture.read('retry-later', value)
        print('PASS a failed source does not starve later buffered files and retries on the next scan', flush=True)

        marker = arm(fixture, 'buffered-file-synced')
        fixture.put('sync-commit', b'synced before metadata commit')
        wait(marker.with_suffix('.hit').exists, 'sync did not reach its metadata boundary')
        assert source(fixture, 'sync-commit')['durability'] == 'buffered'
        stop(crash=True)
        marker.unlink(missing_ok=True)
        start()
        fixture.read('sync-commit', b'synced before metadata commit')
        assert source(fixture, 'sync-commit')['durability'] == 'durable'
        print('PASS restart repeats sync safely after the file barrier but before its database update', flush=True)

        marker = arm(fixture, 'buffered-before-sync')
        fixture.put('survives', value)
        for key in ['missing', 'truncated', 'corrupt']:
            fixture.put(key, value)
        wait(marker.with_suffix('.hit').exists, 'restart sources did not pause')
        paths = {key: file(fixture, key) for key in ['missing', 'truncated', 'corrupt']}
        stop(crash=True)
        marker.unlink(missing_ok=True)
        paths['missing'].unlink()
        paths['truncated'].write_bytes(value[:12])
        paths['corrupt'].write_bytes(bytes([value[0] ^ 1]) + value[1:])
        start()
        fixture.read('survives', value)
        assert source(fixture, 'survives')['durability'] == 'durable'
        for key, code in [('missing', 'remote_missing'), ('truncated', 'length_mismatch'), ('corrupt', 'hash_mismatch')]:
            assert source(fixture, key)['quarantine_code'] == code
            try:
                fixture.read(key, value)
            except ClientError as error:
                assert error.response['ResponseMetadata']['HTTPStatusCode'] >= 400
            else:
                raise AssertionError('recovery served a damaged buffered file')
        print('PASS restart fully verifies buffered sources and isolates missing, truncated and corrupt files', flush=True)

        if backend:
            remote = Fixture()
            configure(remote, cdc_enabled=False, pack_range_enabled=False, upload_cache=True,
                      cache_enabled=True, write_durability='buffered')
            uploader = arm(remote, 'raw-upload-before-process')
            sync = arm(remote, 'buffered-before-sync')
            remote.put('pending', value)
            wait(sync.with_suffix('.hit').exists, 'remote buffered sync did not pause')
            assert source(remote, 'pending')['pending_upload']
            assert source(remote, 'pending')['durability'] == 'buffered'
            remote.read('pending', value)
            cached = file(remote, 'pending')
            assert cached.is_file()
            stop(crash=True)
            sync.unlink(missing_ok=True)
            start()
            remote.read('pending', value)
            assert source(remote, 'pending')['durability'] == 'durable'
            uploader.unlink(missing_ok=True)
            remote.cli('cache', 'flush', '--wait', '--timeout', '30s')
            destination = Path(database.config['instance']['data_path']) / 'cache' / cached.relative_to(
                Path(database.config['instance']['data_path']) / 'pending')
            wait(destination.is_file, 'raw pending file did not enter read cache')
            assert not source(remote, 'pending')['pending_upload'] and not cached.exists()
            remote.read('pending', value)
            print('PASS S3 buffered cache survives restart and becomes read cache after durable upload', flush=True)

            configure(remote, upload_cache_size='1KiB', upload_cache_overflow='fallback')
            remote.put('fallback', value)
            assert not source(remote, 'fallback')['pending_upload']
            remote.read('fallback', value)
            configure(remote, upload_cache_overflow='reject')
            try:
                remote.put('rejected', value)
            except ClientError as error:
                assert error.response['Error']['Code'] == 'SlowDown'
                assert error.response['ResponseMetadata']['HTTPStatusCode'] == 503
            else:
                raise AssertionError('a full buffered cache ignored the rejection policy')
            assert source(remote, 'rejected') is None
            print('PASS buffered cache overflow falls back to final S3 or rejects without publishing', flush=True)

        marker = arm(fixture, 'buffered-before-sync')
        fixture.put('shutdown', b'graceful shutdown')
        wait(marker.with_suffix('.hit').exists, 'shutdown source did not pause')
        process.terminate()
        time.sleep(.3)
        assert process.poll() is None, 'shutdown exited before its buffered flush'
        marker.unlink(missing_ok=True)
        process.wait(timeout=40)
        assert process.returncode == 0
        assert source(fixture, 'shutdown')['durability'] == 'durable'
        print('PASS graceful shutdown waits for buffered local files', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        stop(crash=True)
        log.close()


if __name__ == '__main__':
    main()
