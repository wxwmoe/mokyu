"""Buffered chunk files, durable references, crash recovery and cache ownership."""
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from botocore.exceptions import ClientError, ResponseStreamingError
from urllib3.exceptions import ProtocolError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'buffered-chunks.log').open('a')
    process = None
    markers = []

    def stop(crash=True):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)

        def ready():
            assert process.poll() is None, 'service exited; inspect buffered-chunks.log'
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

    def rows(fixture, key):
        location = 'encoding_id' if database.type == 'postgres' else 'location_id'
        cache = 'staging_object_key' if database.type == 'postgres' else 'cache_key'
        logical = 'c.id' if database.type == 'postgres' else 'c.uuid'
        return database.query(f'''SELECT c.id,l.{location} AS file_uuid,l.sync_pending,
            l.quarantine_code,p.{cache} AS cache_key,coalesce(l.{location},{logical}) AS cache_uuid FROM objects o
            JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id
            LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id
            LEFT JOIN pending_uploads p ON p.chunk_id=c.id
            WHERE o.bucket_id=%s AND o.object_key=%s ORDER BY e.offset_bytes''',
            uuid.UUID(fixture.bucket_id), key if database.type == 'postgres' else key.encode())

    def file(fixture, key):
        source = rows(fixture, key)[0]
        if source['cache_key']:
            identity = uuid.UUID(source['cache_key'].rsplit('.', 1)[0])
            return Path(database.config['instance']['data_path']) / 'pending' / 'chunks' / identity.hex[-2:] / (identity.hex + '.raw')
        identity = source['file_uuid']
        identity = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
        return fixture.root / identity.hex[-2:] / identity.hex

    def synced(fixture, key):
        return all(not row['sync_pending'] for row in rows(fixture, key))

    def rejected(call):
        try:
            call()
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] >= 400
        except (ResponseStreamingError, ProtocolError):
            # A stream whose headers were sent must terminate without a valid body.
            pass
        else:
            raise AssertionError('unavailable source was accepted')

    try:
        start()
        backend = os.environ.pop('MOKYU_TEST_BACKEND', None)
        local, database = Fixture(), Database()
        if backend:
            os.environ['MOKYU_TEST_BACKEND'] = backend
        local.put('durable', b'default durable chunk')
        assert synced(local, 'durable')
        fixtures = [local]
        if backend:
            remote = Fixture()
            configure(remote, upload_cache=True, cache_enabled=True, pack_upload_flush_timeout='300s')
            fixtures.append(remote)

        for fixture in fixtures:
            label = 'S3 cache' if fixture.backend else 'local'
            configure(fixture, write_durability='buffered')
            uploader = arm(fixture, 'upload-before-work') if fixture.backend else None
            sync = arm(fixture, 'buffered-chunk-before-sync')
            data = os.urandom(192 * 1024)
            fixture.put('buffered', data)
            wait(sync.with_suffix('.hit').exists, 'sync did not pause')
            assert not synced(fixture, 'buffered')
            fixture.read('buffered', data)
            with fixture.client.get_object(Bucket=fixture.name, Key='buffered', Range='bytes=17-1041')['Body'] as body:
                assert body.read() == data[17:1042]
            assert fixture.client.head_object(Bucket=fixture.name, Key='buffered')['ContentLength'] == len(data)
            if fixture.backend:
                assert not fixture.keys(), 'buffered cache uploaded before processing'
            if fixture.backend:
                configure(fixture, upload_cache=False)
            else:
                configure(fixture, write_durability='durable')
            with ThreadPoolExecutor(max_workers=2) as pool:
                copied = pool.submit(fixture.client.copy_object, Bucket=fixture.name, Key='copied',
                                     CopySource={'Bucket': fixture.name, 'Key': 'buffered'})
                reused = pool.submit(fixture.put, 'reused', data)
                try:
                    time.sleep(.3)
                    assert not copied.done() and not reused.done(), 'durable reference escaped the barrier'
                finally:
                    sync.unlink(missing_ok=True)
                copied.result(timeout=30)
                reused.result(timeout=30)
            assert synced(fixture, 'buffered')
            assert rows(fixture, 'buffered')[0]['id'] == rows(fixture, 'reused')[0]['id']
            fixture.read('copied', data)
            print('PASS ' + label + ' immediate reads, Range and durable reference barriers', flush=True)

            configure(fixture, write_durability='buffered', upload_cache=bool(fixture.backend))
            sync = arm(fixture, 'buffered-chunk-synced')
            value = os.urandom(96 * 1024)
            fixture.put('commit', value)
            wait(sync.with_suffix('.hit').exists, 'metadata boundary did not pause')
            assert not synced(fixture, 'commit')
            stop()
            sync.unlink(missing_ok=True)
            start()
            fixture.read('commit', value)
            assert synced(fixture, 'commit')
            print('PASS ' + label + ' restart repeats file sync before the database mark', flush=True)

            cached = file(fixture, 'commit') if fixture.backend else None
            if uploader:
                uploader.unlink(missing_ok=True)
            fixture.cli('cache', 'flush', '--wait', '--timeout', '60s')
            fixture.read('buffered', data)
            if cached:
                value = rows(fixture, 'commit')[0]['cache_uuid']
                identity = uuid.UUID(bytes=value) if isinstance(value, bytes) else uuid.UUID(str(value))
                destination = Path(database.config['instance']['data_path']) / 'cache/chunks' / identity.hex[-2:] / (identity.hex + '.raw')
                wait(destination.is_file, 'chunk pending file did not enter read cache')
                assert not cached.exists()
            print('PASS ' + label + ' explicit flush and cache ownership transfer', flush=True)

        if backend:
            configure(remote, upload_cache=True, upload_cache_size=None)
            uploader = arm(remote, 'upload-before-work')
            sync = arm(remote, 'buffered-chunk-before-sync')
            seed = os.urandom(4 * 1024 * 1024)
            remote.put('fallback-seed', seed)
            wait(sync.with_suffix('.hit').exists, 'fallback seed did not pause')
            value = seed + os.urandom(128 * 1024)
            configure(remote, upload_cache_size='1B')
            previous = remote.cli('cache', 'status')['fallbacks']
            with ThreadPoolExecutor(max_workers=1) as pool:
                written = pool.submit(remote.put, 'fallback-reuse', value)
                try:
                    wait(lambda: remote.cli('cache', 'status')['fallbacks'] > previous,
                         'upload cache did not fall back')
                    time.sleep(3)
                    if written.done():
                        written.result()
                        assert rows(remote, 'fallback-reuse')[0]['id'] == rows(remote, 'fallback-seed')[0]['id']
                        raise AssertionError('fallback skipped the existing buffered source barrier')
                finally:
                    sync.unlink(missing_ok=True)
                written.result(timeout=30)
            remote.read('fallback-reuse', value)
            assert rows(remote, 'fallback-reuse')[0]['id'] == rows(remote, 'fallback-seed')[0]['id']
            assert synced(remote, 'fallback-reuse')
            uploader.unlink(missing_ok=True)
            remote.cli('cache', 'flush', '--wait', '--timeout', '60s')
            configure(remote, upload_cache_size=None)
            print('PASS cache overflow fallback synchronizes reused buffered chunks', flush=True)

        sync = arm(local, 'buffered-chunk-before-sync')
        local.put('retry-gate', os.urandom(64 * 1024))
        wait(sync.with_suffix('.hit').exists, 'retry source did not pause')
        value = os.urandom(64 * 1024)
        local.put('retry-first', value)
        damaged = file(local, 'retry-first')
        original = damaged.read_bytes()
        damaged.write_bytes(b'truncated')
        local.put('retry-later', os.urandom(64 * 1024))
        sync.unlink(missing_ok=True)
        wait(lambda: synced(local, 'retry-later'), 'one failed chunk starved later files')
        assert not synced(local, 'retry-first')
        damaged.write_bytes(original)
        wait(lambda: synced(local, 'retry-first'), 'failed chunk was not retried')
        local.read('retry-first', value)
        print('PASS failed chunks retry without starving later files', flush=True)

        upload = local.client.create_multipart_upload(Bucket=local.name, Key='multipart')['UploadId']
        value = os.urandom(5 * 1024 * 1024)
        sync = arm(local, 'buffered-chunk-before-sync')
        with ThreadPoolExecutor(max_workers=1) as pool:
            part = pool.submit(local.client.upload_part, Bucket=local.name, Key='multipart',
                               UploadId=upload, PartNumber=1, Body=value)
            try:
                wait(sync.with_suffix('.hit').exists, 'multipart part did not reach sync')
                assert not part.done(), 'multipart part acknowledged before durable chunks'
            finally:
                sync.unlink(missing_ok=True)
            etag = part.result(timeout=40)['ETag']
            sync = arm(local, 'buffered-chunk-before-sync')
            complete = pool.submit(local.client.complete_multipart_upload, Bucket=local.name, Key='multipart',
                                   UploadId=upload, MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag}]})
            try:
                wait(sync.with_suffix('.hit').exists, 'multipart completion did not sync its assembled tail')
                assert not complete.done(), 'multipart completion skipped its durable barrier'
            finally:
                sync.unlink(missing_ok=True)
            complete.result(timeout=40)
        local.read('multipart', value)
        assert synced(local, 'multipart')
        print('PASS multipart parts and completion preserve durable barriers', flush=True)

        if fault_file := os.environ.get('MOKYU_TEST_FSYNC_FAULT'):
            assert os.environ.get('LD_PRELOAD'), 'fsync regression requires the Linux interposer'
            fault, trace = Path(fault_file), Path(os.environ['MOKYU_TEST_FSYNC_TRACE'])
            try:
                for mode in ['f', 'd']:
                    trace.write_text('')
                    fault.write_text(mode)
                    value = os.urandom(64 * 1024)
                    local.put('sync-error-' + mode, value)
                    wait(lambda: mode + ' 1' in trace.read_text().splitlines(), 'sync error was not injected')
                    assert not synced(local, 'sync-error-' + mode)
                    assert rows(local, 'sync-error-' + mode)[0]['quarantine_code'] is None
                    local.read('sync-error-' + mode, value)
                    if mode == 'f':
                        stop()
                        process = subprocess.Popen(command, stdout=log, stderr=log)
                        process.wait(timeout=30)
                        assert process.returncode != 0, 'startup ignored the failed sync'
                    fault.unlink()
                    if mode == 'f':
                        start()
                    wait(lambda: synced(local, 'sync-error-' + mode), 'failed sync did not retry')
            finally:
                fault.unlink(missing_ok=True)
            print('PASS file and directory sync errors preserve obligations and fail closed at startup', flush=True)

        sync = arm(local, 'buffered-chunk-before-sync')
        local.put('manual-flush', os.urandom(64 * 1024))
        wait(sync.with_suffix('.hit').exists, 'manual flush source did not pause')
        with ThreadPoolExecutor(max_workers=1) as pool:
            flush = pool.submit(local.cli, 'cache', 'flush', '--wait', '--timeout', '30s')
            try:
                time.sleep(.3)
                assert not flush.done(), 'flush ignored local buffered chunks'
            finally:
                sync.unlink(missing_ok=True)
            flush.result(timeout=40)
        assert synced(local, 'manual-flush')
        print('PASS explicit flush waits for local buffered chunks', flush=True)

        if backend:
            configure(remote, pack_enabled=True, pack_max_size='8MiB', compression_enabled=True,
                      compression_strategy='always', pack_upload_flush_timeout='300s')
            key = remote.cli('key', 'create', remote.name + '-key', 'block')
            remote.cli('storage', 'key', 'set', remote.name, key['name'])
            uploader = arm(remote, 'upload-before-work')
            sync = arm(remote, 'buffered-chunk-before-sync')
            value = b''.join(os.urandom(1024) + b'x' * (63 * 1024) for _ in range(96))
            remote.put('packed', value)
            wait(sync.with_suffix('.hit').exists, 'buffered pack source did not pause')
            remote.read('packed', value)
            uploader.unlink(missing_ok=True)
            wait(lambda: database.query("SELECT COUNT(*) AS n FROM packs WHERE storage_id=%s AND state='ready'",
                 uuid.UUID(remote.store['id']))[0]['n'] > 0, 'buffered chunks did not form a durable pack')
            sync.unlink(missing_ok=True)
            remote.cli('cache', 'flush', '--wait', '--timeout', '60s')
            remote.read('packed', value)
            configure(remote, pack_enabled=False, compression_enabled=False)
            remote.cli('storage', 'key', 'clear', remote.name)
            print('PASS cached buffered chunks become encrypted compressed packs while sync is in flight', flush=True)

        configure(local, write_durability='buffered', compression_enabled=True, compression_strategy='always')
        key = local.cli('key', 'create', local.name + '-key', 'block')
        local.cli('storage', 'key', 'set', local.name, key['name'])
        sync = arm(local, 'buffered-chunk-before-sync')
        value = b'encrypted and compressed buffered source\n' * 4096
        local.put('encrypted', value)
        wait(sync.with_suffix('.hit').exists, 'encrypted source did not pause')
        stop()
        sync.unlink(missing_ok=True)
        start()
        local.read('encrypted', value)
        assert synced(local, 'encrypted')
        assert database.query('SELECT reserved_id FROM nonce_counters WHERE block_key_id=%s', uuid.UUID(key['id']))
        print('PASS encrypted buffered source preserves nonce allocation and verifies on restart', flush=True)
        sync = arm(local, 'buffered-chunk-before-sync')
        local.put('shutdown', os.urandom(64 * 1024))
        wait(sync.with_suffix('.hit').exists, 'shutdown source did not pause')
        process.terminate()
        time.sleep(.3)
        assert process.poll() is None, 'shutdown skipped buffered chunks'
        sync.unlink(missing_ok=True)
        process.wait(timeout=40)
        assert process.returncode == 0 and synced(local, 'shutdown')
        print('PASS graceful shutdown waits for buffered chunks', flush=True)
        start()
        for fixture in fixtures:
            label = 'S3 cache' if fixture.backend else 'local'
            if fixture.backend:
                arm(fixture, 'upload-before-work')
            else:
                fixture.cli('storage', 'key', 'clear', fixture.name)
                configure(fixture, compression_enabled=False)
            sync = arm(fixture, 'buffered-chunk-before-sync')
            fixture.put('survives', os.urandom(64 * 1024))
            wait(sync.with_suffix('.hit').exists, 'recovery boundary did not pause')
            values = {key: os.urandom(64 * 1024) for key in ['missing', 'truncated', 'corrupt']}
            for key, value in values.items():
                fixture.put(key, value)
            paths = {key: file(fixture, key) for key in values}
            stop()
            sync.unlink(missing_ok=True)
            paths['missing'].unlink()
            paths['truncated'].write_bytes(b'truncated')
            value = paths['corrupt'].read_bytes()
            paths['corrupt'].write_bytes(bytes([value[0] ^ 1]) + value[1:])
            start()
            assert synced(fixture, 'survives')
            for key, code in [('missing', 'remote_missing'), ('truncated', 'length_mismatch'), ('corrupt', 'hash_mismatch')]:
                assert rows(fixture, key)[0]['quarantine_code'] == code, rows(fixture, key)
                rejected(lambda: fixture.read(key, values[key]))
                rejected(lambda: fixture.client.copy_object(Bucket=fixture.name, Key='bad-copy',
                         CopySource={'Bucket': fixture.name, 'Key': key}))
            print('PASS ' + label + ' restart isolates missing, truncated and corrupt sources', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
