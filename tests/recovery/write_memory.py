"""Bounded volatile sources, durable references and lost-RAM recovery on all databases."""
import base64
import os
import re
import socket
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

from botocore.exceptions import ClientError, ResponseStreamingError
from urllib3.exceptions import ProtocolError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    assert os.environ.get('MOKYU_TEST_BACKEND'), 'requires an isolated S3 backend'
    config = Path(os.environ['MOKYU_TEST_CONFIG'])
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'write-memory.log').open('a')
    process = None
    markers = []

    def memory_size(value):
        text = re.sub(r'^write_memory_size = .*\n', '', config.read_text(), flags=re.M)
        config.write_text(text.replace('[instance]\n', '[instance]\nwrite_memory_size = "' + value + '"\n'))

    def stop(crash=True):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)

        def ready():
            assert process.poll() is None, 'service exited; inspect write-memory.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                  timeout=5).returncode == 0
        wait(ready, 'memory service did not start', seconds=60)

    def configure(fixture, **changes):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(changes)
        return fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(fixture, name):
        marker = fixture.arm(name)
        markers.append(marker)
        return marker

    def repair(fixture, key, value):
        names = ['repair-prepared', 'repair-uploading', 'repair-stored', 'repair-before-commit']
        checkpoints = [arm(fixture, name) for name in names]
        with ThreadPoolExecutor(max_workers=1) as pool:
            writing = pool.submit(fixture.put, key, value)
            try:
                for name, checkpoint in zip(names, checkpoints):
                    wait(lambda: checkpoint.with_suffix('.hit').exists() or writing.done(),
                         'repair did not reach ' + name)
                    if writing.done():
                        print('Repair stopped before ' + name, flush=True)
                        writing.result()
                    assert checkpoint.with_suffix('.hit').exists()
                    checkpoint.unlink(missing_ok=True)
                writing.result(timeout=30)
            finally:
                for checkpoint in checkpoints:
                    checkpoint.unlink(missing_ok=True)

    def key_value(key):
        return key if database.type == 'postgres' else key.encode()

    def raw(key):
        return database.query(f'''SELECT r.id,r.{database.raw_location} AS file_uuid,
            r.medium,r.boot_id,r.durability,r.pending_upload,r.quarantine_code FROM objects o
            JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(remote.bucket_id), key_value(key))[0]

    def chunks(key):
        location = 'encoding_id' if database.type == 'postgres' else 'location_id'
        return database.query(f'''SELECT c.id,c.pack_id,l.{location} AS file_uuid,l.sync_pending,
            l.boot_id,l.quarantine_code,l.state AS encoding_state FROM objects o JOIN extents e ON e.stream_id=o.stream_id
            JOIN chunks c ON c.id=e.chunk_id JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id
            WHERE o.bucket_id=%s AND o.object_key=%s ORDER BY e.offset_bytes''',
            uuid.UUID(chunked.bucket_id), key_value(key))

    def path(row, chunk=False):
        identity = row['file_uuid']
        identity = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
        return Path(database.config['instance']['data_path']) / ('chunks' if chunk else 'raw') / identity.hex[-2:] / (identity.hex + ('.raw' if chunk else ''))

    def used():
        return remote.cli('cache', 'status')['write_memory']['used']

    def rejected(call, status=None):
        try:
            call()
        except ClientError as error:
            actual = error.response['ResponseMetadata']['HTTPStatusCode']
            assert actual == status if status else actual >= 400
        except (ResponseStreamingError, ProtocolError):
            assert status is None
        else:
            raise AssertionError('unavailable source or exhausted budget was accepted')

    def read_ranges(fixture, key, value):
        fixture.read(key, value)
        with fixture.client.get_object(Bucket=fixture.name, Key=key, Range='bytes=17-1041')['Body'] as body:
            assert body.read() == value[17:1042]
        assert fixture.client.head_object(Bucket=fixture.name, Key=key)['ContentLength'] == len(value)

    try:
        settings = re.sub(r'^cache_size = .*$', 'cache_size = "64MiB"', config.read_text(), flags=re.M)
        config.write_text(settings)
        memory_size('8MiB')
        start()
        remote, database = Fixture(), Database()
        configure(remote, cdc_enabled=False, pack_range_enabled=False, upload_cache=True,
                  cache_enabled=True, write_durability='volatile')
        status = remote.cli('cache', 'status')
        assert status['write_memory'] == {'capacity': 8 * 1024 * 1024, 'used': 0}
        profile = remote.cli('storage', 'profile', 'show', remote.name)
        remote.cli('storage', 'profile', 'set', remote.name, '-',
                   value={**profile, 'upload_cache': False}, succeeds=False)
        backend = os.environ.pop('MOKYU_TEST_BACKEND')
        try:
            local = Fixture()
        finally:
            os.environ['MOKYU_TEST_BACKEND'] = backend
        profile = local.cli('storage', 'profile', 'show', local.name)
        local.cli('storage', 'profile', 'set', local.name, '-',
                  value={**profile, 'write_durability': 'volatile'}, succeeds=False)
        print('PASS bounded instance budget and illegal volatile profiles', flush=True)

        rejected(lambda: remote.client.put_object(Bucket=remote.name, Key='bad-digest',
                 Body=b'bad digest', ContentMD5=base64.b64encode(bytes(16)).decode()), 400)
        assert used() == 0
        rejected(lambda: remote.client.head_object(Bucket=remote.name, Key='bad-digest'), 404)
        url = urlsplit(remote.client.generate_presigned_url('put_object',
                       Params={'Bucket': remote.name, 'Key': 'disconnected'}, ExpiresIn=60))
        with socket.create_connection((url.hostname, url.port or 80), timeout=10) as connection:
            request = (f'PUT {url.path}?{url.query} HTTP/1.1\r\nHost: {url.netloc}\r\n'
                       'Content-Length: 65536\r\nConnection: close\r\n\r\nx').encode()
            connection.sendall(request)
            wait(lambda: used() > 0, 'incomplete upload did not reserve RAM', seconds=10)
        wait(lambda: used() == 0, 'disconnected upload leaked its RAM reservation')
        rejected(lambda: remote.client.head_object(Bucket=remote.name, Key='disconnected'), 404)
        print('PASS checksum failure and interrupted receive release RAM without publishing', flush=True)

        uploader = arm(remote, 'raw-upload-before-process')
        value = os.urandom(192 * 1024)
        remote.put('memory', value)
        wait(uploader.with_suffix('.hit').exists, 'raw uploader did not pause')
        row = raw('memory')
        assert row['medium'] == 'memory' and row['boot_id'] and row['durability'] == 'volatile'
        assert row['pending_upload'] and not path(row).exists() and not remote.keys()
        assert used() == len(value)
        read_ranges(remote, 'memory', value)
        print('PASS raw RAM acknowledgement, UUID identity, immediate GET, HEAD and Range', flush=True)

        reader = arm(remote, 'read-body')
        with ThreadPoolExecutor(max_workers=1) as pool:
            reading = pool.submit(remote.read, 'memory', value)
            try:
                wait(reader.with_suffix('.hit').exists, 'RAM reader did not hold its body')
                uploader.unlink(missing_ok=True)
                wait(lambda: not raw('memory')['pending_upload'], 'RAM source did not reach S3')
                assert used() >= len(value), 'reader released its RAM allowance too early'
            finally:
                reader.unlink(missing_ok=True)
            reading.result(timeout=30)
        wait(lambda: used() == 0, 'last reader did not release memory')
        stop()
        start()
        remote.read('memory', value)
        assert raw('memory')['boot_id'] is None
        print('PASS last-reader accounting and final publication across restart', flush=True)

        uploader = arm(remote, 'raw-upload-before-process')
        remote.put('promote', value)
        sync = arm(remote, 'raw-file-durable')
        with ThreadPoolExecutor(max_workers=1) as pool:
            copying = pool.submit(remote.client.copy_object, Bucket=remote.name, Key='copy',
                                  CopySource={'Bucket': remote.name, 'Key': 'promote'})
            try:
                wait(sync.with_suffix('.hit').exists, 'Copy skipped RAM persistence')
                assert not copying.done() and raw('promote')['medium'] == 'memory'
            finally:
                sync.unlink(missing_ok=True)
            copying.result(timeout=30)
        assert raw('copy')['durability'] == 'durable' and raw('copy')['boot_id'] is None
        assert path(raw('copy')).read_bytes() == value
        stop()
        start()
        remote.read('copy', value)
        uploader.unlink(missing_ok=True)
        remote.cli('cache', 'flush', '--wait', '--timeout', '60s')
        print('PASS Copy creates durable data before success even when its input remains volatile', flush=True)

        configure(remote, write_durability='durable', upload_cache=False)
        remote.put('lost', b'previous version')
        configure(remote, write_durability='volatile', upload_cache=True)
        uploader = arm(remote, 'raw-upload-before-process')
        remote.put('lost', value)
        assert raw('lost')['medium'] == 'memory'
        stop()
        start()
        assert raw('lost')['quarantine_code'] == 'remote_missing' and not raw('lost')['pending_upload']
        rejected(lambda: remote.read('lost', value))
        rejected(lambda: remote.client.copy_object(Bucket=remote.name, Key='bad-copy',
                                                   CopySource={'Bucket': remote.name, 'Key': 'lost'}))
        assert remote.api('/api/v1/maintenance/quarantines?storage_id=' + remote.store['id'] + '&source=raw')['items']
        uploader.unlink(missing_ok=True)
        configure(remote, write_durability='durable', upload_cache=False)
        remote.put('lost', value)
        remote.read('lost', value)
        print('PASS restart isolates lost RAM, never exposes an older overwrite, and allows repair', flush=True)

        chunked = Fixture()
        configure(chunked, upload_cache=True, cache_enabled=True, write_durability='volatile',
                  pack_upload_flush_timeout='1s')
        uploader = arm(chunked, 'upload-before-work')
        value = os.urandom(192 * 1024)
        chunked.put('memory', value)
        wait(uploader.with_suffix('.hit').exists, 'chunk uploader did not pause')
        assert chunks('memory')[0]['boot_id'] and not path(chunks('memory')[0], True).exists()
        read_ranges(chunked, 'memory', value)
        configure(chunked, write_durability='durable')
        chunked.put('reused', value)
        assert chunks('memory')[0]['id'] == chunks('reused')[0]['id']
        assert not chunks('memory')[0]['sync_pending'] and chunks('memory')[0]['boot_id'] is None
        assert path(chunks('memory')[0], True).read_bytes() == value
        stop()
        start()
        chunked.read('reused', value)
        uploader.unlink(missing_ok=True)
        chunked.cli('cache', 'flush', '--wait', '--timeout', '60s')
        print('PASS chunks RAM reads and durable deduplication references survive restart', flush=True)

        configure(chunked, write_durability='volatile')
        uploader = arm(chunked, 'upload-before-work')
        lost = os.urandom(128 * 1024)
        chunked.put('lost', lost)
        assert chunks('lost')[0]['boot_id']
        stop()
        start()
        assert chunks('lost')[0]['quarantine_code'] == 'remote_missing'
        assert chunks('lost')[0]['encoding_state'] == 'uploading'
        rejected(lambda: chunked.read('lost', lost))
        rejected(lambda: chunked.client.copy_object(Bucket=chunked.name, Key='bad-copy',
                                                    CopySource={'Bucket': chunked.name, 'Key': 'lost'}))
        assert chunked.api('/api/v1/maintenance/quarantines?storage_id=' + chunked.store['id'] + '&source=chunk')['items']
        uploader.unlink(missing_ok=True)
        repair(chunked, 'repaired', lost)
        chunked.read('lost', lost)
        chunked.read('repaired', lost)
        print('PASS lost chunk RAM is quarantined and identical retransmission repairs references', flush=True)

        configure(remote, write_durability='volatile', upload_cache=True)
        uploader = arm(remote, 'raw-upload-before-process')
        full = os.urandom(8 * 1024 * 1024)
        remote.put('full', full)
        assert raw('full')['medium'] == 'memory' and used() == len(full)
        small = os.urandom(64 * 1024)
        sync = arm(remote, 'buffered-file-synced')
        remote.put('fallback-file', small)
        wait(sync.with_suffix('.hit').exists, 'file fallback did not reach its sync barrier')
        assert raw('fallback-file')['medium'] == 'local' and raw('fallback-file')['boot_id'] is None
        assert path(raw('fallback-file')).read_bytes() == small and used() == len(full)
        configure(remote, upload_cache_size='1B', upload_cache_overflow='reject')
        rejected(lambda: remote.put('reject', small), 503)
        rejected(lambda: remote.client.head_object(Bucket=remote.name, Key='reject'), 404)
        configure(remote, upload_cache_overflow='fallback')
        remote.put('fallback-final', small)
        final = raw('fallback-final')
        assert not final['pending_upload'] and final['durability'] == 'durable', final
        remote.read('fallback-final', small)
        configure(remote, cache_soft_limit='1B')
        rejected(lambda: remote.put('no-space', small), 503)
        assert used() == len(full)
        sync.unlink(missing_ok=True)
        wait(lambda: raw('fallback-file')['durability'] == 'durable', 'file fallback was not synced')
        remote.cli('cache', 'flush', '--wait', '--timeout', '20s', succeeds=False)
        assert raw('full')['durability'] == 'volatile' and used() == len(full)
        remote.read('full', full)
        configure(remote, cache_soft_limit=None, upload_cache_size=None)
        with ThreadPoolExecutor(max_workers=1) as pool:
            flushing = pool.submit(remote.cli, 'cache', 'flush', '--wait', '--timeout', '60s')
            try:
                wait(lambda: raw('full')['durability'] == 'durable' or flushing.done(),
                     'flush did not persist its RAM source')
                if flushing.done():
                    flushing.result()
                assert raw('full')['durability'] == 'durable'
                assert raw('full')['boot_id'] is None and path(raw('full')).read_bytes() == full
                assert not flushing.done(), 'flush ignored the pending final S3 upload'
            finally:
                uploader.unlink(missing_ok=True)
            flushing.result(timeout=70)
        remote.read('full', full)
        wait(lambda: used() == 0, 'budget was not released after fallback/flush')
        print('PASS shared RAM cap, file fallback, final-S3 fallback and retryable exhaustion', flush=True)

        disk_uploader = arm(remote, 'raw-upload-before-process')
        disk_value = os.urandom(12 * 1024 * 1024)
        remote.put('disk-pressure', disk_value)
        assert raw('disk-pressure')['medium'] == 'local'
        configure(chunked, write_durability='volatile', upload_cache_overflow='fallback')
        uploader = arm(chunked, 'upload-before-work')
        pressure = arm(chunked, 'buffered-chunk-before-final')
        mixed = os.urandom(12 * 1024 * 1024)
        with ThreadPoolExecutor(max_workers=1) as pool:
            writing = pool.submit(chunked.put, 'mixed-pressure', mixed)
            try:
                wait(lambda: pressure.with_suffix('.hit').exists() or writing.done(),
                     'mixed RAM/final request did not reach its durable wait', seconds=60)
                if writing.done():
                    writing.result()
                assert pressure.with_suffix('.hit').exists() and not writing.done()
                rejected(lambda: chunked.client.head_object(Bucket=chunked.name, Key='mixed-pressure'), 404)
            finally:
                pressure.unlink(missing_ok=True)
                uploader.unlink(missing_ok=True)
            writing.result(timeout=60)
        assert all(not c['sync_pending'] and c['boot_id'] is None for c in chunks('mixed-pressure'))
        chunked.read('mixed-pressure', mixed)
        disk_uploader.unlink(missing_ok=True)
        remote.cli('cache', 'flush', '--wait', '--timeout', '60s')
        stop()
        start()
        chunked.read('mixed-pressure', mixed)
        remote.read('disk-pressure', disk_value)
        wait(lambda: used() == 0, 'mixed request retained RAM after final upload')
        print('PASS mixed RAM/final fallback waits for durable upload under full file quota', flush=True)

        uploader = arm(remote, 'raw-upload-before-process')
        upload = remote.client.create_multipart_upload(Bucket=remote.name, Key='multipart')['UploadId']
        part = remote.client.upload_part(Bucket=remote.name, Key='multipart', UploadId=upload,
                                         PartNumber=1, Body=small)
        remote.client.complete_multipart_upload(Bucket=remote.name, Key='multipart', UploadId=upload,
            MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
        assert raw('multipart')['durability'] == 'durable' and raw('multipart')['boot_id'] is None
        stop()
        start()
        remote.read('multipart', small)
        uploader.unlink(missing_ok=True)
        remote.cli('cache', 'flush', '--wait', '--timeout', '60s')
        print('PASS volatile profile keeps Multipart checkpoints and Complete durable', flush=True)

        configure(chunked, pack_enabled=True, pack_max_size='8MiB', compression_enabled=True,
                  compression_strategy='always')
        key = chunked.cli('key', 'create', chunked.name + '-key', 'block')
        chunked.cli('storage', 'key', 'set', chunked.name, key['name'])
        uploader = arm(chunked, 'upload-before-work')
        lost = os.urandom(128 * 1024)
        chunked.put('encrypted-lost', lost)
        assert chunks('encrypted-lost')[0]['boot_id']
        stop()
        start()
        assert chunks('encrypted-lost')[0]['quarantine_code'] == 'remote_missing'
        assert chunks('encrypted-lost')[0]['encoding_state'] == 'uploading'
        stop()
        start()
        assert chunks('encrypted-lost')[0]['encoding_state'] == 'uploading'
        rejected(lambda: chunked.read('encrypted-lost', lost))
        uploader.unlink(missing_ok=True)
        repair(chunked, 'encrypted-repaired', lost)
        chunked.read('encrypted-lost', lost)
        chunked.cli('cache', 'flush', '--wait', '--timeout', '60s')
        print('PASS encrypted RAM loss preserves nonce validation and identical retransmission', flush=True)

        release = arm(chunked, 'upload-before-release')
        packed = b''.join(os.urandom(1024) + b'x' * (63 * 1024) for _ in range(96))
        chunked.put('packed', packed)
        wait(lambda: database.query("SELECT COUNT(*) AS n FROM packs WHERE storage_id=%s AND state='ready'",
                                    uuid.UUID(chunked.store['id']))[0]['n'] > 0, 'RAM chunks were not packed')
        wait(release.with_suffix('.hit').exists, 'pack publication did not pause before release')
        assert used() > 0
        stop()
        start()
        release.unlink(missing_ok=True)
        chunked.read('packed', packed)
        assert all(row['quarantine_code'] is None for row in chunks('packed'))
        assert used() == 0
        assert database.query('SELECT reserved_id FROM nonce_counters WHERE block_key_id=%s', uuid.UUID(key['id']))
        print('PASS background encoding publishes an encrypted pack without lost-RAM quarantine', flush=True)

        configure(remote, cdc_enabled=True, write_processing='background', pack_enabled=False,
                  compression_enabled=False)
        uploader = arm(remote, 'raw-upload-before-process')
        conversion = arm(remote, 'cdc-before-read')
        remote.put('cdc', value)
        wait(conversion.with_suffix('.hit').exists, 'CDC did not pause')
        assert raw('cdc')['medium'] == 'memory'
        conversion.unlink(missing_ok=True)
        uploader.unlink(missing_ok=True)
        wait(lambda: database.query('SELECT COUNT(*) AS n FROM objects o JOIN extents e ON e.stream_id=o.stream_id '
                                    'WHERE o.bucket_id=%s AND o.object_key=%s',
                                    uuid.UUID(remote.bucket_id), key_value('cdc'))[0]['n'] > 0,
             'RAM raw was not converted to chunks')
        remote.read('cdc', value)
        wait(lambda: used() == 0, 'background CDC leaked RAM')
        print('PASS background CDC consumes the same bounded RAM source', flush=True)

        configure(remote, cdc_enabled=False, write_processing='inline')
        uploader = arm(remote, 'raw-upload-before-process')
        remote.put('shutdown', small)
        assert raw('shutdown')['medium'] == 'memory'
        uploader.unlink(missing_ok=True)
        stop(crash=False)
        assert raw('shutdown')['durability'] == 'durable'
        memory_size('0B')
        start()
        uploader = arm(remote, 'raw-upload-before-process')
        remote.put('disabled', small)
        assert raw('disabled')['medium'] == 'local' and used() == 0
        remote.read('shutdown', small)
        uploader.unlink(missing_ok=True)
        remote.cli('cache', 'flush', '--wait', '--timeout', '60s')
        print('PASS graceful shutdown drains RAM and zero budget uses buffered files', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
