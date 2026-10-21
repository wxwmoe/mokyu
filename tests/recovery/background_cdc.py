"""Background CDC preserves the accepted representation until a fenced switch."""
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database, location
from tests.benchmarks.object_io import drain_raw
from tests.storage.source_quarantine import Fixture, Fault, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    cluster = bool(os.environ.get('MOKYU_TEST_PEER_ENDPOINT'))
    log = (faults / 'background-cdc.log').open('a')
    process = None
    markers = []

    def stop():
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect background-cdc.log'
            try:
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                      timeout=5).returncode == 0
            except subprocess.TimeoutExpired:
                return False
        wait(ready, 'service did not recover', seconds=60)

    try:
        if not cluster:
            start()
        backend = os.environ.pop('MOKYU_TEST_BACKEND', None) if os.environ.get('MOKYU_TEST_CDC_LOCAL') else None
        fixture, database = Fixture(), Database()
        if backend:
            os.environ['MOKYU_TEST_BACKEND'] = backend
        storage, bucket = uuid.UUID(fixture.store['id']), uuid.UUID(fixture.bucket_id)
        roots = [Path(root) for root in os.environ.get('MOKYU_TEST_SOURCE_FAULT_DIRS', str(faults)).split(',')]

        def arm(name):
            paths = [root / name for root in roots]
            for path in paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.with_suffix('.hit').unlink(missing_ok=True)
                path.touch()
            marker = Fault(paths)
            markers.append(marker)
            return marker

        def configure(**changes):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(changes)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def object_row(key):
            rows = database.query('''SELECT s.id,s.payload_type,s.payload_revision,s.etag,s.metadata,
                s.public_read,s.created_at,s.touched_at FROM objects o JOIN streams s ON s.id=o.stream_id
                WHERE o.bucket_id=%s AND o.object_key=%s''', bucket,
                key if database.type == 'postgres' else key.encode())
            return rows[0] if rows else None

        def source_row(key):
            return database.query(f'''SELECT r.id,r.{database.raw_location} AS file_uuid,r.processing_task
                FROM raw_ranges e JOIN raw_sources r ON r.id=e.source_id WHERE e.stream_id=%s''',
                object_row(key)['id'])[0]

        def converted(key):
            def complete():
                row = object_row(key)
                if row and row['payload_type'] == 'chunks':
                    return row
                failures = database.query("SELECT error FROM tasks WHERE bucket_id=%s AND state='failed'", bucket)
                assert not failures, failures
                return None
            return wait(complete, 'background CDC did not finish', seconds=180)

        def drain():
            wait(lambda: not database.query('SELECT u.source_id FROM raw_uploads u JOIN raw_sources r '
                 'ON r.id=u.source_id WHERE r.storage_id=%s', storage), 'raw cache did not release', seconds=180)

        remote = bool(fixture.backend)
        key = fixture.cli('key', 'create', fixture.name + '-key', 'block')
        fixture.cli('storage', 'key', 'set', fixture.name, key['name'])
        configure(cdc_enabled=True, write_processing='background', upload_cache=remote,
                  pack_enabled=remote, pack_max_size='8MiB', compression_enabled=True,
                  compression_strategy='always', pack_upload_flush_timeout='300s')
        data = b''.join(os.urandom(1024) + b'x' * (63 * 1024) for _ in range(96))
        baseline = set(fixture.keys())
        marker = arm('cdc-before-read')
        reply = fixture.put('original', data)
        wait(marker.with_suffix('.hit').exists, 'CDC did not pause')
        before = object_row('original')
        assert before['payload_type'] == 'raw'
        assert not drain_raw(database, fixture.name, 0)['completed']
        assert not database.chunks(fixture)
        if remote:
            assert set(fixture.keys()) == baseline, 'accepted local cache uploaded the raw file'
        fixture.read('original', data)
        assert fixture.client.head_object(Bucket=fixture.name, Key='original')['ETag'] == reply['ETag']
        with fixture.client.get_object(Bucket=fixture.name, Key='original', Range='bytes=17-1041')['Body'] as body:
            assert body.read() == data[17:1042]
        replacement = fixture.cli('key', 'create', fixture.name + '-next-key', 'block')
        fixture.cli('storage', 'key', 'set', fixture.name, replacement['name'])
        configure(pack_enabled=False, compression_enabled=False)
        fixture.cli('key', 'revoke', key['id'], succeeds=False)
        if not cluster:
            stop()
            start()
            fixture.read('original', data)
        marker.unlink()
        after = converted('original')
        for name in ('id', 'etag', 'metadata', 'public_read', 'created_at', 'touched_at'):
            assert after[name] == before[name], name
        assert after['payload_revision'] == before['payload_revision'] + 1
        fixture.read('original', data)
        drain()
        assert drain_raw(database, fixture.name, 30)['completed']
        if remote:
            packs = database.query("SELECT id,block_key_id,pack_compressed FROM packs WHERE storage_id=%s AND state='ready'", storage)
            assert packs and all(row['pack_compressed'] for row in packs), packs
            assert len(set(fixture.keys()) - baseline) == len(packs), 'conversion uploaded intermediate raw/chunk files'
            assert database.query("SELECT COUNT(*) AS n FROM packs WHERE storage_id=%s AND block_key_id=%s AND state='ready'",
                                  storage, uuid.UUID(key['id']))[0]['n'] == len(packs)
        print('PASS acknowledged raw survives restart and converts with the accepted key/policy without duplicate uploads', flush=True)

        keys = set(fixture.keys())
        ids = {row['id'] for row in database.chunks(fixture)}
        configure(write_processing='inline')
        fixture.put('duplicate', data)
        converted('duplicate')
        fixture.read('duplicate', data)
        drain()
        assert {row['id'] for row in database.chunks(fixture)} == ids
        if remote:
            assert set(fixture.keys()) == keys
        configure(write_processing='background')
        print('PASS inline and background CDC produce the same boundaries and reuse healthy chunks', flush=True)

        marker = arm('cdc-batch-stored')
        value = os.urandom(1024 * 1024)
        fixture.put('contended', value)
        wait(marker.with_suffix('.hit').exists, 'active allocation did not pause')
        active = {row['id'] for row in database.query(
            "SELECT id FROM chunks WHERE storage_id=%s AND state='preparing'", storage)}
        assert active
        configure(write_processing='inline')
        try:
            fixture.put('contender', value)
            raise AssertionError('a foreground write reused an unpublished allocation')
        except ClientError as error:
            assert error.response['Error']['Code'] == 'OperationAborted', error.response
        assert {row['id'] for row in database.query(
            "SELECT id FROM chunks WHERE storage_id=%s AND state='preparing'", storage)} == active
        marker.unlink()
        converted('contended')
        fixture.put('contender', value)
        fixture.read('contender', value)
        configure(write_processing='background')
        drain()
        print('PASS concurrent writes cannot invalidate an active CDC allocation and can retry after publication', flush=True)

        marker = arm('cdc-before-switch')
        value = os.urandom(1024 * 1024)
        fixture.put('pinned', value)
        wait(marker.with_suffix('.hit').exists, 'conversion did not reach switch')
        reader = arm('read-pinned')
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(fixture.read, 'pinned', value)
            wait(reader.with_suffix('.hit').exists, 'old reader did not pin raw representation')
            marker.unlink()
            time.sleep(.8)
            assert object_row('pinned')['payload_type'] == 'raw'
            reader.unlink()
            pending.result(timeout=90)
        converted('pinned')
        fixture.read('pinned', value)
        drain()
        print('PASS existing readers retain the raw representation through the atomic switch', flush=True)

        marker = arm('cdc-before-switch')
        fixture.put('overwritten', b'old conversion' * 4096)
        wait(marker.with_suffix('.hit').exists, 'overwrite fixture did not pause')
        configure(write_processing='inline')
        fixture.put('overwritten', b'new object')
        marker.unlink()
        fixture.read('overwritten', b'new object')
        drain()
        assert object_row('overwritten')['payload_type'] == 'chunks'
        print('PASS a stale conversion cannot replace a newer object', flush=True)

        configure(write_processing='background')
        marker = arm('cdc-before-read')
        fixture.put('deleted', b'cancel this conversion' * 4096)
        wait(marker.with_suffix('.hit').exists, 'delete fixture did not pause')
        original = source_row('deleted')
        fixture.client.delete_object(Bucket=fixture.name, Key='deleted')
        marker.unlink()
        wait(lambda: database.query('SELECT state FROM tasks WHERE id=%s', original['processing_task'])[0]['state'] == 'completed',
             'deleted conversion did not cancel')
        drain()
        fixture.put('empty', b'')
        converted('empty')
        fixture.read('empty', b'')
        drain()
        print('PASS delete cancels accepted work and empty objects convert without physical chunks', flush=True)

        if not cluster:
            marker = arm('cdc-batch-stored')
            value = os.urandom(1024 * 1024)
            fixture.put('interrupted', value)
            wait(marker.with_suffix('.hit').exists, 'stored candidate did not pause')
            fixture.read('interrupted', value)
            stop()
            marker.unlink()
            start()
            converted('interrupted')
            fixture.read('interrupted', value)
            drain()
            print('PASS restart fences an interrupted candidate after physical upload', flush=True)

            for checkpoint in ('cdc-switch-before-commit', 'cdc-after-switch'):
                marker = arm(checkpoint)
                value = os.urandom(512 * 1024)
                fixture.put(checkpoint, value)
                wait(marker.with_suffix('.hit').exists, 'conversion switch did not pause')
                stop()
                marker.unlink()
                start()
                converted(checkpoint)
                fixture.read(checkpoint, value)
                drain()
            print('PASS restart before and after the atomic switch preserves a complete representation', flush=True)

        configure(upload_cache=False)
        marker = arm('cdc-before-read')
        value = os.urandom(1024 * 1024)
        fixture.put('corrupt', value)
        wait(marker.with_suffix('.hit').exists, 'bad source did not pause')
        original = source_row('corrupt')
        ident = original['file_uuid']
        ident = ident if isinstance(ident, uuid.UUID) else uuid.UUID(bytes=bytes(ident))
        relative = 'raw/' + ident.hex[-2:] + '/' + ident.hex
        path = fixture.prefix + relative if remote else str(fixture.root / relative)
        fixture.bytes(path, b'!' + value[1:])
        marker.unlink()
        wait(lambda: database.query('SELECT error FROM tasks WHERE id=%s', original['processing_task'])[0]['error'],
             'bad source did not retain a diagnostic')
        assert object_row('corrupt')['payload_type'] == 'raw'
        fixture.bytes(path, value)
        task = original['processing_task']
        task = task if isinstance(task, uuid.UUID) else uuid.UUID(bytes=bytes(task))
        fixture.cli('task', 'resume', str(task))
        converted('corrupt')
        fixture.read('corrupt', value)
        print('PASS a corrupt raw source cannot publish a candidate and repaired bytes can resume', flush=True)

        if cluster:
            marker = arm('cdc-before-switch')
            value = os.urandom(1024 * 1024)
            fixture.put('fenced', value)
            wait(marker.with_suffix('.hit').exists, 'leased conversion did not pause')
            before = object_row('fenced')
            task = source_row('fenced')['processing_task']
            token = database.query('SELECT lease_token FROM tasks WHERE id=%s', task)[0]['lease_token']
            owner = next(path for path in marker.paths if path.with_suffix('.hit').exists())
            database.query("UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second' "
                           "WHERE stream_id=%s AND lease_type='read'", before['id'])
            database.query("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", task)
            for path in marker.paths:
                if path != owner:
                    path.unlink()
            after = converted('fenced')
            assert after['id'] == before['id'] and after['payload_revision'] == before['payload_revision'] + 1
            assert database.query('SELECT lease_token FROM tasks WHERE id=%s', task)[0]['lease_token'] != token
            owner.unlink()
            fixture.read('fenced', value)
            assert object_row('fenced')['payload_revision'] == after['payload_revision']
            print('PASS a peer resumes an expired conversion and the late owner cannot publish twice', flush=True)

        if remote:
            configure(upload_cache=True)
            marker = arm('cdc-before-read')
            value = b'flush the accepted source independently of conversion' * 4096
            fixture.put('flush', value)
            wait(marker.with_suffix('.hit').exists, 'flush fixture did not pause')
            original = source_row('flush')
            task = original['processing_task']
            task = task if isinstance(task, uuid.UUID) else uuid.UUID(bytes=bytes(task))
            fixture.cli('task', 'pause', str(task))
            fixture.finished(fixture.cli('cache', 'flush'))
            assert not database.query('SELECT pending_upload FROM raw_sources WHERE id=%s', original['id'])[0]['pending_upload']
            fixture.read('flush', value)
            marker.unlink()
            wait(lambda: not database.query("SELECT id FROM streams WHERE owner_task_id=%s AND state='writing'", task),
                 'paused candidate did not stop')
            fixture.cli('task', 'resume', str(task))
            converted('flush')
            fixture.read('flush', value)
            drain()
            print('PASS explicit cache flush persists raw while conversion is paused and conversion can resume', flush=True)

            configure(upload_cache_size='1MiB', upload_cache_overflow='fallback', compression_enabled=True)
            keys = set(fixture.keys())
            value = os.urandom(1024) + b'z' * (2 * 1024 * 1024 - 1024)
            marker = arm('cdc-inline-fallback')
            with ThreadPoolExecutor(max_workers=1) as pool:
                write = pool.submit(fixture.put, 'fallback', value)
                wait(marker.with_suffix('.hit').exists, 'CDC fallback did not pause')
                next_key = fixture.cli('key', 'create', fixture.name + '-fallback-key', 'block')
                fixture.cli('storage', 'key', 'set', fixture.name, next_key['name'])
                configure(compression_enabled=False)
                marker.unlink()
                write.result(timeout=60)
            assert object_row('fallback')['payload_type'] == 'chunks'
            encodings = database.query('SELECT c.block_key_id,c.standalone_compressed FROM extents e JOIN chunks c '
                                       'ON c.id=e.chunk_id WHERE e.stream_id=%s', object_row('fallback')['id'])
            accepted_key = uuid.UUID(replacement['id'])
            assert all(row['block_key_id'] == (accepted_key if database.type == 'postgres' else accepted_key.bytes)
                       and row['standalone_compressed'] for row in encodings), encodings
            assert not database.query('SELECT chunk_id FROM pending_uploads WHERE stream_id=%s', object_row('fallback')['id'])
            assert not any('/raw/' in path for path in set(fixture.keys()) - keys)
            fixture.read('fallback', value)
            broken = database.query('SELECT c.id,c.current_encoding_id FROM extents e JOIN chunks c '
                                    'ON c.id=e.chunk_id WHERE e.stream_id=%s ORDER BY e.offset_bytes',
                                    object_row('fallback')['id'])[0]
            physical = database.query('SELECT location_id FROM chunk_encodings WHERE encoding_id=%s',
                                      broken['current_encoding_id'])[0]['location_id']
            path = location(fixture, physical)
            encoded = fixture.bytes(path)
            fixture.bytes(path, bytes([encoded[0] ^ 1]) + encoded[1:])
            scope = dict(storage_id=fixture.store['id'], source='chunk', id=str(broken['current_encoding_id']))
            fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
            configure(compression_enabled=True)
            marker = arm('cdc-inline-fallback')
            with ThreadPoolExecutor(max_workers=1) as pool:
                write = pool.submit(fixture.put, 'repaired-fallback', value)
                wait(marker.with_suffix('.hit').exists, 'repair fallback did not pause')
                configure(compression_enabled=False)
                marker.unlink()
                write.result(timeout=60)
            repaired = database.query('SELECT block_key_id,standalone_compressed FROM chunks WHERE id=%s',
                                      broken['id'])[0]
            accepted_key = uuid.UUID(next_key['id'])
            assert repaired['standalone_compressed'] and repaired['block_key_id'] == (
                accepted_key if database.type == 'postgres' else accepted_key.bytes), repaired
            fixture.read('repaired-fallback', value)
            fixture.read('fallback', value)
            print('PASS synchronous fallback repairs quarantined chunks with the accepted encoding policy', flush=True)
            configure(upload_cache_overflow='reject')
            try:
                fixture.put('fallback', os.urandom(2 * 1024 * 1024))
                raise AssertionError('full raw cache ignored reject policy')
            except ClientError as error:
                assert error.response['Error']['Code'] == 'SlowDown', error.response
                assert error.response['ResponseMetadata']['HTTPStatusCode'] == 503
            fixture.read('fallback', value)
            configure(upload_cache_size=None, upload_cache_overflow='fallback')
            print('PASS CDC cache overflow falls back to synchronous chunks without a raw upload or rejects without replacing data', flush=True)

        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='multipart')['UploadId']
        parts = []
        values = [os.urandom(5 * 1024 * 1024), b'last multipart bytes']
        for number, value in enumerate(values, 1):
            result = fixture.client.upload_part(Bucket=fixture.name, Key='multipart', UploadId=upload,
                PartNumber=number, Body=value)
            parts.append({'PartNumber': number, 'ETag': result['ETag']})
        completion_started = time.monotonic()
        fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=upload,
            MultipartUpload={'Parts': parts})
        assert time.monotonic() - completion_started < 60, 'Complete waited for the normal cache flush interval'
        fixture.read('multipart', b''.join(values))
        assert object_row('multipart')['payload_type'] == 'chunks'
        print('PASS background profiles retain streaming CDC Multipart semantics', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        if not cluster:
            stop()
        log.close()


if __name__ == '__main__':
    main()
