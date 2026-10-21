"""Cached chunks acknowledge before final compression, encryption and packing."""
import os
import subprocess
import uuid
from pathlib import Path

from botocore.exceptions import ClientError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, Fault, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    cluster = bool(os.environ.get('MOKYU_TEST_PEER_ENDPOINT'))
    log = (faults / 'cached-encoding.log').open('a')
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
            assert process.poll() is None, 'service exited; inspect cached-encoding.log'
            try:
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                      timeout=5).returncode == 0
            except subprocess.TimeoutExpired:
                return False

        wait(ready, 'service did not recover', seconds=60)

    try:
        if not cluster:
            start()
        fixture, database = Fixture(), Database()
        assert fixture.backend, 'requires an isolated S3 backend'
        storage = uuid.UUID(fixture.store['id'])
        bucket = uuid.UUID(fixture.bucket_id)
        roots = [Path(root) for root in os.environ.get('MOKYU_TEST_SOURCE_FAULT_DIRS', str(faults)).split(',')]

        def configure(**changes):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(changes)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def arm():
            paths = [root / 'upload-before-work' for root in roots]
            for path in paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.with_suffix('.hit').unlink(missing_ok=True)
                path.touch()
            marker = Fault(paths)
            markers.append(marker)
            return marker

        def chunks(key):
            return database.query('''SELECT c.id,c.standalone_size,c.standalone_compressed,c.nonce,c.pack_id
                FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id
                WHERE o.bucket_id=%s AND o.object_key=%s ORDER BY e.offset_bytes''', bucket,
                key if database.type == 'postgres' else key.encode())

        def drain():
            wait(lambda: database.query('SELECT COUNT(*) AS n FROM pending_uploads u JOIN chunks c '
                 'ON c.id=u.chunk_id WHERE c.storage_id=%s', storage)[0]['n'] == 0,
                 'cached encodings did not drain', seconds=180)

        key = fixture.cli('key', 'create', fixture.name + '-key', 'block')
        fixture.cli('storage', 'key', 'set', fixture.name, key['name'])
        configure(upload_cache=True, cache_enabled=False, pack_enabled=True, pack_max_size='8MiB',
                  compression_enabled=True, compression_strategy='always', pack_upload_flush_timeout='300s')
        data = b''.join(os.urandom(1024) + b'x' * (63 * 1024) for _ in range(96))
        baseline = set(fixture.keys())
        marker = arm()
        fixture.put('packed', data)
        wait(marker.with_suffix('.hit').exists, 'worker did not pause before final encoding')
        pending = chunks('packed')
        assert len(pending) > 1 and all(row['standalone_size'] is None and row['nonce'] is None for row in pending), pending
        assert set(fixture.keys()) == baseline, 'cache acknowledgement uploaded a physical source'
        counters = database.query('SELECT reserved_id FROM nonce_counters WHERE block_key_id=%s', uuid.UUID(key['id']))
        assert counters and all(row['reserved_id'] == 0 for row in counters), counters
        fixture.read('packed', data)
        with fixture.client.get_object(Bucket=fixture.name, Key='packed', Range='bytes=17-1041')['Body'] as body:
            assert body.read() == data[17:1042]
        fixture.put('duplicate', data)
        assert [row['id'] for row in chunks('duplicate')] == [row['id'] for row in pending]
        replacement = fixture.cli('key', 'create', fixture.name + '-next-key', 'block')
        fixture.cli('storage', 'key', 'set', fixture.name, replacement['name'])
        fixture.cli('key', 'revoke', key['id'], succeeds=False)
        configure(pack_enabled=False, compression_enabled=False, pack_max_size='4MiB',
                  pack_upload_flush_timeout='60s')
        later = b'use the new policy only for new sources\n' * 4096
        fixture.put('later', later)
        if not cluster:
            stop()
            start()
            fixture.read('packed', data)
            fixture.read('duplicate', data)
        print('PASS acknowledged unencoded chunks retain checksums, Range, deduplication and restart reads', flush=True)

        marker.unlink()
        drain()
        fixture.read('packed', data)
        fixture.read('duplicate', data)
        outputs = chunks('packed')
        assert any(row['pack_id'] is not None for row in outputs), outputs
        packs = database.query("SELECT id,pack_compressed FROM packs WHERE storage_id=%s AND state='ready'", storage)
        assert packs and all(row['pack_compressed'] for row in packs), packs
        assert database.query("SELECT COUNT(*) AS n FROM packs WHERE storage_id=%s AND block_key_id=%s AND state='ready'",
                              storage, uuid.UUID(key['id']))[0]['n'] == len(packs)
        later_output = chunks('later')
        assert len(later_output) == 1 and later_output[0]['pack_id'] is None
        assert not later_output[0]['standalone_compressed'], later_output
        assert database.query("SELECT COUNT(*) AS n FROM chunk_encodings WHERE chunk_id=%s AND block_key_id=%s AND state='ready'",
                              later_output[0]['id'], uuid.UUID(replacement['id']))[0]['n'] == 1
        fixture.read('later', later)
        print('PASS accepted encoding survives profile/key changes and restart while new writes use the new policy', flush=True)
        independent = database.query("SELECT encoding_id FROM chunk_encodings WHERE storage_id=%s AND state='ready'", storage)
        assert len(set(fixture.keys()) - baseline) == len(packs) + len(independent)
        counters = database.query('SELECT type_tag,reserved_id FROM nonce_counters WHERE block_key_id=%s', uuid.UUID(key['id']))
        assert any(row['type_tag'] == 1 and row['reserved_id'] > 0 for row in counters), counters
        print('PASS new cached chunks go directly to final packs without intermediate backend PUTs', flush=True)

        configure(pack_enabled=False, compression_enabled=True)
        marker = arm()
        small = b'compress the independent cached chunk\n' * 4096
        fixture.put('independent', small)
        assert chunks('independent')[0]['standalone_size'] is None
        fixture.read('independent', small)
        marker.unlink()
        drain()
        fixture.read('independent', small)
        output = chunks('independent')
        assert len(output) == 1 and output[0]['pack_id'] is None
        assert output[0]['standalone_compressed'] and output[0]['standalone_size'] < len(small), output
        print('PASS independent cached output still applies compression and encryption', flush=True)

        configure(upload_cache_size='1MiB', upload_cache_overflow='reject')
        previous = b'keep the accepted object'
        fixture.put('overflow', previous)
        drain()
        try:
            fixture.put('overflow', b'j' * (2 * 1024 * 1024))
            raise AssertionError('cache overflow did not reject')
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 503, error.response
        fixture.read('overflow', previous)
        configure(upload_cache_overflow='fallback')
        fallback = b'j' * (2 * 1024 * 1024)
        fixture.put('overflow', fallback)
        drain()
        fixture.read('overflow', fallback)
        print('PASS rejected staging preserves the old object and fallback retains final encoding', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        if not cluster:
            stop()
        log.close()


if __name__ == '__main__':
    main()
