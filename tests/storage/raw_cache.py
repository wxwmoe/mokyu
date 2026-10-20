"""Raw file cache, shared eviction, source inspection and restart recovery.

Requires a fresh isolated service configuration, 9 MiB disk cache, disabled
response cache, and a disposable S3 backend. This script owns the service process.
"""
import os
import stat
import subprocess
import tomllib
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    config = tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())
    data_path = Path(config['instance']['data_path'])
    assert config['instance']['cache_size'] == '9MiB'
    assert config['instance']['response_cache_size'] == '0B'
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'raw-cache.log').open('a')
    database = Database()
    process = None

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect raw-cache.log'
            return subprocess.run(command + ['cli', 'status', '--ready'],
                                  capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not start', seconds=60)

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    try:
        start()
        fixture = Fixture()
        assert fixture.backend, 'select an isolated S3 backend'

        def configure(**values):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(values)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        def status():
            return fixture.cli('status')

        def idle():
            wait(lambda: status()['cache_jobs_active'] == 0, 'cache work did not drain')

        def files():
            return [path for path in (data_path / 'raw').glob('*/*') if path.is_file()]

        def source(key):
            rows = database.query('''SELECT r.id FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id
                JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s''',
                uuid.UUID(fixture.bucket_id), key if database.type == 'postgres' else key.encode())
            assert len(rows) == 1, rows
            value = rows[0]['id']
            return value if isinstance(value, uuid.UUID) else uuid.UUID(bytes=bytes(value))

        def cache_path(identity):
            return data_path / 'raw' / identity.hex[:2] / identity.hex

        configure(cdc_enabled=False, pack_range_enabled=False, deduplication_enabled=False, cache_enabled=True)
        data = bytes(range(256)) * (6 * 1024 * 1024 // 256)
        fixture.put('original', data)
        assert not files(), 'raw PUT unexpectedly filled a read cache'
        before = status()
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda _: fixture.read('original', data), range(2)))
        after = status()
        assert after['backend_gets'] - before['backend_gets'] == 2, (before, after)
        assert after['backend_read_bytes'] - before['backend_read_bytes'] == len(data)
        assert len(files()) == 1
        cached = files()[0]
        original_source = source('original')
        assert cached == cache_path(original_source)
        assert cached.suffix == '' and stat.S_IMODE(cached.stat().st_mode) == 0o644
        assert cached.read_bytes() == data and after['local_bytes'][1] == len(data)
        before = status()
        with fixture.client.get_object(Bucket=fixture.name, Key='original', Range='bytes=17-73')['Body'] as body:
            assert body.read() == data[17:74]
        fixture.client.head_object(Bucket=fixture.name, Key='original')
        after = status()
        assert after['backend_gets'] == before['backend_gets']
        assert after['cache_hit_bytes'] - before['cache_hit_bytes'] == 57
        print('PASS a shared cold read downloads once; raw objects larger than a chunk share the file cache and Range', flush=True)

        fixture.client.put_object_acl(Bucket=fixture.name, Key='original', ACL='public-read')
        response = fixture.session.put(fixture.web + '/api/v1/buckets/' + fixture.bucket_id + '/cors',
            headers=fixture.headers, json=[{'origins': ['https://cache.test'], 'methods': ['GET', 'HEAD']}], timeout=30)
        assert response.status_code == 200, response.text
        alias = fixture.name + '.test'
        fixture.cli('domain', 'set', alias, fixture.name)
        public = os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/original'
        web = os.environ['MOKYU_TEST_PUBLIC_WEB'] + '/original'
        before = status()
        for url, headers in [(public, {}), (web, {'Host': alias})]:
            response = requests.get(url, headers={**headers, 'Accept-Encoding': 'identity',
                'Origin': 'https://cache.test'}, timeout=30)
            assert response.status_code == 200 and response.content == data
            assert response.headers['Access-Control-Allow-Origin'] == 'https://cache.test'
        assert status()['backend_gets'] == before['backend_gets']
        fixture.client.put_object_acl(Bucket=fixture.name, Key='original', ACL='private')
        for url, headers in [(public, {}), (web, {'Host': alias})]:
            assert requests.get(url, headers=headers, timeout=30).status_code == 403
        print('PASS anonymous S3 and public Web reuse raw files while retaining current ACL and CORS decisions', flush=True)

        # Same-size corruption must not be mistaken for a valid immutable cache.
        with cached.open('r+b') as file:
            file.write(b'!')
        before = status()
        fixture.read('original', data)
        after = status()
        assert after['backend_gets'] - before['backend_gets'] == 2
        assert after['io']['diagnostics']['cache']['invalid'] == before['io']['diagnostics']['cache']['invalid'] + 1
        assert after['io']['diagnostics']['content'] == before['io']['diagnostics']['content']
        assert cached.read_bytes() == data and after['local_bytes'][1] == len(data)
        cached.unlink()
        fixture.read('original', data)
        assert cached.read_bytes() == data and status()['local_bytes'][1] == len(data)
        print('PASS corrupt and missing cache files fall back without quarantining a healthy source', flush=True)

        # A pinned 6 MiB file cannot be evicted to fit a second one into 9 MiB.
        other = b'other' * (len(data) // 5 + 1)
        fixture.put('other', other)
        marker = fixture.arm('raw-cache-hit')
        with ThreadPoolExecutor(max_workers=2) as pool:
            reading = pool.submit(fixture.read, 'original', data)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or reading.done(), 'cached reader did not pause')
                assert not reading.done(), reading.exception()
                before = status()
                fixture.read('other', other)
                assert cached.exists(), 'a pinned file was evicted'
                assert status()['local_bytes'][1] == len(data)
                assert status()['backend_read_bytes'] - before['backend_read_bytes'] == 2 * len(other)
            finally:
                marker.unlink(missing_ok=True)
            reading.result(timeout=30)
        fixture.read('other', other)
        assert not cached.exists() and status()['local_bytes'][1] == len(other)
        fixture.read('original', data)
        assert cached.exists()
        print('PASS raw cache eviction shares the disk budget and keeps active readers valid', flush=True)

        # A healthy cached copy must not hide a corrupt backend from a full recheck.
        identity = source('original')
        physical = fixture.prefix + 'raw/' + identity.hex[:2] + '/' + identity.hex
        fixture.bytes(physical, b'!' + data[1:])
        task = fixture.finished(fixture.cli('integrity', 'check', '--mode', 'full',
                                           '--bucket', fixture.name, '--key', 'original'))
        issues = fixture.api('/api/v1/tasks/' + task['id'] + '/issues')['items']
        assert any(issue['code'] == 'hash_mismatch' for issue in issues), issues
        scope = dict(storage_id=fixture.store['id'], source='raw', id=str(identity))
        fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
        rejected(lambda: fixture.client.get_object(Bucket=fixture.name, Key='original'))
        fixture.recheck(scope, state='failed')
        fixture.bytes(physical, data)
        fixture.recheck(scope)
        before = status()
        fixture.read('original', data)
        assert status()['backend_gets'] == before['backend_gets']
        assert cached == cache_path(identity) and cached.read_bytes() == data
        print('PASS full inspection bypasses cache; quarantine blocks reads and recovery reuses verified immutable bytes', flush=True)

        idle()
        stop(crash=True)
        orphan = data_path / 'raw' / 'aa' / ('aa' + '0' * 30 + '.tmp')
        orphan.parent.mkdir(exist_ok=True)
        orphan.write_bytes(b'incomplete')
        start()
        assert not orphan.exists()
        before = status()
        fixture.read('original', data)
        assert status()['backend_gets'] == before['backend_gets']
        assert status()['local_bytes'][1] == sum(path.stat().st_size for path in files())
        print('PASS restart restores whole raw files above the chunk bound and removes incomplete fills', flush=True)

        changed = b'replacement' * 4096
        fixture.put('original', changed)
        before = status()
        fixture.read('original', changed)
        assert status()['backend_gets'] - before['backend_gets'] == 2
        replacement = source('original')
        assert replacement != original_source and cache_path(replacement).read_bytes() == changed
        fixture.client.copy_object(Bucket=fixture.name, Key='copy',
                                   CopySource={'Bucket': fixture.name, 'Key': 'original'})
        fixture.read('copy', changed)
        copied = source('copy')
        assert copied != replacement and cache_path(copied).read_bytes() == changed
        print('PASS overwrite and CopyObject follow the current source rather than a logical-key cache', flush=True)

        configure(deduplication_enabled=True)
        shared = b'immutable raw source shared across logical references' * 1024
        fixture.put('shared-a', shared)
        fixture.read('shared-a', shared)
        canonical = source('shared-a')
        shared_file = cache_path(canonical)
        inode = shared_file.stat().st_ino
        before = status()
        fixture.put('shared-b', shared)
        assert source('shared-b') == canonical
        fixture.read('shared-b', shared)
        assert status()['backend_gets'] == before['backend_gets']
        assert shared_file.stat().st_ino == inode
        configure(deduplication_enabled=False)
        fixture.read('shared-a', shared)
        assert status()['backend_gets'] == before['backend_gets']
        assert shared_file.stat().st_ino == inode
        print('PASS deduplicated references and profile revisions reuse the backend UUID and the same cached inode', flush=True)

        fixture.put('empty', b'')
        fixture.read('empty', b'')
        assert any(path.stat().st_size == 0 for path in files())
        interrupted = b'interrupted cache fill' * 8192
        fixture.put('interrupted', interrupted)
        marker = fixture.arm('raw-cache-written')
        with ThreadPoolExecutor(max_workers=1) as pool:
            reading = pool.submit(fixture.read, 'interrupted', interrupted)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or reading.done(),
                     'cache publication did not pause')
                assert not reading.done(), reading.exception()
                stop(crash=True)
                assert list((data_path / 'raw').glob('*/*.tmp'))
            finally:
                marker.unlink(missing_ok=True)
            try:
                reading.result(timeout=30)
            except Exception:
                pass
            else:
                raise AssertionError('unpublished cache read succeeded after service death')
        start()
        assert not list((data_path / 'raw').glob('*/*.tmp'))
        fixture.read('interrupted', interrupted)
        fixture.read('empty', b'')
        configure(cache_enabled=False)
        before = status()
        fixture.read('original', changed)
        assert status()['backend_gets'] - before['backend_gets'] == 3
        configure(cache_enabled=True)
        fixture.cli('storage', 'create', fixture.name + '-overlap', 'local', '--init',
                    '--path', str(data_path / 'raw'), succeeds=False)
        assert not (data_path / 'raw' / 'meta.json').exists()
        print('PASS empty files, killed fills, disabled cache and overlapping storage roots remain safe', flush=True)

        # Optional chunks read-cache publication must not delay a durable PUT.
        configure(cdc_enabled=True, deduplication_enabled=True)
        marker = fixture.arm('cache-written')
        try:
            fixture.put('chunk', b'cache publication is optional' * 1000)
            wait(lambda: marker.with_suffix('.hit').exists(), 'optional chunk fill did not pause')
            fixture.read('chunk', b'cache publication is optional' * 1000)
        finally:
            marker.unlink(missing_ok=True)
        idle()
        assert status()['local_bytes'][1] == sum(path.stat().st_size
            for folder in ('raw', 'chunks') for path in (data_path / folder).glob('*/*') if path.is_file())
        print('PASS durable chunks PUT completes while optional cache publication is paused', flush=True)

        configure(cdc_enabled=False, write_processing='background')
        marker = fixture.arm('raw-processing-before-hash')
        pending = b'cached bytes cannot certify a broken backend' * 1000
        try:
            fixture.put('background', pending)
            wait(lambda: marker.with_suffix('.hit').exists(), 'background work did not pause')
            fixture.read('background', pending)
            identity = source('background')
            physical = fixture.prefix + 'raw/' + identity.hex[:2] + '/' + identity.hex
            fixture.bytes(physical, b'!' + pending[1:])
        finally:
            marker.unlink(missing_ok=True)
        wait(lambda: database.query('SELECT last_error FROM raw_processing WHERE source_id=%s',
                                    identity)[0]['last_error'],
             'background hashing trusted cached bytes instead of the backend')
        assert database.query('SELECT content_hash FROM raw_sources WHERE id=%s', identity)[0]['content_hash'] is None
        fixture.client.delete_object(Bucket=fixture.name, Key='background')
        print('PASS background deduplication inspects the backend even when a healthy file is cached', flush=True)
    finally:
        for path in faults.glob('*'):
            if path.is_file() and path.suffix not in ('.hit', '.log'):
                path.unlink()
        stop()
        log.close()


if __name__ == '__main__':
    main()
