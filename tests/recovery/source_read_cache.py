"""Source snapshot expiry, physical pins and strong maintenance through real HTTP."""
import os
import re
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    config = Path(os.environ['MOKYU_TEST_CONFIG'])
    config.write_text(re.sub(r'^response_cache_size = .*$', 'response_cache_size = "0B"', config.read_text(), flags=re.M))
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    path = faults / 'source-read-cache.log'
    markers = []
    with path.open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log,
            env=dict(os.environ, RUST_LOG='mokyu=info,mokyu::status::stats=debug,sqlx::query=debug', NO_COLOR='1'))
        try:
            def ready():
                assert process.poll() is None, 'service exited; inspect source-read-cache.log'
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
            wait(ready, 'service did not start', seconds=60)
            fixture, database = Fixture(), Database()

            def policy(**values):
                profile = fixture.cli('storage', 'profile', 'show', fixture.name)
                profile.update(values)
                return fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

            def read(key='raw', **headers):
                return requests.get(os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/' + key,
                    headers={'Accept-Encoding': 'identity', **headers}, timeout=30)

            def count(response):
                request_id = response.headers['x-request-id']
                def trace():
                    return [line for line in path.read_text().splitlines() if request_id in line]
                wait(lambda: any('request finished' in line for line in trace()), 'request did not finish')
                return len([line for line in trace() if 'rows_returned=' in line])

            def unavailable(key):
                try:
                    assert read(key).status_code >= 400, 'unavailable source was returned'
                except requests.exceptions.ChunkedEncodingError:
                    pass

            def source_stats():
                def find(value):
                    if isinstance(value, dict):
                        if 'source_cache' in value:
                            return value['source_cache']
                        for child in value.values():
                            if result := find(child):
                                return result
                    return None
                return find(fixture.cli('status'))

            defaults = fixture.cli('storage', 'profile', 'show', fixture.name)
            assert defaults['read_source_validation'] == 'strict' and defaults['source_validation_max_age'] == '5s'
            for values in ({'read_source_validation': 'off'}, {'source_validation_max_age': '0s'}, {'source_validation_max_age': '301s'}):
                response = fixture.session.put(fixture.web + '/api/v1/storages/' + fixture.name + '/profile',
                    headers=fixture.headers, json={**defaults, **values}, timeout=15)
                assert response.status_code == 400, response.text
            policy(cdc_enabled=False, pack_range_enabled=False, public_cache_validation='background',
                   public_cache_refresh_interval='30s', public_cache_max_age='30s',
                   read_source_validation='background', source_validation_max_age='30s')
            body = bytes(range(256)) * 128
            fixture.client.put_object(Bucket=fixture.name, Key='raw', Body=body, ACL='public-read')
            assert read().content == body
            response = read()
            assert response.content == body and count(response) == 0
            response = read(Range='bytes=17-234')
            assert response.status_code == 206 and response.content == body[17:235] and count(response) == 0
            assert count(read(**{'Cache-Control': 'no-cache'})) >= 1
            signed = fixture.client.generate_presigned_url('get_object', Params={'Bucket': fixture.name, 'Key': 'raw'})
            response = requests.get(signed, timeout=15)
            assert response.content == body and count(response) >= 1
            assert fixture.cli('status')['response_cache']['live_bytes'] == 0
            print('PASS raw metadata-only reads and Range use zero SQL with explicit source snapshots; signed and no-cache remain fresh', flush=True)

            policy(source_validation_max_age='1s')
            assert read().content == body
            assert count(read()) == 0
            time.sleep(1.1)
            response = read()
            assert response.content == body and count(response) >= 1, 'source expiry renewed on access'
            policy(read_source_validation='strict')
            assert read().content == body
            assert count(read()) >= 1
            print('PASS source hard expiry and strict profile switch independently restore authoritative selection', flush=True)

            policy(read_source_validation='background', source_validation_max_age='30s')
            assert read().content == body and read().content == body
            raw, = database.query('SELECT id FROM raw_sources WHERE storage_id=%s', uuid.UUID(fixture.store['id']))
            scope = dict(storage_id=fixture.store['id'], source='raw', id=str(raw['id']))
            physical, = fixture.keys()
            original = fixture.bytes(physical)
            fixture.bytes(physical, bytes([original[0] ^ 1]) + original[1:])
            preview = fixture.preview('quarantine', scope)
            fixture.finished(fixture.execute('quarantine', preview))
            fixture.bytes(physical, original)
            assert read().status_code >= 400, 'quarantine left a cached physical source usable'
            fixture.recheck(scope)
            assert read().content == body
            fixture.client.put_object_acl(Bucket=fixture.name, Key='raw', ACL='private')
            assert read().status_code == 403
            fixture.client.put_object(Bucket=fixture.name, Key='raw', Body=body[::-1], ACL='public-read')
            assert read().content == body[::-1]
            fixture.client.delete_object(Bucket=fixture.name, Key='raw')
            assert read().status_code == 404
            print('PASS raw quarantine, recheck, ACL, overwrite and delete fence cached source selection', flush=True)

            policy(unreferenced_grace='1s', min_storage_duration='0s', upload_retention='1s')
            fixture.client.put_object(Bucket=fixture.name, Key='pinned', Body=body, ACL='public-read')
            assert read('pinned').content == body
            response = read('pinned')
            assert response.content == body and count(response) == 0
            row, = database.query(f'''SELECT r.{database.raw_location} AS file_uuid FROM objects o
                JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
                WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(fixture.bucket_id),
                'pinned' if database.type == 'postgres' else b'pinned')
            identity = row['file_uuid']
            identity = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
            pinned_path = (fixture.prefix + 'raw/' + identity.hex[-2:] + '/' + identity.hex if fixture.backend
                           else str(fixture.root / 'raw' / identity.hex[-2:] / identity.hex))
            marker = fixture.arm('read-pinned')
            markers.append(marker)
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(read, 'pinned')
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or pending.done(), 'cached reader did not acquire pins')
                    assert not pending.done()
                    fixture.client.put_object(Bucket=fixture.name, Key='pinned', Body=body[::-1], ACL='public-read')
                    time.sleep(1.1)
                    fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
                    assert pinned_path in fixture.keys(), 'cached metadata bypassed the pin before file open'
                finally:
                    marker.unlink(missing_ok=True)
                assert pending.result(timeout=30).content == body
            assert read('pinned').content == body[::-1]
            def reclaimed():
                fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
                return pinned_path not in fixture.keys()
            wait(reclaimed, 'cached physical pin outlived its reader', seconds=45)
            print('PASS zero-SQL raw selection pins the file before open and releases it for GC after overwrite', flush=True)

            policy(cdc_enabled=True, public_cache_validation='strict', source_validation_max_age='3s')
            fixture.client.put_object(Bucket=fixture.name, Key='chunks', Body=body, ACL='public-read')
            assert read('chunks').content == body
            before = source_stats()
            assert read('chunks').content == body
            assert source_stats()['hits'] > before['hits']
            marker = fixture.arm('source-cache-refresh')
            markers.append(marker)
            time.sleep(1.6)
            assert read('chunks').content == body
            wait(marker.with_suffix('.hit').exists, 'source refresh did not start')
            chunk, = database.chunks(fixture)
            database.query("UPDATE chunk_encodings SET quarantine_code='hash_mismatch',quarantined_at=created_at WHERE encoding_id=%s", chunk['current_encoding_id'])
            assert read('chunks').content == body, 'did not exercise the explicitly bounded source window'
            time.sleep(1.6)
            unavailable('chunks')
            marker.unlink(missing_ok=True)
            wait(lambda: source_stats()['failed'] > 0, 'failed refresh not reported')
            database.query('UPDATE chunk_encodings SET quarantine_code=NULL,quarantined_at=NULL WHERE encoding_id=%s', chunk['current_encoding_id'])
            policy(read_source_validation='strict')
            assert read('chunks').content == body
            previous = source_stats()['hits']
            assert read('chunks').content == body
            assert source_stats()['hits'] == previous
            print('PASS chunks cache bounded health snapshots, failed refresh cannot extend expiry, strict mode bypasses cache', flush=True)
            if fixture.backend:
                policy(pack_enabled=True, pack_interval='24h', pack_repack_interval='24h')
                packed = os.urandom(9 * 1024 * 1024 + 17)
                fixture.client.put_object(Bucket=fixture.name, Key='packed', Body=packed, ACL='public-read')
                fixture.finished(fixture.cli('pack', 'run'))
                packs = fixture.api('/api/v1/storage/packs?bucket=' + fixture.bucket_id + '&state=ready')['items']
                assert packs, 'expected a real remote pack'
                policy(read_source_validation='background', source_validation_max_age='30s')
                assert read('packed').content == packed
                hits = source_stats()['hits']
                assert read('packed').content == packed
                assert source_stats()['hits'] > hits
                detail = fixture.api('/api/v1/packs/' + packs[0]['id'])
                identity = detail['pack']['id' if database.type == 'postgres' else 'location_id'].replace('-', '')
                physical = fixture.prefix + 'packs/' + identity[-2:] + '/' + identity
                original = fixture.bytes(physical)
                fixture.bytes(physical, bytes([original[0] ^ 1]) + original[1:])
                scope = dict(storage_id=fixture.store['id'], source='pack', id=packs[0]['id'])
                fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
                fixture.bytes(physical, original)
                unavailable('packed')
                fixture.recheck(scope)
                assert read('packed').content == packed
                print('PASS remote pack source snapshots retain full verification and quarantine/recheck invalidation', flush=True)
        finally:
            for marker in markers:
                marker.unlink(missing_ok=True)
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=40)


if __name__ == '__main__':
    main()
