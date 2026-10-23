"""Optional ordinary-read digests, bounded checks and verified cache admission."""
import os
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
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    markers = []
    with (faults / 'read-integrity.log').open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log,
            env=dict(os.environ, RUST_LOG='mokyu=debug', NO_COLOR='1'))
        try:
            def ready():
                assert process.poll() is None, 'service exited; inspect read-integrity.log'
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
            wait(ready, 'service did not start', seconds=60)
            database = Database()

            def policy(fixture, **values):
                profile = fixture.cli('storage', 'profile', 'show', fixture.name)
                profile.update(values)
                return fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

            def get(fixture, key='raw', **headers):
                return requests.get(os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/' + key,
                    headers={'Accept-Encoding': 'identity', **headers}, timeout=15)

            def unavailable(fixture, key='raw'):
                try:
                    assert get(fixture, key).status_code >= 400, 'unverified corrupt source was returned'
                except requests.exceptions.ChunkedEncodingError:
                    pass

            def arm(fixture, name):
                marker = fixture.arm(name)
                markers.append(marker)
                return marker

            def status(fixture):
                return fixture.cli('status')

            def checks(fixture):
                return status(fixture)['read_checks']

            def settled(fixture):
                wait(lambda: checks(fixture)['active'] == 0, 'optional checks did not drain', seconds=65)

            def quarantines(fixture, source):
                return fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=' + source)['items']

            def put(fixture, key, body):
                fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read')

            def confirmed(fixture, source):
                if quarantines(fixture, source):
                    return True
                # Optional checks may skip a busy attempt; a fresh read can retry.
                try:
                    get(fixture, **{'Cache-Control': 'no-cache'})
                except requests.exceptions.ChunkedEncodingError:
                    pass
                return False

            fixture = Fixture()
            defaults = fixture.cli('storage', 'profile', 'show', fixture.name)
            assert defaults['read_integrity'] == 'inline'
            response = fixture.session.put(fixture.web + '/api/v1/storages/' + fixture.name + '/profile',
                headers=fixture.headers, json={**defaults, 'read_integrity': 'unchecked'}, timeout=15)
            assert response.status_code == 400, response.text
            policy(fixture, cdc_enabled=False, pack_range_enabled=False)
            body = os.urandom(32768)
            put(fixture, 'raw', body)
            physical, = fixture.keys()
            original = fixture.bytes(physical)
            bad = bytes([original[0] ^ 1]) + original[1:]
            fixture.bytes(physical, bad)
            unavailable(fixture)
            policy(fixture, read_integrity='off')
            for _ in range(3):
                assert get(fixture).content == bad
            fixture.read('raw', bad)
            domain = fixture.name + '.example.test'
            fixture.cli('domain', 'set', domain, fixture.name)
            public = requests.get(os.environ['MOKYU_TEST_PUBLIC_WEB'] + '/raw',
                headers={'Host': domain, 'Accept-Encoding': 'identity'}, timeout=15)
            assert public.status_code == 200 and public.content == bad
            hits = status(fixture)['response_cache']['hits']
            assert get(fixture).content == bad
            assert status(fixture)['response_cache']['hits'] > hits, 'off did not admit a bounded unchecked body'
            fixture.bytes(physical, original)
            policy(fixture, read_integrity='inline')
            assert get(fixture).content == body, 'reliable mode reused an unchecked cached body'
            print('PASS default inline, profile validation, explicit off and cache fencing on strict switch', flush=True)

            race = Fixture()
            policy(race, cdc_enabled=False, pack_range_enabled=False, read_integrity='off')
            put(race, 'raw', body)
            physical, = race.keys()
            race.bytes(physical, bad)
            assert get(race).content == bad
            marker = arm(race, 'read-body')
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(get, race)
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or pending.done(), 'reader did not reach its body')
                    assert not pending.done()
                    policy(race, read_integrity='inline')
                finally:
                    marker.unlink(missing_ok=True)
                assert pending.result(timeout=20).content == bad
            unavailable(race)
            race.bytes(physical, original)
            assert get(race).content == body
            print('PASS a late unchecked builder cannot acquire the new reliable configuration generation', flush=True)

            faulty = Fixture()
            policy(faulty, cdc_enabled=False, pack_range_enabled=False, read_integrity='background')
            put(faulty, 'raw', body)
            physical, = faulty.keys()
            faulty.bytes(physical, bad)
            marker = arm(faulty, 'read-integrity-check')
            try:
                assert get(faulty).content == bad
                wait(lambda: marker.with_suffix('.hit').exists(), 'background digest did not start')
                assert checks(faulty)['active'] > 0, 'response waited for its optional check'
            finally:
                marker.unlink(missing_ok=True)
            wait(lambda: confirmed(faulty, 'raw'), 'confirmed bad raw source was not isolated')
            unavailable(faulty)
            settled(faulty)
            print('PASS background returns before checking and isolates only after current backend confirmation', flush=True)

            busy = Fixture()
            policy(busy, cdc_enabled=False, pack_range_enabled=False, read_integrity='background')
            for index in range(5):
                put(busy, str(index), body + bytes([index]))
            marker = arm(busy, 'read-integrity-check')
            before = checks(busy)
            try:
                for index in range(5):
                    assert get(busy, str(index)).content == body + bytes([index])
                current = checks(busy)
                assert current['active'] == 4 and current['skipped'] > before['skipped'], current
                assert current['used_bytes'] <= current['limit_bytes'] == 8 * 1024 * 1024
                live = status(busy)['response_cache']['live_bytes']
                assert get(busy, '4').content == body + b'\x04'
                assert status(busy)['response_cache']['live_bytes'] == live, 'skipped digest admitted a persistent body'
            finally:
                marker.unlink(missing_ok=True)
            settled(busy)
            assert not quarantines(busy, 'raw')
            before = checks(busy)['verified']
            marker = arm(busy, 'read-integrity-check')
            try:
                response = get(busy, '4', Range='bytes=17-511')
                assert response.status_code == 206 and response.content == body[17:512]
                wait(lambda: marker.with_suffix('.hit').exists(), 'Range did not schedule a full-source check')
            finally:
                marker.unlink(missing_ok=True)
            wait(lambda: checks(busy)['verified'] > before, 'Range source was not checked')
            settled(busy)
            put(busy, 'cancel', body * 256)
            marker = arm(busy, 'read-integrity-check')
            before = checks(busy)
            try:
                response = requests.get(os.environ['MOKYU_TEST_ENDPOINT'] + '/' + busy.name + '/cancel',
                    headers={'Accept-Encoding': 'identity'}, stream=True, timeout=15)
                assert response.status_code == 200
                response.raw.read(100)
                response.close()
            finally:
                marker.unlink(missing_ok=True)
            settled(busy)
            current = checks(busy)
            assert current['skipped'] > before['skipped'] and current['used_bytes'] == 0, current
            assert not quarantines(busy, 'raw'), 'client cancellation falsely quarantined a source'
            print('PASS four-job and byte limits, nonblocking overflow, skipped cache admission, Range and cancellation', flush=True)

            chunks = Fixture()
            policy(chunks, read_integrity='off')
            put(chunks, 'raw', body)
            physical, = chunks.keys()
            original = chunks.bytes(physical)
            bad = bytes([original[0] ^ 1]) + original[1:]
            chunks.bytes(physical, bad)
            assert get(chunks).content == bad
            policy(chunks, read_integrity='inline')
            unavailable(chunks)
            policy(chunks, read_integrity='off')
            row, = database.chunks(chunks)
            scope = dict(storage_id=chunks.store['id'], source='chunk', id=str(row['current_encoding_id']))
            preview = chunks.preview('quarantine', scope)
            assert preview, 'explicit full inspection used the weak ordinary-read mode'
            policy(chunks, read_integrity='background')
            marker = arm(chunks, 'read-integrity-check')
            try:
                assert get(chunks).content == bad
                wait(lambda: marker.with_suffix('.hit').exists(), 'chunk check did not start')
            finally:
                marker.unlink(missing_ok=True)
            wait(lambda: confirmed(chunks, 'chunk'), 'bad chunk was not isolated')
            unavailable(chunks)
            settled(chunks)
            print('PASS chunks obey the read policy while full inspection and source isolation remain strong', flush=True)

            if chunks.backend:
                cached = Fixture()
                policy(cached, cdc_enabled=False, pack_range_enabled=False, cache_enabled=True)
                put(cached, 'raw', body)
                assert get(cached).content == body
                row, = database.query(f'SELECT {database.raw_location} AS file_uuid FROM raw_sources WHERE storage_id=%s', uuid.UUID(cached.store['id']))
                identity = row['file_uuid']
                identity = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
                cache = Path(database.config['instance']['data_path']) / 'raw' / identity.hex[-2:] / identity.hex
                wait(cache.exists, 'raw disk cache was not populated')
                cache.write_bytes(bytes([body[0] ^ 1]) + body[1:])
                policy(cached, read_integrity='background')
                marker = arm(cached, 'read-integrity-check')
                before = checks(cached)['failed']
                try:
                    assert get(cached).content != body
                finally:
                    marker.unlink(missing_ok=True)
                wait(lambda: checks(cached)['failed'] > before, 'actual corrupt cache bytes were not checked')
                settled(cached)
                assert not quarantines(cached, 'raw'), 'bad cache falsely isolated a healthy backend'
                assert get(cached).content == body
                policy(cached, read_integrity='inline')
                assert get(cached).content == body
                print('PASS actual cached bytes are checked, bad cache is rejected and a healthy remote backend remains usable', flush=True)
                settled(cached)
                packed = Fixture()
                policy(packed, pack_enabled=True, read_integrity='off', pack_interval='24h', pack_repack_interval='24h')
                payload = os.urandom(9 * 1024 * 1024 + 17)
                put(packed, 'raw', payload)
                packed.finished(packed.cli('pack', 'run'))
                packs = packed.api('/api/v1/storage/packs?bucket=' + packed.bucket_id + '&state=ready')['items']
                assert packs, 'expected an actual remote pack'
                assert get(packed).content == payload
                detail = packed.api('/api/v1/packs/' + packs[0]['id'])
                identity = detail['pack']['id' if database.type == 'postgres' else 'location_id'].replace('-', '')
                physical = packed.prefix + 'packs/' + identity[-2:] + '/' + identity
                original = packed.bytes(physical)
                packed.bytes(physical, original[:-1] + bytes([original[-1] ^ 1]))
                assert get(packed).content != payload
                policy(packed, read_integrity='inline')
                unavailable(packed)
                print('PASS weak pack reads do not supply verified decoded packs or unchecked members to reliable reads', flush=True)
        finally:
            for marker in markers:
                marker.unlink(missing_ok=True)
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=40)


if __name__ == '__main__':
    main()
