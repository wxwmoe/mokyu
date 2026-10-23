"""Public metadata expiry, invalidation and signed isolation through real HTTP."""
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
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    path = faults / 'public-read-cache.log'
    log = path.open('w')
    environment = dict(os.environ, RUST_LOG='mokyu=info,mokyu::status::stats=debug,sqlx::query=debug', NO_COLOR='1')
    process = None
    markers = []

    def start(cache):
        nonlocal process
        text = config.read_text()
        config.write_text(re.sub(r'^response_cache_size = .*$', f'response_cache_size = "{cache}"', text, flags=re.M))
        process = subprocess.Popen(command, stdout=log, stderr=log, env=environment)
        def ready():
            assert process.poll() is None, 'service exited; inspect public-read-cache.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not start', seconds=60)

    def stop():
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=40)

    def count(response):
        request_id = response.headers['x-request-id']
        def trace():
            return [line for line in path.read_text().splitlines() if request_id in line]
        wait(lambda: any('request finished' in line for line in trace()), 'request did not finish')
        return len([line for line in trace() if 'rows_returned=' in line])

    def policy(**values):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(values)
        return fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(name):
        marker = fixture.arm(name)
        markers.append(marker)
        return marker

    try:
        start('0B')
        fixture, database = Fixture(), Database()
        settings = fixture.cli('settings', 'get', 'requests')
        settings['s3_domain'] = 's3.example.test'
        fixture.cli('settings', 'set', 'requests', '-', value=settings)
        stop()
        start('0B')
        defaults = fixture.cli('storage', 'profile', 'show', fixture.name)
        assert defaults['public_cache_validation'] == 'strict'
        assert defaults['public_cache_refresh_interval'] == '1s' and defaults['public_cache_max_age'] == '5s'
        for values in ({'public_cache_validation': 'unknown'}, {'public_cache_max_age': '0s'},
                       {'public_cache_refresh_interval': '61s'}, {'public_cache_refresh_interval': '6s', 'public_cache_max_age': '5s'}):
            response = fixture.session.put(fixture.web + '/api/v1/storages/' + fixture.name + '/profile',
                headers=fixture.headers, json={**defaults, **values}, timeout=15)
            assert response.status_code == 400, response.text
        policy(cdc_enabled=False, pack_range_enabled=False, public_cache_validation='background',
               public_cache_refresh_interval='10s', public_cache_max_age='30s')
        fixture.cli('domain', 'set', 'public-cache.test', fixture.name)
        rules = [dict(origins=['https://client.test'], methods=['GET', 'HEAD'], headers=['range'], expose=['ETag'], max_age=60)]
        fixture.cli('bucket', 'cors', fixture.name, '-', value=rules)
        body = b'Public metadata must have a bounded lifetime.\n' * 1700
        key = 'cached'

        def put(value=body, **metadata):
            return fixture.client.put_object(Bucket=fixture.name, Key=key, Body=value, ACL='public-read', **metadata)

        endpoint, public = os.environ['MOKYU_TEST_ENDPOINT'], os.environ['MOKYU_TEST_PUBLIC_WEB']
        routes = [(endpoint + '/' + fixture.name + '/' + key, {}),
                  (endpoint + '/' + key, {'Host': fixture.name}),
                  (endpoint + '/' + key, {'Host': fixture.name + '.s3.example.test'}),
                  (public + '/' + key, {'Host': 'public-cache.test'})]

        def read(route=0, method='GET', **headers):
            url, extra = routes[route]
            return requests.request(method, url, headers={'Accept-Encoding': 'identity', **extra, **headers}, timeout=20)

        def warm():
            for route in range(4):
                for _ in range(3):
                    response = read(route)
                    assert response.status_code == 200 and response.content == body, (route, response.status_code, response.text)

        put()
        for route in range(4):
            assert read(route, 'HEAD').status_code == 200
            response = read(route, 'HEAD', Origin='https://client.test')
            assert count(response) == 0 and response.headers['Access-Control-Allow-Origin'] == 'https://client.test'
            assert read(route).content == body
        state = fixture.cli('status')
        assert state['response_cache']['limit_bytes'] == 0
        # Path and virtual-host requests share the same canonical bucket route.
        assert state['public_read_cache']['entries'] == 3, state['public_read_cache']
        print('PASS zero body budget retains bounded public metadata and zero-SQL HEAD across four routes', flush=True)

        large_rules = [dict(origins=[f'https://site{n}.test'], methods=['GET', 'HEAD'],
                           headers=['range'], expose=['ETag'], max_age=60) for n in range(100)]
        fixture.cli('bucket', 'cors', fixture.name, '-', value=large_rules)
        for route in range(4):
            assert read(route, 'HEAD', Origin='https://site99.test').status_code == 200
            response = read(route, 'HEAD', Origin='https://site99.test')
            assert count(response) == 0 and response.headers['Access-Control-Allow-Origin'] == 'https://site99.test'
        state = fixture.cli('status')
        for cache, used in (('public_read_cache', 'used_bytes'), ('cors_cache', 'live_bytes')):
            assert 0 < state[cache][used] <= state[cache]['limit_bytes'], state[cache]
        fixture.cli('bucket', 'cors', fixture.name, '-', value=rules)
        for route in range(4):
            assert 'Access-Control-Allow-Origin' not in read(route, 'HEAD', Origin='https://site99.test').headers
            assert read(route, 'HEAD', Origin='https://client.test').headers['Access-Control-Allow-Origin'] == 'https://client.test'
        print('PASS large CORS policies reuse zero-SQL snapshots within budget and invalidate on replacement', flush=True)

        stop()
        start('4MiB')
        fixture.client.put_object(Bucket=fixture.name, Key='cold-coordination', Body=body, ACL='public-read')
        warm()
        with ThreadPoolExecutor(max_workers=8) as pool:
            replies = list(pool.map(lambda n: read(n % 4), range(24)))
        assert all(reply.status_code == 200 and reply.content == body and count(reply) == 0 for reply in replies)
        marker = arm('public-cache-validated')
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(requests.head, endpoint + '/' + fixture.name + '/cold-coordination', timeout=20)
            try:
                wait(marker.with_suffix('.hit').exists, 'fresh reader did not hold coordination')
                response = read()
                assert response.content == body and count(response) == 0
                assert response.elapsed.total_seconds() < 1, 'body hit waited for source coordination'
                assert read(0, 'HEAD').status_code == 200
            finally:
                marker.unlink(missing_ok=True)
            assert pending.result(timeout=20).status_code == 200
        for route in range(4):
            response = read(route, Range='bytes=11-93', Origin='https://client.test')
            assert response.status_code == 206 and response.content == body[11:94] and count(response) == 0
            assert 'Access-Control-Allow-Origin' not in read(route, Origin='https://denied.test').headers
            response = read(route, 'OPTIONS', Origin='https://client.test', **{'Access-Control-Request-Method': 'GET'})
            assert response.status_code == 204 and not response.content and count(response) == 0
        for headers in ({'Cache-Control': 'no-cache'}, {'Pragma': 'no-cache'}, {'If-Match': read().headers['ETag']}):
            response = read(**headers)
            assert response.content == body and count(response) >= 1
        signed = fixture.client.generate_presigned_url('get_object', Params={'Bucket': fixture.name, 'Key': key})
        response = requests.get(signed, timeout=15)
        assert response.content == body and count(response) >= 1
        tampered = re.sub(r'(X-Amz-Signature=)[^&]+', lambda m: m[1] + '0' * 64, signed)
        assert requests.get(tampered, timeout=15).status_code == 403
        fixture.cli('cred', 'disable', fixture.credentials['access_key'])
        assert requests.get(signed, timeout=15).status_code == 403
        fixture.cli('cred', 'enable', fixture.credentials['access_key'])
        print('PASS zero-SQL cached bodies and Range, per-request CORS, fresh conditional/no-cache reads and signed isolation', flush=True)

        fixture.client.put_object_acl(Bucket=fixture.name, Key=key, ACL='private')
        assert all(read(route).status_code == 403 for route in range(4))
        fixture.read(key, body)
        put(body[::-1])
        assert all(read(route).content == body[::-1] for route in range(4))
        put()
        warm()
        fixture.cli('bucket', 'cors', fixture.name, '-', value=[])
        assert all('Access-Control-Allow-Origin' not in read(route, Origin='https://client.test').headers for route in range(4))
        fixture.client.delete_object(Bucket=fixture.name, Key=key)
        assert [read(route).status_code for route in range(4)] == [404, 403, 404, 404]
        put()
        warm()
        policy(public_cache_validation='strict')
        assert all(count(read(route)) >= 1 for route in range(4))
        print('PASS local ACL, overwrite, delete, CORS and profile changes invalidate before acknowledgement', flush=True)

        policy(public_cache_validation='background', public_cache_refresh_interval='1s', public_cache_max_age='3s')
        warm()
        marker = arm('public-cache-refresh')
        time.sleep(1.05)
        assert read().content == body
        wait(marker.with_suffix('.hit').exists, 'refresh did not reach its queue')
        bucket = uuid.UUID(fixture.bucket_id)
        sqlkey = key if database.type == 'postgres' else key.encode()
        row, = database.query('SELECT stream_id FROM objects WHERE bucket_id=%s AND object_key=%s', bucket, sqlkey)
        database.query('UPDATE streams SET public_read=%s WHERE id=%s', False, row['stream_id'])
        assert read().content == body, 'test did not exercise the explicit stale-permission window'
        time.sleep(2.2)
        assert all(read(route).status_code == 403 for route in range(4)), 'refresh failures extended authority past hard expiry'
        assert fixture.cli('status')['public_read_cache']['failed'] >= 1
        marker.unlink(missing_ok=True)
        database.query('UPDATE streams SET public_read=%s WHERE id=%s', True, row['stream_id'])
        assert read().content == body
        print('PASS background failures cannot renew the hard expiry or serve expired permissions', flush=True)

        policy(public_cache_refresh_interval='10s', public_cache_max_age='30s')
        # A metadata load that started before a domain edit must not repopulate that route.
        other = fixture.cli('bucket', 'create', fixture.name + '-other', '--storage', fixture.store['id'])
        marker = arm('public-cache-validated')
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(read, 3)
            try:
                wait(marker.with_suffix('.hit').exists, 'public metadata load did not pause')
                fixture.cli('domain', 'delete', 'public-cache.test')
                fixture.cli('domain', 'set', 'public-cache.test', other['name'])
            finally:
                marker.unlink(missing_ok=True)
            assert pending.result(timeout=20).status_code == 200
        assert read(3).status_code == 404
        put(CacheControl='private, no-store')
        for _ in range(3):
            assert read().content == body
        assert fixture.cli('status')['public_read_cache']['entries'] == 0
        print('PASS late builders cannot resurrect old domain bindings; original no-store prevents admission', flush=True)

        policy(cdc_enabled=True, compression_enabled=True)
        put(ContentType='text/plain')
        for _ in range(3):
            assert read().content == body
        response = read()
        assert response.content == body and count(response) == 0
        fixture.client.put_object_acl(Bucket=fixture.name, Key=key, ACL='private')
        assert read().status_code == 403
        print('PASS chunks reuse the same public snapshots and immediate local ACL invalidation', flush=True)

        policy(cdc_enabled=False, compression_enabled=False, deduplication_enabled=True, write_processing='background')
        def drained():
            return not database.query('SELECT w.stream_id FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE s.bucket_id=%s', bucket)
        fixture.client.put_object(Bucket=fixture.name, Key='dedup-base', Body=body, ACL='public-read')
        wait(drained, 'initial raw deduplication did not finish')
        marker = arm('raw-processing-before-switch')
        put()
        wait(marker.with_suffix('.hit').exists, 'raw replacement did not pause')
        for _ in range(3):
            assert read().content == body
        assert fixture.cli('status')['public_read_cache']['entries'] > 0
        marker.unlink(missing_ok=True)
        wait(drained, 'raw replacement did not finish')
        assert fixture.cli('status')['public_read_cache']['entries'] == 0
        assert read().content == body
        print('PASS raw background deduplication invalidates the previous representation before releasing its source', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
