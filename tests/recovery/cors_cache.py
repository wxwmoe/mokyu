"""Revisioned CORS reuse across HTTP routes and all bucket settings entrypoints."""
import json
import os
import runpy
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from tests.storage.source_quarantine import Fixture, wait
from tests.recovery.source_repair import Database


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    assert not os.environ.get('MOKYU_TEST_BACKEND'), 'use an isolated local raw fixture'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'cors-cache.log').open('w')
    process = None

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)
        def ready():
            assert process.poll() is None, 'service exited; inspect cors-cache.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'CORS service did not start', seconds=60)

    def stop():
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=40)

    try:
        start()
        fixture = Fixture()
        settings = fixture.cli('settings', 'get', 'requests')
        settings['s3_domain'] = 's3.example.test'
        fixture.cli('settings', 'set', 'requests', '-', value=settings)
        stop()
        start()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, pack_range_enabled=False, cors_cache_enabled=True, public_cache_refresh_interval="1s", public_cache_max_age="3s")
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        fixture.cli('domain', 'set', 'cors.example.test', fixture.name)
        credentials = faults / 'cors-credentials.json'
        credentials.write_text(json.dumps({**fixture.credentials, 'bucket': fixture.name}))
        credentials.chmod(0o600)
        os.environ.update(MOKYU_TEST_CREDENTIALS=str(credentials), MOKYU_TEST_PUBLIC=os.environ['MOKYU_TEST_PUBLIC_WEB'],
            MOKYU_TEST_PUBLIC_HOST='cors.example.test', MOKYU_TEST_S3_DOMAIN='s3.example.test', MOKYU_TEST_PASSWORD=fixture.password)
        runpy.run_module('tests.s3.cors', run_name='__main__')
        assert fixture.cli('status')['response_cache']['limit_bytes'] == 0, 'this suite verifies independent metadata memory'
        key, body = 'cors-cache', b'current CORS rules with immutable public bytes'
        fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read')
        endpoint, public = os.environ['MOKYU_TEST_ENDPOINT'], os.environ['MOKYU_TEST_PUBLIC_WEB']
        routes = [(endpoint + '/' + fixture.name + '/' + key, {}),
                  (endpoint + '/' + key, {'Host': fixture.name}),
                  (endpoint + '/' + key, {'Host': fixture.name + '.s3.example.test'}),
                  (public + '/' + key, {'Host': 'cors.example.test'})]
        origins = ['https://one.example.test', 'https://two.example.test']
        rules = [dict(origins=origins, methods=['GET', 'HEAD'], headers=['range'], expose=['ETag'], max_age=60)]

        def status():
            return fixture.cli('status')['cors_cache']

        def save(value):
            fixture.cli('bucket', 'cors', fixture.name, '-', value=value)

        def read(number, origin=None, method='GET', **headers):
            url, host = routes[number % len(routes)]
            if origin:
                headers['Origin'] = origin
            return requests.request(method, url, headers={**host, 'Accept-Encoding': 'identity', **headers}, timeout=15)

        save(rules)
        before = status()
        for index in range(len(routes)):
            response = read(index)
            assert response.status_code == 200 and response.content == body
            assert 'Origin' in response.headers['Vary'] and 'Access-Control-Allow-Origin' not in response.headers
        assert status()['parses'] == before['parses']

        wait(lambda: status()['buckets'] == 1, 'asynchronous CORS fill did not finish')
        read(0, origins[0])
        before = status()

        def concurrent(index):
            origin = origins[index % 2]
            method = 'HEAD' if index % 3 == 0 else 'GET'
            response = read(index, origin, method)
            assert response.status_code == 200 and response.content == (b'' if method == 'HEAD' else body)
            assert response.headers['Access-Control-Allow-Origin'] == origin
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(concurrent, range(32)))
        after = status()
        assert after['parses'] == before['parses'] and after['hits'] >= before['hits'] + 32, (before, after)
        assert after['live_bytes'] + after['index_limit_bytes'] <= after['limit_bytes']
        assert 'Access-Control-Allow-Origin' not in read(0, 'https://denied.example.test').headers
        for index in range(len(routes)):
            for origin in origins:
                response = read(index, origin, 'OPTIONS', **{'Access-Control-Request-Method': 'HEAD', 'Access-Control-Request-Headers': 'Range'})
                assert response.status_code == 204 and response.headers['Access-Control-Allow-Origin'] == origin
                assert response.headers['Access-Control-Allow-Headers'] == 'Range'
            response = read(index, origins[0], 'OPTIONS', **{'Access-Control-Request-Method': 'HEAD', 'Access-Control-Request-Headers': 'Authorization'})
            assert response.status_code == 403 and 'Access-Control-Allow-Origin' not in response.headers
        print('PASS zero body budget, no-Origin fast path, one concurrent parse and per-request CORS matching', flush=True)

        cors_url = fixture.web + '/api/v1/buckets/' + fixture.bucket_id + '/cors'
        current = [dict(rules[0], origins=[origins[1]])]
        response = fixture.session.put(cors_url, headers=fixture.headers, json=current, timeout=15)
        assert response.status_code == 200, response.text
        before = status()
        for index in range(len(routes)):
            assert 'Access-Control-Allow-Origin' not in read(index, origins[0]).headers
            assert read(index, origins[1]).headers['Access-Control-Allow-Origin'] == origins[1]
        wait(lambda: status()['pending'] == 0, 'CORS update fill did not finish')
        settings_url = fixture.web + '/api/v1/buckets/' + fixture.bucket_id + '/settings'
        original = fixture.session.get(settings_url, timeout=15).json()
        values = {name: original['bucket'][name] for name in ('revision', 'cors', 'website_enabled', 'index_document', 'error_document', 'public_base_url', 'uploads_paused')}
        values.update(cors=[dict(rules[0], origins=['*'])], domains=original['domains'])
        response = fixture.session.put(settings_url, headers=fixture.headers, json=values, timeout=15)
        assert response.status_code == 200, response.text
        assert read(3, origins[0]).headers['Access-Control-Allow-Origin'] == '*'
        save([])
        for index in range(len(routes)):
            response = read(index, origins[0])
            assert response.status_code == 200 and 'Access-Control-Allow-Origin' not in response.headers
            assert 'Origin' not in response.headers.get('Vary', '').split(', ')
        print('PASS CLI, CORS API and complete bucket settings use the current revision, including empty rules', flush=True)

        # A paused worker must not delay misses or reinsert an older local revision.
        marker = fixture.arm('cors-cache-work')
        try:
            save(rules)
            first = read(0, origins[0])
            assert first.status_code == 200 and first.elapsed.total_seconds() < 1
            wait(marker.with_suffix('.hit').exists, 'CORS fill was not queued')
            save([])
            assert 'Access-Control-Allow-Origin' not in read(0, origins[0]).headers
        finally:
            marker.unlink(missing_ok=True)
        wait(lambda: status()['pending'] == 0, 'stale work was not drained')
        assert 'Access-Control-Allow-Origin' not in read(0, origins[0]).headers
        print('PASS paused fills never block readers or restore an obsolete revision', flush=True)

        # Standalone CORS caching retains a bounded stale window without public object caching.
        save(rules)
        read(0, origins[0])
        wait(lambda: status()['buckets'] == 1, 'standalone CORS cache did not fill')
        marker = fixture.arm('cors-cache-work')
        try:
            time.sleep(1.05)
            assert read(0, origins[0]).headers['Access-Control-Allow-Origin'] == origins[0]
            wait(marker.with_suffix('.hit').exists, 'asynchronous CORS validation was not queued')
            database = Database()
            database.query('UPDATE buckets SET cors=%s,settings_revision=settings_revision+1 WHERE id=%s', '[]', uuid.UUID(fixture.bucket_id))
            response = read(0, origins[0])
            assert response.elapsed.total_seconds() < 1
            assert response.headers['Access-Control-Allow-Origin'] == origins[0]
            time.sleep(2.2)
            assert 'Access-Control-Allow-Origin' not in read(0, origins[0]).headers
        finally:
            marker.unlink(missing_ok=True)
        print('PASS CORS-only background confirmation and hard expiry without response caching', flush=True)

        other = Fixture()
        other.client.put_object(Bucket=other.name, Key=key, Body=b'other bucket', ACL='public-read')
        other.cli('bucket', 'cors', other.name, '-', value=[dict(rules[0], origins=[origins[1]])])
        fixture.cli('domain', 'set', 'cors.example.test', other.name, succeeds=False)
        assert read(3, origins[1]).content == body
        fixture.cli('domain', 'delete', 'cors.example.test')
        assert read(3, origins[1]).status_code == 403
        fixture.cli('domain', 'set', 'cors.example.test', other.name)
        response = read(3, origins[1])
        assert response.content == b'other bucket' and response.headers['Access-Control-Allow-Origin'] == origins[1]
        assert 'Access-Control-Allow-Origin' not in read(3, origins[0]).headers
        fixture.cli('domain', 'delete', 'cors.example.test')
        assert read(3, origins[1]).status_code == 403
        print('PASS domain reassignment and removal cannot reuse another bucket policy', flush=True)

        name = fixture.name + '-empty'
        empty = fixture.cli('bucket', 'create', name, '--storage', fixture.store['id'])
        fixture.cli('bucket', 'cors', name, '-', value=rules)
        url = endpoint + '/' + name + '/absent'
        headers = {'Origin': origins[0], 'Access-Control-Request-Method': 'GET'}
        assert requests.options(url, headers=headers, timeout=10).headers['Access-Control-Allow-Origin'] == origins[0]
        fixture.cli('bucket', 'delete', name)
        recreated = fixture.cli('bucket', 'create', name, '--storage', fixture.store['id'])
        assert empty['id'] != recreated['id']
        response = requests.options(url, headers=headers, timeout=10)
        assert response.status_code == 403 and 'Access-Control-Allow-Origin' not in response.headers
        print('PASS deleting and recreating a bucket cannot inherit cached CORS rules', flush=True)
    finally:
        stop()
        log.close()


if __name__ == '__main__':
    main()
