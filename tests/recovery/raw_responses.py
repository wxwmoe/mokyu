"""Bounded raw/chunk response reuse, fresh authority and representation recovery."""
import json
import os
import re
import runpy
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    config = Path(os.environ['MOKYU_TEST_CONFIG'])
    text = config.read_text()
    assert re.search(r'^response_cache_size = ', text, re.M)
    config.write_text(re.sub(r'^response_cache_size = .*$', 'response_cache_size = "4MiB"', text, flags=re.M))
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log_path = faults / 'raw-responses.log'
    log = log_path.open('w')
    environment = dict(os.environ, RUST_LOG='mokyu=info,mokyu::status::stats=debug,sqlx::query=debug', NO_COLOR='1')
    process = subprocess.Popen(command, stdout=log, stderr=log, env=environment)
    markers = []

    def ready():
        assert process.poll() is None, 'service exited; inspect raw-responses.log'
        return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0

    def one_query(response):
        request_id = response.headers['x-request-id']
        def trace():
            return [line for line in re.sub(r'\x1b\[[0-9;]*m', '', log_path.read_text()).splitlines() if request_id in line]
        wait(lambda: any('request finished' in line for line in trace()), 'request did not finish')
        statements = [line for line in trace() if 'rows_returned=' in line]
        assert len(statements) == 1, statements

    def arm(name):
        marker = fixture.arm(name)
        markers.append(marker)
        return marker

    try:
        wait(ready, 'service did not start', seconds=60)
        fixture, database = Fixture(), Database()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, pack_range_enabled=False, unreferenced_grace='1s', min_storage_duration='0s')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        fixture.cli('domain', 'set', 'raw-cache.example.test', fixture.name)
        credentials = faults / 'response-credentials.json'
        credentials.write_text(json.dumps({**fixture.credentials, 'bucket': fixture.name}))
        credentials.chmod(0o600)
        os.environ.update(MOKYU_TEST_CREDENTIALS=str(credentials), MOKYU_TEST_PUBLIC_HOST='raw-cache.example.test')
        runpy.run_module('tests.s3.response_cache_pressure', run_name='__main__')
        print('PASS raw variants, signed reuse, cancellation, late builders and retained memory under pressure', flush=True)

        endpoint = os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/'
        payload = b'Immutable cached bytes must retain current authority.\n' * 2000

        def put(key, body=payload, **metadata):
            return fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read', **metadata)

        def read(key, **headers):
            return requests.get(endpoint + key, headers={'Accept-Encoding': 'identity', **headers}, timeout=20)

        def warm(key):
            for _ in range(3):
                response = read(key)
                assert response.status_code == 200 and response.content == payload
            return response

        def status():
            return fixture.cli('status')

        def source(key):
            return database.query(f'''SELECT r.id,r.{database.raw_location} AS file_uuid FROM objects o
                JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
                WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(fixture.bucket_id),
                key if database.type == 'postgres' else key.encode())[0]

        put('authority')
        warm('authority')
        before = status()
        response = read('authority')
        assert response.content == payload
        one_query(response)
        signed = fixture.client.generate_presigned_url('get_object', Params={'Bucket': fixture.name, 'Key': 'authority'})
        response = requests.get(signed, timeout=15)
        assert response.content == payload
        one_query(response)
        assert status()['backend_gets'] == before['backend_gets']
        assert status()['response_cache']['hits'] == before['response_cache']['hits'] + 2
        tampered = re.sub(r'(X-Amz-Signature=)[^&]+', lambda m: m[1] + '0' * 64, signed)
        assert requests.get(tampered, timeout=15).status_code == 403
        fixture.cli('cred', 'disable', fixture.credentials['access_key'])
        assert requests.get(signed, timeout=15).status_code == 403
        fixture.cli('cred', 'enable', fixture.credentials['access_key'])
        fixture.client.put_object_acl(Bucket=fixture.name, Key='authority', ACL='private')
        assert read('authority').status_code == 403
        fixture.read('authority', payload)
        fixture.client.put_object_acl(Bucket=fixture.name, Key='authority', ACL='public-read')
        print('PASS hot raw GET retains one foreground SQL, current signatures and private-object authorization', flush=True)

        row = source('authority')
        database.query('UPDATE raw_sources SET revision=revision+1 WHERE id=%s', row['id'])
        before = status()['backend_gets']
        assert warm('authority').content == payload
        assert status()['backend_gets'] > before
        database.query("UPDATE raw_sources SET quarantine_code='hash_mismatch',revision=revision+1 WHERE id=%s", row['id'])
        assert read('authority').status_code >= 400
        try:
            fixture.client.get_object(Bucket=fixture.name, Key='authority')
        except ClientError:
            pass
        else:
            raise AssertionError('signed cache hit ignored source quarantine')
        database.query('UPDATE raw_sources SET quarantine_code=NULL,revision=revision+1 WHERE id=%s', row['id'])
        warm('authority')
        print('PASS source revision changes reject stale bodies; quarantine and recovery apply to both readers', flush=True)

        put('restricted', CacheControl='private, no-store')
        before = status()['response_cache']['entries']
        for _ in range(3):
            result = fixture.client.get_object(Bucket=fixture.name, Key='restricted', ResponseCacheControl='public')
            assert result['Body'].read() == payload
        assert status()['response_cache']['entries'] == before
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='multipart', ACL='public-read')['UploadId']
        part = fixture.client.upload_part(Bucket=fixture.name, Key='multipart', UploadId=upload, PartNumber=1, Body=payload)
        fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=upload,
            MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
        warm('multipart')
        before = status()['backend_gets']
        assert read('multipart').content == payload
        assert status()['backend_gets'] == before
        print('PASS original no-store restrictions survive response overrides; completed raw multipart is reusable', flush=True)

        if not fixture.backend:
            put('changed-after-verify')
            assert read('changed-after-verify').content == payload
            row = source('changed-after-verify')
            identity = row['file_uuid']
            identity = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
            path = fixture.root / 'raw' / identity.hex[-2:] / identity.hex
            marker = arm('read-body')
            before = status()['response_cache']['entries']
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(read, 'changed-after-verify')
                try:
                    wait(marker.with_suffix('.hit').exists, 'body did not pause after source verification')
                    path.write_bytes(payload[::-1])
                finally:
                    marker.unlink(missing_ok=True)
                try:
                    pending.result(timeout=20)
                except requests.RequestException:
                    pass
                else:
                    raise AssertionError('changed raw bytes were published')
            assert status()['response_cache']['entries'] == before
            path.write_bytes(payload)
            warm('changed-after-verify')
            print('PASS same-length changes after preflight verification cannot poison the body cache', flush=True)

        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=True, compression_enabled=True, write_processing='background')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        marker = arm('cdc-before-read')
        put('converted')
        wait(marker.with_suffix('.hit').exists, 'background CDC did not pause')
        warm('converted')
        marker.unlink()
        def converted():
            rows = database.query('''SELECT s.payload_type FROM objects o JOIN streams s ON s.id=o.stream_id
                WHERE o.bucket_id=%s AND o.object_key=%s''', uuid.UUID(fixture.bucket_id),
                'converted' if database.type == 'postgres' else b'converted')
            return rows[0]['payload_type'] == 'chunks'
        wait(converted, 'raw representation did not convert', seconds=90)
        assert read('converted').content == payload
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(write_processing='inline')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        runpy.run_module('tests.s3.response_cache', run_name='__main__')
        print('PASS raw-to-chunks transitions fence old mappings; original chunk cache and signed reuse still work', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        log.close()


if __name__ == '__main__':
    main()
