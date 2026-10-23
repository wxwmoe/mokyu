"""Full HTTP raw reads, authority changes and GC, with request-scoped SQL counts."""
import os
import re
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    config = Path(os.environ['MOKYU_TEST_CONFIG'])
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log_path = faults / 'read-snapshots.log'
    log = log_path.open('w')
    environment = dict(os.environ, RUST_LOG='mokyu=info,mokyu::status::stats=debug,sqlx::query=debug', NO_COLOR='1')
    process = subprocess.Popen(command, stdout=log, stderr=log, env=environment)
    markers = []

    def ready():
        assert process.poll() is None, 'service exited; inspect read-snapshots.log'
        return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0

    def arm(name):
        marker = fixture.arm(name)
        markers.append(marker)
        return marker

    def trace(request_id):
        lines = re.sub(r'\x1b\[[0-9;]*m', '', log_path.read_text()).splitlines()
        return [line for line in lines if request_id in line]

    def one_query(request_id):
        wait(lambda: any('request finished' in line for line in trace(request_id)), 'request did not finish in SQL trace')
        statements = [line for line in trace(request_id) if 'rows_returned=' in line]
        assert len(statements) == 1, (request_id, statements)

    def denied(call):
        try:
            call()
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403, error.response['Error']
        else:
            raise AssertionError('request bypassed authorization')

    try:
        wait(ready, 'service did not start', seconds=60)
        fixture, database = Fixture(), Database()
        settings = fixture.cli('settings', 'get', 'requests')
        settings['s3_domain'] = 's3.example.test'
        fixture.cli('settings', 'set', 'requests', '-', value=settings)
        process.terminate()
        process.wait(timeout=40)
        process = subprocess.Popen(command, stdout=log, stderr=log, env=environment)
        wait(ready, 'service did not apply its S3 domain', seconds=60)
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, pack_range_enabled=False, deduplication_enabled=False,
                       unreferenced_grace='1s', min_storage_duration='0s', upload_retention='1s')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        fixture.cli('domain', 'set', 'public.example.test', fixture.name)
        fixture.cli('bucket', 'cors', fixture.name, '-', value=[dict(origins=['https://client.example.test'],
            methods=['GET', 'HEAD'], headers=['range'], expose=['ETag'], max_age=60)])
        body = bytes(range(256)) * 257
        key = 'a/b+%2F'
        fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read')
        fixture.client.put_object(Bucket=fixture.name, Key='empty', Body=b'', ACL='public-read')
        endpoint = os.environ['MOKYU_TEST_ENDPOINT']
        public = os.environ['MOKYU_TEST_PUBLIC_WEB']
        paths = [(endpoint + '/' + fixture.name + '/' + quote(key, safe='/'), {}),
                 (endpoint + '/' + quote(key, safe='/'), {'Host': fixture.name}),
                 (endpoint + '/' + quote(key, safe='/'), {'Host': fixture.name + '.s3.example.test'}),
                 (public + '/' + quote(key, safe='/'), {'Host': 'public.example.test'})]
        for url, extra in paths:
            for method in ('GET', 'HEAD'):
                response = requests.request(method, url, headers={**extra, 'Accept-Encoding': 'identity',
                    'Origin': 'https://client.example.test'}, timeout=10)
                assert response.status_code == 200, (method, url, response.status_code, response.text)
                assert response.content == (body if method == 'GET' else b'')
                assert response.headers['Access-Control-Allow-Origin'] == 'https://client.example.test'
                one_query(response.headers['x-request-id'])
            response = requests.get(url, headers={**extra, 'Range': 'bytes=17-89'}, timeout=10)
            assert response.status_code == 206 and response.content == body[17:90]
            one_query(response.headers['x-request-id'])
            response = requests.options(url, headers={**extra, 'Origin': 'https://client.example.test',
                'Access-Control-Request-Method': 'GET', 'Access-Control-Request-Headers': 'range'}, timeout=10)
            assert response.status_code == 204 and not response.content
        for item in (key, 'empty'):
            response = fixture.client.get_object(Bucket=fixture.name, Key=item)
            with response['Body'] as stream:
                assert stream.read() == (body if item == key else b'')
            one_query(response['ResponseMetadata']['HTTPHeaders']['x-request-id'])
            response = fixture.client.head_object(Bucket=fixture.name, Key=item)
            one_query(response['ResponseMetadata']['HTTPHeaders']['x-request-id'])
        signed_url = fixture.client.generate_presigned_url('get_object', Params={'Bucket': fixture.name, 'Key': key})
        response = requests.get(signed_url, timeout=10)
        assert response.status_code == 200 and response.content == body
        one_query(response.headers['x-request-id'])
        print('PASS one foreground SQL per ordinary signed/anonymous raw GET, HEAD and Range across all routes', flush=True)

        invalid = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
            aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key='incorrect-secret',
            config=Config(signature_version='s3v4', retries={'max_attempts': 0}))
        denied(lambda: invalid.get_object(Bucket=fixture.name, Key=key))
        tampered = re.sub(r'(X-Amz-Signature=)[^&]+', lambda match: match[1] + '0' * 64, signed_url)
        assert requests.get(tampered, timeout=10).status_code == 403
        fixture.client.put_object_acl(Bucket=fixture.name, Key=key, ACL='private')
        for url, extra in paths:
            response = requests.get(url, headers=extra, timeout=10)
            assert response.status_code == 403
        fixture.read(key, body)
        fixture.cli('cred', 'disable', fixture.credentials['access_key'])
        denied(lambda: fixture.client.head_object(Bucket=fixture.name, Key=key))
        fixture.cli('cred', 'enable', fixture.credentials['access_key'])
        expiring = fixture.cli('cred', 'create', fixture.name, '--ttl', '1s')
        expired_client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
            aws_access_key_id=expiring['access_key'], aws_secret_access_key=expiring['secret_key'],
            config=Config(signature_version='s3v4', retries={'max_attempts': 0}))
        time.sleep(1.1)
        denied(lambda: expired_client.head_object(Bucket=fixture.name, Key=key))
        other = fixture.cli('bucket', 'create', fixture.name + '-other', '--storage', fixture.store['id'])
        other_credentials = fixture.cli('cred', 'create', other['name'])
        other_client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
            aws_access_key_id=other_credentials['access_key'], aws_secret_access_key=other_credentials['secret_key'],
            config=Config(signature_version='s3v4', retries={'max_attempts': 0}))
        denied(lambda: other_client.get_object(Bucket=fixture.name, Key=key))
        fixture.client.put_object_acl(Bucket=fixture.name, Key=key, ACL='public-read')
        print('PASS malformed signatures never fall back to public reads; ACL, disabled credentials and grants remain authoritative', flush=True)

        for url, extra in (paths[0], paths[-1]):
            for change in ('acl', 'overwrite'):
                fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read')
                etag = fixture.client.head_object(Bucket=fixture.name, Key=key)['ETag']
                marker = arm('read-metadata')
                with ThreadPoolExecutor(max_workers=1) as executor:
                    pending = executor.submit(requests.get, url, headers={**extra, 'If-Match': etag}, timeout=30)
                    try:
                        wait(lambda: marker.with_suffix('.hit').exists() or pending.done(), 'conditional metadata did not pause')
                        assert not pending.done()
                        if change == 'acl':
                            fixture.client.put_object_acl(Bucket=fixture.name, Key=key, ACL='private')
                        else:
                            fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body[::-1], ACL='public-read')
                    finally:
                        marker.unlink(missing_ok=True)
                    assert pending.result(timeout=30).status_code == (403 if change == 'acl' else 412)
        print('PASS conditional public bodies recheck ACL and object version after metadata evaluation', flush=True)

        fixture.put('pinned', body)
        bucket = uuid.UUID(fixture.bucket_id)
        row, = database.query(f'''SELECT r.id,r.{database.raw_location} AS file_uuid FROM objects o
            JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, 'pinned' if database.type == 'postgres' else b'pinned')
        identity = row['file_uuid']
        identity = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
        path = fixture.root / 'raw' / identity.hex[-2:] / identity.hex
        marker = arm('read-pinned')
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(fixture.read, 'pinned', body)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or pending.done(), 'reader did not pin before opening raw')
                assert not pending.done()
                fixture.put('pinned', b'new value')
                time.sleep(1.1)
                fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
                assert path.exists(), 'GC deleted the selected raw file before open'
            finally:
                marker.unlink(missing_ok=True)
            pending.result(timeout=30)
        fixture.read('pinned', b'new value')
        def reclaimed():
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            return not path.exists()
        wait(reclaimed, 'raw source was not reclaimed after the reader finished', seconds=45)
        print('PASS snapshot pins protect raw before file open, through overwrite and GC, and release after completion', flush=True)

        fixture.put('health', body)
        row, = database.query('''SELECT r.id FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id
            JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s''',
            bucket, 'health' if database.type == 'postgres' else b'health')
        database.query("UPDATE raw_sources SET quarantine_code='hash_mismatch',revision=revision+1 WHERE id=%s", row['id'])
        try:
            fixture.client.get_object(Bucket=fixture.name, Key='health')
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] >= 400
        else:
            raise AssertionError('snapshot accepted a quarantined raw source')
        database.query('UPDATE raw_sources SET quarantine_code=NULL,revision=revision+1 WHERE id=%s', row['id'])
        fixture.read('health', body)
        print('PASS raw source quarantine and recovery are read from the current database generation', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        log.close()


if __name__ == '__main__':
    main()
