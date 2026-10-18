"""Explicit quarantine and full recheck through public management and S3 APIs.

Requires an isolated fault-injection service, boto3, requests, administrator
credentials and a backend owned by this test. MOKYU_TEST_BACKEND optionally
selects a disposable S3 fixture; otherwise MOKYU_TEST_FSYNC_ROOT holds local data.
"""
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError


def wait(predicate, message, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(.05)
    raise AssertionError(message)


class Fault:
    def __init__(self, paths):
        self.paths = paths

    def with_suffix(self, suffix):
        return Fault([path.with_suffix(suffix) for path in self.paths])

    def exists(self):
        return any(path.exists() for path in self.paths)

    def unlink(self, missing_ok=False):
        for path in self.paths:
            path.unlink(missing_ok=missing_ok)


class Fixture:
    def __init__(self):
        assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
        self.command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
        self.name = 'source-' + uuid.uuid4().hex[:12]
        self.web = os.environ['MOKYU_TEST_WEB']
        self.faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
        self.password = (os.environ.get('MOKYU_TEST_PASSWORD') or Path(os.environ['MOKYU_TEST_PASSWORD_FILE']).read_text()).strip()
        self.session = requests.Session()
        response = self.session.post(self.web + '/api/v1/auth/login', headers={'Origin': self.web},
                                     json={'username': 'tester', 'password': self.password}, timeout=30)
        response.raise_for_status()
        self.headers = {'Origin': self.web, 'X-CSRF-Token': response.json()['csrf_token']}
        client_config = Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                               request_checksum_calculation='when_required', response_checksum_validation='when_required')
        self.backend = None
        if file := os.environ.get('MOKYU_TEST_BACKEND'):
            backend = json.loads(Path(file).read_text())
            self.prefix = backend.get('prefix', '').strip('/') + '/' + self.name + '/'
            self.prefix = self.prefix.lstrip('/')
            self.remote_bucket = backend['bucket']
            self.backend = boto3.client('s3', endpoint_url=backend['endpoint'], region_name=backend['region'],
                                       aws_access_key_id=backend['access_key'], aws_secret_access_key=backend['secret_key'], config=client_config)
            self.store = self.cli('storage', 'create', self.name, 's3', '--init', '--endpoint', backend['endpoint'],
                                  '--bucket', backend['bucket'], '--region', backend['region'], '--prefix', self.prefix.rstrip('/'),
                                  '--access', backend['access_key'], '--secret', backend['secret_key'])
        else:
            self.root = Path(os.environ['MOKYU_TEST_FSYNC_ROOT']) / self.name
            self.store = self.cli('storage', 'create', self.name, 'local', '--init', '--path', str(self.root))
        profile = self.cli('storage', 'profile', 'show', self.name)
        profile.update(upload_cache=False, pack_enabled=False, compression_enabled=False, cache_enabled=False)
        self.cli('storage', 'profile', 'set', self.name, '-', value=profile)
        bucket = self.cli('bucket', 'create', self.name, '--storage', self.store['id'])
        self.bucket_id = bucket['id']
        credentials = self.cli('cred', 'create', self.name)
        self.credentials = credentials
        self.client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                                   aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'], config=client_config)

    def cli(self, *args, value=None, succeeds=True):
        reply = subprocess.run(self.command + list(args), input=json.dumps(value) if value is not None else None,
                               capture_output=True, text=True, timeout=150)
        assert (reply.returncode == 0) == succeeds, reply.stderr
        return json.loads(reply.stdout) if succeeds else None

    def api(self, path, value=None, expected=200, headers=None):
        response = self.session.request('GET' if value is None else 'POST', self.web + path,
                                        headers=self.headers if headers is None else headers, json=value, timeout=150)
        assert response.status_code == expected, (path, response.status_code, response.text)
        return response.json() if response.content else None

    def finished(self, reply, state='completed'):
        task = reply['task_id']
        row = wait(lambda: (row if (row := self.api('/api/v1/tasks/' + task))['status']
                            in ('completed', 'failed', 'paused') else None), 'task did not finish')
        assert row['status'] == state, row
        assert 'source_operation' not in row['detail'], 'task exposed its internal authorization snapshot'
        return row

    def keys(self):
        if self.backend:
            return [item['Key'] for item in self.backend.list_objects_v2(Bucket=self.remote_bucket, Prefix=self.prefix).get('Contents', [])
                    if not item['Key'].endswith('meta.json')]
        return [str(path) for path in self.root.rglob('*') if path.is_file() and path.name != 'meta.json']

    def bytes(self, path, value=None):
        if self.backend:
            if value is None:
                with self.backend.get_object(Bucket=self.remote_bucket, Key=path)['Body'] as body:
                    return body.read()
            self.backend.put_object(Bucket=self.remote_bucket, Key=path, Body=value)
        elif value is None:
            return Path(path).read_bytes()
        else:
            Path(path).write_bytes(value)

    def put(self, key, raw):
        return self.client.put_object(Bucket=self.name, Key=key, Body=raw)

    def read(self, key, expected):
        with self.client.get_object(Bucket=self.name, Key=key)['Body'] as body:
            assert body.read() == expected

    def arm(self, name):
        roots = os.environ.get('MOKYU_TEST_SOURCE_FAULT_DIRS', str(self.faults)).split(',') if name == 'source-before-commit' else [self.faults]
        paths = [Path(root) / name for root in roots]
        for path in paths:
            path.with_suffix('.hit').unlink(missing_ok=True)
            path.touch()
        return Fault(paths)

    def scope(self):
        reply = self.cli('integrity', 'check', '--mode', 'full', '--bucket', self.name, '--key', 'original')
        task = self.finished(reply)
        # Cluster inspections have per-storage children.
        if task['detail'].get('role') == 'parent':
            tasks = self.cli('task', 'list')
            children = [item for item in tasks if item.get('parent_task_id') == task['id']]
            assert len(children) == 1
            task = self.finished({'task_id': children[0]['id']})
        issues = self.api('/api/v1/tasks/' + task['id'] + '/issues')['items']
        assert issues, task
        issue = issues[0]
        assert issue['code'] in ('hash_mismatch', 'authentication_failed', 'length_mismatch', 'remote_missing')
        source = issue['detail']
        return dict(storage_id=self.store['id'], source=source['source'],
                    id=source.get('encoding_id') or source.get('pack_id'))

    def preview(self, operation, scope):
        return self.api('/api/v1/maintenance/' + operation + '/preview', scope)

    def execute(self, operation, preview):
        return self.api('/api/v1/maintenance/' + operation + '/execute',
                        dict(preview_id=preview['id'], confirmation=preview['confirmation']))

    def recheck(self, scope, state='completed'):
        return self.finished(self.execute('recheck', self.preview('recheck', scope)), state)


def rejected(call):
    try:
        call()
    except ClientError as error:
        assert error.response['ResponseMetadata']['HTTPStatusCode'] >= 400, error.response
    else:
        raise AssertionError('operation reused a quarantined source')


def main():
    fixture = Fixture()
    raw = bytes(range(256)) * 128
    fixture.client.put_object(Bucket=fixture.name, Key='original', Body=raw, ACL='public-read')
    public_url = os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/original'
    for _ in range(3):
        public = requests.get(public_url, headers={'Accept-Encoding': 'identity'}, timeout=30)
        assert public.status_code == 200 and public.content == raw
    etag = public.headers['ETag']
    keys = wait(fixture.keys, 'backend listing did not expose the uploaded source')
    assert len(keys) == 1, keys
    physical = keys[0]
    original = fixture.bytes(physical)
    bad = bytes([original[0] ^ 1]) + original[1:]
    old = os.urandom(32768)
    fixture.put('replace', old)
    for point in ('source-before-reference', 'source-before-publish'):
        multipart = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='complete')['UploadId']
        part = fixture.client.upload_part(Bucket=fixture.name, Key='complete', UploadId=multipart, PartNumber=1, Body=raw)
        marker = fixture.arm(point)
        with ThreadPoolExecutor(max_workers=2) as pool:
            upload = pool.submit(fixture.put, 'replace', raw)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or upload.done(), 'upload did not reach ' + point)
                assert not upload.done(), 'fault checkpoint did not hold the upload'
                fixture.bytes(physical, bad)
                scope = fixture.scope()
                preview = fixture.preview('quarantine', scope)
                body = dict(preview_id=preview['id'], confirmation=preview['confirmation'])
                fixture.api('/api/v1/maintenance/quarantine/execute', body, expected=403, headers={'Origin': fixture.web})
                if second := os.environ.get('MOKYU_TEST_SECOND_CONTROL'):
                    headers = dict(fixture.headers, Cookie='; '.join(cookie.name + '=' + cookie.value for cookie in fixture.session.cookies))
                    with ThreadPoolExecutor(max_workers=2) as controls:
                        left = controls.submit(fixture.execute, 'quarantine', preview)
                        right = controls.submit(requests.post, second + '/api/v1/maintenance/quarantine/execute', headers=headers, json=body, timeout=30)
                        first = left.result()
                        response = right.result()
                        assert response.status_code == 200 and response.json() == first, response.text
                else:
                    first = fixture.execute('quarantine', preview)
                assert fixture.execute('quarantine', preview) == first
                fixture.finished(first)
                public = requests.get(public_url, headers={'Accept-Encoding': 'identity'}, timeout=30)
                assert public.status_code == 200 and public.content == raw and public.headers['ETag'] == etag
                partial = requests.get(public_url, headers={'Accept-Encoding': 'identity', 'Range': 'bytes=7-47'}, timeout=30)
                assert partial.status_code == 206 and partial.content == raw[7:48]
                assert requests.get(public_url, headers={'If-None-Match': etag}, timeout=30).status_code == 304
                listed = fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=chunk')
                assert len(listed['items']) == 1
            finally:
                marker.unlink(missing_ok=True)
            rejected(lambda: upload.result(timeout=30))
        fixture.read('replace', old)
        rejected(lambda: fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='complete', UploadId=multipart,
            MultipartUpload={'Parts': [{'ETag': part['ETag'], 'PartNumber': 1}]}))
        assert fixture.client.list_parts(Bucket=fixture.name, Key='complete', UploadId=multipart)['Parts'][0]['ETag'] == part['ETag']
        fixture.client.abort_multipart_upload(Bucket=fixture.name, Key='complete', UploadId=multipart)
        rejected(lambda: fixture.client.copy_object(Bucket=fixture.name, Key='copy', CopySource={'Bucket': fixture.name, 'Key': 'original'}))
        multipart = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='parts')['UploadId']
        try:
            rejected(lambda: fixture.client.upload_part_copy(Bucket=fixture.name, Key='parts', UploadId=multipart, PartNumber=1,
                CopySource={'Bucket': fixture.name, 'Key': 'original'}))
        finally:
            fixture.client.abort_multipart_upload(Bucket=fixture.name, Key='parts', UploadId=multipart)
        fixture.recheck(scope, state='failed')
        fixture.bytes(physical, original)
        marker = fixture.arm('source-before-commit')
        try:
            reply = fixture.execute('recheck', fixture.preview('recheck', scope))
            wait(lambda: marker.with_suffix('.hit').exists(), 'recheck did not reach commit barrier')
            fixture.cli('task', 'pause', reply['task_id'])
        finally:
            marker.unlink(missing_ok=True)
        fixture.finished(reply, 'paused')
        assert fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=chunk')['items']
        fixture.cli('task', 'resume', reply['task_id'])
        fixture.finished(reply)
        fixture.read('original', raw)
        fixture.put('reuse', raw)
        fixture.read('reuse', raw)
        print('PASS ' + point + ': quarantine fences publication, copy and part-copy; full recheck restores the exact source', flush=True)
    fixture.cli('integrity', 'quarantines', '--storage', fixture.store['id'], '--source', 'chunk')
    print('PASS admin confirmation, CSRF, idempotency, source listing and failed recheck preserve quarantine', flush=True)


if __name__ == '__main__':
    main()
