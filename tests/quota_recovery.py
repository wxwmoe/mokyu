"""Restart regression: the isolated runner kills/restarts the app after the ready marker appears."""
import http.client
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
marker = Path(os.environ['MOKYU_TEST_RESTART_MARKER'])
assert not marker.exists(), 'use a new restart marker'
url, endpoint = os.environ['MOKYU_TEST_WEB'], os.environ['MOKYU_TEST_ENDPOINT']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']


def cli(*args):
    return json.loads(subprocess.run(command + list(args), capture_output=True, text=True, check=True, timeout=20).stdout)


admin = requests.Session()
reply = admin.post(url + '/api/login', headers={'Origin': url}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
prefix = 'quota-restart-' + uuid.uuid4().hex[:10]
project = admin.post(url + '/api/projects', json={'name': prefix}).json()['id']
bucket = cli('bucket', 'create', prefix)
db.execute('UPDATE buckets SET project_id=%s WHERE id=%s', (project, bucket['id']))
credential = cli('credential', 'create', prefix)
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
reply = admin.put(url + '/api/quotas/project/' + project, json={'byte_limit': '10', 'inflight_limit': '20', 'bucket_limit': None})
assert reply.status_code == 200
s3.put_object(Bucket=prefix, Key='base', Body=b'base')
upload = s3.create_multipart_upload(Bucket=prefix, Key='mpu')['UploadId']
etag = s3.upload_part(Bucket=prefix, Key='mpu', UploadId=upload, PartNumber=1, Body=b'abcdef')['ETag']
target = urlsplit(s3.generate_presigned_url('put_object', Params={'Bucket': prefix, 'Key': 'base', 'ContentLength': 4}, HttpMethod='PUT', ExpiresIn=120))
connection = http.client.HTTPConnection(target.hostname, target.port, timeout=60)
connection.putrequest('PUT', target.path + '?' + target.query)
connection.putheader('Content-Length', '4')
connection.endheaders()
connection.send(b'pa')
try:
    deadline = time.monotonic() + 15
    while db.execute("SELECT inflight_bytes FROM quota_accounts WHERE kind='project' AND id=%s", (project,)).fetchone()[0] != 10:
        assert time.monotonic() < deadline
        time.sleep(.02)
    marker.write_text('ready')
    print('READY quota restart: accepted multipart plus an interrupted overwrite', flush=True)
    try:
        response = connection.getresponse()
        raise AssertionError('the app was not killed: ' + str(response.status))
    except (ConnectionError, http.client.RemoteDisconnected):
        pass
    deadline = time.monotonic() + 45
    while True:
        try:
            if requests.get(url + '/api/info', timeout=1).status_code == 200:
                break
        except requests.RequestException:
            pass
        assert time.monotonic() < deadline, 'app did not restart'
        time.sleep(.1)
    row = db.execute("SELECT used_bytes,reserved_bytes,inflight_bytes FROM quota_accounts WHERE kind='project' AND id=%s", (project,)).fetchone()
    assert row == (4, 6, 6), row
    assert s3.get_object(Bucket=prefix, Key='base')['Body'].read() == b'base'
    s3.complete_multipart_upload(Bucket=prefix, Key='mpu', UploadId=upload, MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag}]})
    assert s3.get_object(Bucket=prefix, Key='mpu')['Body'].read() == b'abcdef'
    assert db.execute("SELECT used_bytes,reserved_bytes,inflight_bytes FROM quota_accounts WHERE kind='project' AND id=%s", (project,)).fetchone() == (10, 0, 0)
    print('PASS abrupt restart releases incomplete receiver, preserves accepted parts and publishes once', flush=True)
finally:
    connection.close()
    db.close()
