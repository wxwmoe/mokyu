"""Statistics / RequestId contracts against a fresh isolated test bucket.

Uses the integration.py environment plus MGW_TEST_WEB, MGW_TEST_PASSWORD and
MGW_TEST_DATABASE_FILE. Configure statistics.refresh_interval=2s for this test.
The lock test deliberately delays an inventory scan; never use a real deployment.
"""
import json
import hashlib
import os
import time
import uuid
from pathlib import Path
from xml.etree import ElementTree
from urllib.parse import quote

import boto3
import psycopg
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MGW_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
endpoint, web = os.environ['MGW_TEST_ENDPOINT'], os.environ['MGW_TEST_WEB']
db_url = Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text()
session = requests.Session()
session.headers['Origin'] = web
login = session.post(web + '/api/login', json={'username': 'tester', 'password': os.environ['MGW_TEST_PASSWORD']})
login.raise_for_status()
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))


def status():
    response = session.get(web + '/api/status', timeout=10)
    response.raise_for_status()
    return response.json()


def wait(predicate):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        value = status()
        if predicate(value):
            return value
        time.sleep(.2)
    raise AssertionError(value)


ids = set()
def request_id(response, s3_response=False):
    value = response.headers['x-request-id']
    assert uuid.UUID(value).version == 4
    assert value not in ids
    ids.add(value)
    if s3_response:
        assert response.headers['x-amz-request-id'] == value
    return value


before = wait(lambda v: v['storage']['snapshot'] is not None)
assert before['version'] == '0.0.2'
assert before['storage']['snapshot']['objects'] == 0, 'requires a fresh test bucket/database'
assert before['process_memory']['rss_bytes'] > 0
assert requests.get(web + '/api/status').status_code == 403
raw, compressed = os.urandom(512 * 1024), b'Z' * (768 * 1024)
for name, data in [('one', raw), ('duplicate', raw), ('compressed', compressed)]:
    response = s3.put_object(Bucket=bucket, Key='statistics/' + name, Body=data, ACL='public-read')
    rid = response['ResponseMetadata']['RequestId']
    assert uuid.UUID(rid).version == 4
    assert response['ResponseMetadata']['HTTPHeaders']['x-request-id'] == rid
    assert rid not in ids
    ids.add(rid)
snapshot = wait(lambda v: v['storage']['snapshot']['objects'] == 3)['storage']['snapshot']
assert snapshot['logical_bytes'] == 2 * len(raw) + len(compressed)
assert snapshot['live']['reference_bytes'] == snapshot['logical_bytes']
assert snapshot['live']['raw_bytes'] == len(raw) + len(compressed)
assert snapshot['live']['payload_bytes'] < snapshot['live']['raw_bytes']
assert snapshot['chunks']['stored_bytes'] == snapshot['live']['stored_bytes']
assert snapshot['buckets'][0]['objects'] == 3
assert snapshot['buckets'][0]['logical_bytes'] == snapshot['logical_bytes']

# Uploads now populate the cache; remove one test file to exercise backend GET accounting.
with psycopg.connect(db_url) as db:
    storage_id = db.execute('''SELECT c.storage_id FROM objects o JOIN buckets b ON b.id=o.bucket_id
        JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id
        WHERE b.name=%s AND o.key='statistics/one' ''', (bucket,)).fetchone()[0].hex
for path in (Path(os.environ['MGW_TEST_DATA']) / 'chunks' / storage_id[:2]).glob(storage_id + '.*'):
    path.unlink()

# A complete stream, Range, HEAD and an empty object must never count as canceled.
initial = status()['runtime']['http']['s3']
for _ in range(2):
    result = s3.get_object(Bucket=bucket, Key='statistics/one')
    with result['Body'] as body:
        assert body.read() == raw
result = s3.get_object(Bucket=bucket, Key='statistics/one', Range='bytes=7-23')
with result['Body'] as body:
    assert body.read() == raw[7:24]
s3.head_object(Bucket=bucket, Key='statistics/one')
s3.put_object(Bucket=bucket, Key='statistics/empty', Body=b'')
result = s3.get_object(Bucket=bucket, Key='statistics/empty')
with result['Body'] as body:
    assert body.read() == b''
after = wait(lambda v: v['runtime']['http']['s3']['active'] == 0)
assert after['runtime']['http']['s3']['canceled'] == initial['canceled'], after
assert after['io']['cache_lookups'] >= 3
assert 0 < after['io']['cache_hit_rate'] <= 1
assert after['io']['backend']['get']['completed'] >= 1

for method in ('GET', 'HEAD', 'OPTIONS'):
    response = requests.request(method, endpoint + '/' + bucket + '/statistics/missing',
                                headers={'X-Request-ID': 'untrusted-client-id'})
    rid = request_id(response, True)
    if method == 'GET':
        assert response.status_code in (403, 404)
        assert ElementTree.fromstring(response.content).findtext('RequestId') == rid
for port in [web, os.environ['MGW_TEST_PUBLIC']]:
    response = requests.get(port + '/missing', headers={'Host': 'media.test', 'X-Request-ID': 'untrusted'})
    request_id(response)
presigned = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': 'statistics/one'})
response = requests.get(presigned.replace('X-Amz-Signature=', 'X-Amz-Signature=0'), timeout=10)
rid = request_id(response, True)
assert response.status_code == 403
assert ElementTree.fromstring(response.content).findtext('RequestId') == rid
try:
    s3.get_object(Bucket=bucket, Key='statistics/missing')
    raise AssertionError('missing object succeeded')
except ClientError as error:
    assert error.response['ResponseMetadata']['RequestId']

# A scan failure retains the prior consistent snapshot, and status remains quick.
saved = status()['storage']['snapshot']
with psycopg.connect(db_url) as db:
    db.execute('LOCK TABLE chunks IN ACCESS EXCLUSIVE MODE')
    failed = wait(lambda v: v['storage']['last_error'] is not None)
    assert failed['storage']['snapshot'] == saved or failed['storage']['snapshot']['objects'] == 4
    assert failed['storage']['stale']
    started = time.monotonic()
    for _ in range(10):
        assert status()['storage']['last_error'] == 'refresh_failed'
    assert time.monotonic() - started < 4
wait(lambda v: v['storage']['last_error'] is None and not v['storage']['stale'])

# Uncompleted multipart data is retained physical storage, not visible logical data.
upload = s3.create_multipart_upload(Bucket=bucket, Key='statistics/incomplete')['UploadId']
s3.upload_part(Bucket=bucket, Key='statistics/incomplete', UploadId=upload, PartNumber=1, Body=os.urandom(6 * 1024 * 1024))
multipart = wait(lambda v: v['storage']['snapshot']['uploads'].get('active') == 1)['storage']['snapshot']
assert multipart['objects'] == 4
assert multipart['chunks']['stored_bytes'] > multipart['live']['stored_bytes']
s3.abort_multipart_upload(Bucket=bucket, Key='statistics/incomplete', UploadId=upload)

# CompleteMultipartUpload can return HTTP 200 then report failure inside streamed XML.
upload = s3.create_multipart_upload(Bucket=bucket, Key='statistics/bad-complete',
                                    ChecksumAlgorithm='CRC32', ChecksumType='FULL_OBJECT')['UploadId']
part = s3.upload_part(Bucket=bucket, Key='statistics/bad-complete', UploadId=upload,
                       PartNumber=1, Body=b'completion checksum mismatch', ChecksumAlgorithm='CRC32')
xml = f'<CompleteMultipartUpload><Part><ETag>{part["ETag"]}</ETag><PartNumber>1</PartNumber><ChecksumCRC32>{part["ChecksumCRC32"]}</ChecksumCRC32></Part></CompleteMultipartUpload>'.encode()
target = endpoint + '/' + bucket + '/statistics/bad-complete?uploadId=' + quote(upload)
request = AWSRequest(method='POST', url=target, data=xml, headers={
    'Content-Type': 'application/xml', 'x-amz-content-sha256': hashlib.sha256(xml).hexdigest(),
    'x-amz-checksum-crc32': 'AAAAAA==', 'x-amz-checksum-type': 'FULL_OBJECT'})
SigV4Auth(Credentials(credential['access_key'], credential['secret_key']), 's3', 'us-east-1').add_auth(request)
failed_before = status()['runtime']['http']['s3']['failed']
response = requests.post(target, headers=dict(request.headers), data=xml, timeout=20)
assert response.status_code == 200, response.content
rid = request_id(response, True)
error = ElementTree.fromstring(response.content)
assert error.findtext('Code') == 'BadDigest', response.content
assert error.findtext('RequestId') == rid, response.content
wait(lambda v: v['runtime']['http']['s3']['failed'] > failed_before)
s3.abort_multipart_upload(Bucket=bucket, Key='statistics/bad-complete', UploadId=upload)

# Real client disconnect after response headers must not appear as a successful GET.
data = os.urandom(4 * 1024 * 1024) * 8
s3.put_object(Bucket=bucket, Key='statistics/large', Body=data, ACL='public-read')
previous = status()['runtime']['http']['web']['canceled']
with requests.get(os.environ['MGW_TEST_PUBLIC'] + '/statistics/large', headers={'Host': 'media.test'}, stream=True) as response:
    response.raise_for_status()
    request_id(response)
    response.raw.read(128)
wait(lambda v: v['runtime']['http']['web']['canceled'] > previous)

for name in ('one', 'duplicate', 'compressed', 'empty', 'large'):
    s3.delete_object(Bucket=bucket, Key='statistics/' + name)
deleted = wait(lambda v: v['storage']['snapshot']['objects'] == 0 and v['storage']['snapshot']['unreferenced']['chunks'] > 0)
assert deleted['storage']['snapshot']['live']['raw_bytes'] == 0
assert deleted['storage']['snapshot']['chunks']['stored_bytes'] > 0
print('PASS statistics: snapshots, shared chunks, multipart, failures, stream cancellation, request IDs')
