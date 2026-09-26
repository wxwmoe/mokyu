"""Multipart completion quotas in an isolated fault-injection build.

Uses integration.py's environment plus MGW_TEST_BINARY, MGW_TEST_CONFIG,
MGW_TEST_DATABASE_FILE and MGW_TEST_FAULT_DIR. Configure upload_concurrency=4,
read_concurrency=6 and enough inflight_bytes for at least 10 data slots.
"""
import http.client
import json
import os
import socket
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit
from xml.sax.saxutils import escape

import psycopg
import requests
from botocore.exceptions import ClientError
from integration import bucket, s3, read, error

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text(), autocommit=True)
marker = Path(os.environ['MGW_TEST_FAULT_DIR']) / 'multipart-completing'
prefix = 'multipart-admission-' + uuid.uuid4().hex + '/'


def status():
    return json.loads(subprocess.check_output([os.environ['MGW_TEST_BINARY'], '--config',
        os.environ['MGW_TEST_CONFIG'], 'cli', 'status'], text=True, timeout=10))


def wait(predicate):
    deadline = time.monotonic() + 20
    while not predicate():
        assert time.monotonic() < deadline, 'condition timed out'
        time.sleep(.02)


def state(params):
    return db.execute('SELECT state FROM uploads WHERE id=%s', (params['UploadId'],)).fetchone()[0]


def slots(snapshot):
    return tuple(snapshot[name] for name in
                 ('upload_slots_available', 'read_slots_available', 'data_slots_available'))


def assert_slots(used):
    actual = slots(status())
    assert actual == (4 - used, 6, data_slots - used), actual


def released():
    wait(lambda: slots(status()) == (4, 6, data_slots))


def arm():
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()


def prepare(suffix, checksum=False):
    args = {'Bucket': bucket, 'Key': prefix + suffix}
    checks = {'ChecksumAlgorithm': 'CRC32', 'ChecksumType': 'FULL_OBJECT'} if checksum else {}
    args['UploadId'] = s3.create_multipart_upload(**args, **checks)['UploadId']
    data = os.urandom(16384)
    out = s3.upload_part(**args, PartNumber=1, Body=data,
                         **({'ChecksumAlgorithm': 'CRC32'} if checksum else {}))
    part = {'PartNumber': 1, 'ETag': out['ETag']}
    if checksum:
        part['ChecksumCRC32'] = out['ChecksumCRC32']
        args.update(ChecksumCRC32=out['ChecksumCRC32'], ChecksumType='FULL_OBJECT')
    args['MultipartUpload'] = {'Parts': [part]}
    return args, data


def slowed(fn, **args):
    try:
        fn(**args)
    except ClientError as exc:
        assert exc.response['Error']['Code'] == 'SlowDown', exc.response
        assert exc.response['ResponseMetadata']['HTTPStatusCode'] == 503, exc.response
    else:
        raise AssertionError('expected HTTP 503 SlowDown')


initial = status()
assert initial['resources']['upload_concurrency'] == 4
assert initial['resources']['read_concurrency'] == 6
data_slots = initial['resources']['data_slots']
assert data_slots >= 10, 'separate class limits from the shared memory budget'
released()
warm_key, warm = prefix + 'warm', os.urandom(32768)
s3.put_object(Bucket=bucket, Key=warm_key, Body=warm, ACL='public-read')
assert read(warm_key)[0] == warm
uploads = [prepare(str(n)) for n in range(5)]

# All four workers remain paused after the HTTP handler transfers their permits.
arm()
with ThreadPoolExecutor(max_workers=4) as pool:
    pending = [pool.submit(s3.complete_multipart_upload, **args) for args, _ in uploads[:4]]
    try:
        wait(lambda: marker.with_suffix('.hit').exists()
             and all(state(args) == 'completing' for args, _ in uploads[:4]))
        assert all(not future.done() for future in pending)
        assert_slots(4)
        fifth, _ = uploads[4]
        slowed(s3.complete_multipart_upload, **fifth)
        slowed(s3.put_object, Bucket=bucket, Key=prefix + 'blocked', Body=b'blocked')
        slowed(s3.upload_part, Bucket=bucket, Key=fifth['Key'], UploadId=fifth['UploadId'],
               PartNumber=2, Body=b'blocked')
        assert state(fifth) == 'active'
        before = status()['backend_gets']
        assert read(warm_key)[0] == warm
        body, response = read(warm_key, Range='bytes=7-1030')
        assert body == warm[7:1031] and response['ResponseMetadata']['HTTPStatusCode'] == 206
        for headers, code, expected in [({}, 200, warm), ({'Range': 'bytes=7-1030'}, 206, warm[7:1031])]:
            response = requests.get(os.environ['MGW_TEST_PUBLIC'] + '/' + warm_key,
                headers={'Host': os.environ.get('MGW_TEST_PUBLIC_HOST', 'media.test'), **headers}, timeout=10)
            assert response.status_code == code and response.content == expected
        wait(lambda: slots(status()) == (0, 6, data_slots - 4))
        assert status()['backend_gets'] == before, 'warm GET/Range should stay in cache'
    finally:
        marker.unlink(missing_ok=True)
    for future in pending:
        assert future.result(timeout=20)['ResponseMetadata']['HTTPStatusCode'] == 200
released()
s3.complete_multipart_upload(**uploads[4][0])
released()
for args, data in uploads:
    assert read(args['Key'])[0] == data
print('PASS four completions fill upload quota; excess Complete/PUT/part return 503; cached S3/public GET and Range remain available', flush=True)

# A checksum mismatch is detected inside the background worker, after admission.
args, data = prepare('bad-checksum', checksum=True)
wrong = 'AAAAAA==' if args['ChecksumCRC32'] != 'AAAAAA==' else 'AQAAAA=='
arm()
with ThreadPoolExecutor(max_workers=1) as pool:
    pending = pool.submit(s3.complete_multipart_upload, **{**args, 'ChecksumCRC32': wrong})
    try:
        wait(lambda: marker.with_suffix('.hit').exists() and state(args) == 'completing')
        assert_slots(1)
    finally:
        marker.unlink(missing_ok=True)
    error('BadDigest', pending.result, timeout=20)
wait(lambda: state(args) == 'active')
released()
s3.complete_multipart_upload(**args)
released()
assert read(args['Key'])[0] == data
print('PASS failed merge releases upload/data permits, restores active state and permits retry', flush=True)

# Disconnect the real HTTP client while its detached merge is still paused.
args, data = prepare('disconnect')
url = urlsplit(s3.generate_presigned_url('complete_multipart_upload',
    Params={name: args[name] for name in ('Bucket', 'Key', 'UploadId')}, HttpMethod='POST', ExpiresIn=60))
etag = escape(args['MultipartUpload']['Parts'][0]['ETag'])
body = f'<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>'
connection = (http.client.HTTPSConnection if url.scheme == 'https' else http.client.HTTPConnection)(
    url.hostname, url.port, timeout=10)
canceled = status()['runtime']['http']['s3']['canceled']
arm()
try:
    connection.request('POST', url.path + '?' + url.query, body=body,
                       headers={'Content-Type': 'application/xml'})
    wait(lambda: marker.with_suffix('.hit').exists() and state(args) == 'completing')
    assert_slots(1)
    connection.sock.shutdown(socket.SHUT_RDWR)
    connection.close()
    wait(lambda: status()['runtime']['http']['s3']['canceled'] > canceled)
    assert state(args) == 'completing'
    assert_slots(1)
finally:
    connection.close()
    marker.unlink(missing_ok=True)
wait(lambda: state(args) == 'completed')
released()
assert read(args['Key'])[0] == data
released()
db.close()
print('PASS disconnected client leaves merge/permits alive until completion, then releases both quotas', flush=True)
