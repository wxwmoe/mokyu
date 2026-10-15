"""Multipart completion quotas in an isolated fault-injection build.

Uses integration.py's environment plus MOKYU_TEST_BINARY, MOKYU_TEST_CONFIG,
MOKYU_TEST_DATABASE_FILE and MOKYU_TEST_FAULT_DIR. The derived data budget must
leave read capacity after every upload slot is occupied. The test bucket must use
a cache-enabled S3 storage with a short upload flush timeout.
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
from tests.integration import bucket, s3, read, error

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'multipart-completing'
prefix = 'multipart-admission-' + uuid.uuid4().hex + '/'


def status():
    return json.loads(subprocess.check_output([os.environ['MOKYU_TEST_BINARY'], '--config',
        os.environ['MOKYU_TEST_CONFIG'], 'cli', 'status'], text=True, timeout=10))


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
    assert actual == (upload_slots - used, read_slots, data_slots - used), actual


def released():
    wait(lambda: slots(status()) == (upload_slots, read_slots, data_slots))


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
upload_slots = initial['resources']['upload_concurrency']
read_slots = initial['resources']['read_concurrency']
assert upload_slots > 0 and read_slots > 0
data_slots = initial['resources']['data_slots']
assert data_slots > upload_slots, 'separate upload limits from the shared memory budget'
released()
warm_key, warm = prefix + 'warm', os.urandom(32768)
s3.put_object(Bucket=bucket, Key=warm_key, Body=warm, ACL='public-read')
# Wait for publication to settle before asserting cache hits across a held completion.
wait(lambda: not db.execute("SELECT EXISTS(SELECT 1 FROM pending_uploads p JOIN extents e ON e.chunk_id=p.chunk_id JOIN objects o ON o.stream_id=e.stream_id JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s)", (bucket, warm_key)).fetchone()[0])
def warmed():
    before = status()
    assert read(warm_key)[0] == warm
    after = status()
    return after['cache_hits'] > before['cache_hits'] and after['backend_gets'] == before['backend_gets']
wait(warmed)
uploads = [prepare(str(n)) for n in range(upload_slots + 1)]

# All upload workers remain paused after the HTTP handler transfers their permits.
arm()
with ThreadPoolExecutor(max_workers=upload_slots) as pool:
    pending = [pool.submit(s3.complete_multipart_upload, **args) for args, _ in uploads[:upload_slots]]
    try:
        wait(lambda: marker.with_suffix('.hit').exists()
             and all(state(args) == 'completing' for args, _ in uploads[:upload_slots]))
        assert all(not future.done() for future in pending)
        assert_slots(upload_slots)
        excess, _ = uploads[upload_slots]
        slowed(s3.complete_multipart_upload, **excess)
        slowed(s3.put_object, Bucket=bucket, Key=prefix + 'blocked', Body=b'blocked')
        slowed(s3.upload_part, Bucket=bucket, Key=excess['Key'], UploadId=excess['UploadId'],
               PartNumber=2, Body=b'blocked')
        assert state(excess) == 'active'
        before = status()['backend_gets']
        assert read(warm_key)[0] == warm
        body, response = read(warm_key, Range='bytes=7-1030')
        assert body == warm[7:1031] and response['ResponseMetadata']['HTTPStatusCode'] == 206
        for headers, code, expected in [({}, 200, warm), ({'Range': 'bytes=7-1030'}, 206, warm[7:1031])]:
            response = requests.get(os.environ['MOKYU_TEST_PUBLIC'] + '/' + warm_key,
                headers={'Host': os.environ.get('MOKYU_TEST_PUBLIC_HOST', 'media.test'), **headers}, timeout=10)
            assert response.status_code == code and response.content == expected
        wait(lambda: slots(status()) == (0, read_slots, data_slots - upload_slots))
        assert status()['backend_gets'] == before, ('warm GET/Range should stay in cache', before, status()['backend_gets'])
    finally:
        marker.unlink(missing_ok=True)
    for future in pending:
        assert future.result(timeout=20)['ResponseMetadata']['HTTPStatusCode'] == 200
released()
s3.complete_multipart_upload(**uploads[upload_slots][0])
released()
for args, data in uploads:
    assert read(args['Key'])[0] == data
print('PASS completions fill upload quota; excess Complete/PUT/part return 503; cached S3/public GET and Range remain available', flush=True)

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
