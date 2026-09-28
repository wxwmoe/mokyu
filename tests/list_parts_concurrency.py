"""ListParts snapshots during replacement/cleanup in an isolated fault-injection build.

Uses the MOKYU_TEST_* environment from cleanup_concurrency.py. Requires boto3 and
psycopg; creates its own uploads in the configured disposable test bucket.
"""
import base64
import hashlib
import json
import os
import subprocess
import time
import uuid
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credentials = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credentials['bucket']
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                  config=Config(retries={'max_attempts': 0}, read_timeout=30,
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
key = 'list-parts-' + uuid.uuid4().hex
upload = s3.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm='SHA256', ChecksumType='COMPOSITE')['UploadId']
args = {'Bucket': bucket, 'Key': key, 'UploadId': upload}


def scalar(sql, values=()):
    return db.execute(sql, values or None).fetchone()[0]


def wait(predicate):
    deadline = time.monotonic() + 20
    while not predicate():
        assert time.monotonic() < deadline, 'condition timed out'
        time.sleep(.02)


def gc():
    subprocess.run([os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'],
                    'cli', 'gc', 'run'], check=True, capture_output=True, timeout=10)


def arm():
    marker = faults / 'list-parts-read'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    return marker


def error(code, **params):
    try:
        s3.list_parts(**args, **params)
    except ClientError as exc:
        assert exc.response['Error']['Code'] == code, exc.response
    else:
        raise AssertionError('expected ' + code)


empty = s3.list_parts(**args)
assert not empty.get('Parts') and not empty['IsTruncated']
for number in range(1, 1001):
    s3.upload_part(**args, PartNumber=number, Body=b'', ChecksumAlgorithm='SHA256')
snapshot = s3.list_parts(**args)
assert not snapshot['IsTruncated'] and len(snapshot['Parts']) == 1000
assert snapshot['ChecksumAlgorithm'] == 'SHA256' and snapshot['ChecksumType'] == 'COMPOSITE'
assert [p['PartNumber'] for p in snapshot['Parts']] == list(range(1, 1001))
assert all(p['Size'] == 0 and p['ChecksumSHA256'] == base64.b64encode(hashlib.sha256(b'').digest()).decode()
           and p['ETag'] == '"' + hashlib.md5(b'').hexdigest() + '"' and p['LastModified']
           for p in snapshot['Parts'])

# Stop after rows are read, then actually remove the old stream before building XML.
old = scalar('SELECT stream_id FROM parts WHERE upload_id=%s AND part_number=1000', (upload,))
marker = arm()
with ThreadPoolExecutor(max_workers=1) as pool:
    listing = pool.submit(s3.list_parts, **args)
    try:
        wait(lambda: marker.with_suffix('.hit').exists())
        replaced = s3.upload_part(**args, PartNumber=1000, Body=b'replacement', ChecksumAlgorithm='SHA256')
        gc()
        wait(lambda: scalar('SELECT count(*) FROM streams WHERE id=%s', (old,)) == 0)
        assert not listing.done()
    finally:
        marker.unlink(missing_ok=True)
    result = listing.result(timeout=10)
assert result['ResponseMetadata']['HTTPStatusCode'] == 200
assert result['Parts'] == snapshot['Parts'], 'page changed after its rows were read'
updated = s3.list_parts(**args, PartNumberMarker=999)['Parts'][0]
assert updated['Size'] == len(b'replacement') and updated['ETag'] == replaced['ETag']
assert updated['ChecksumSHA256'] == replaced['ChecksumSHA256']
print('PASS 1000-part snapshot survives replacement and actual old-stream cleanup', flush=True)

s3.upload_part(**args, PartNumber=1001, Body=b'', ChecksumAlgorithm='SHA256')
touched = scalar('SELECT touched_at FROM uploads WHERE id=%s', (upload,))
page = s3.list_parts(**args, MaxParts=1000)
assert len(page['Parts']) == 1000 and page['IsTruncated'] and page['NextPartNumberMarker'] == 1000
tail = s3.list_parts(**args, PartNumberMarker=page['NextPartNumberMarker'], MaxParts=1000)
assert [p['PartNumber'] for p in tail['Parts']] == [1001] and not tail['IsTruncated']
assert not s3.list_parts(**args, PartNumberMarker=10000).get('Parts')
zero = s3.list_parts(**args, MaxParts=0)
assert not zero.get('Parts') and zero['IsTruncated'] and zero['NextPartNumberMarker'] == 0
single = s3.list_parts(**args, MaxParts=1, PartNumberMarker=999)
assert single['Parts'] == [updated] and single['IsTruncated'] and single['NextPartNumberMarker'] == 1000
error('InvalidArgument', MaxParts=1001)
error('InvalidArgument', PartNumberMarker=10001)
assert scalar('SELECT touched_at FROM uploads WHERE id=%s', (upload,)) == touched
print('PASS empty/zero/one/full pages, marker boundaries, lookahead and unchanged upload expiry', flush=True)

# An in-flight page remains valid if abort removes all mappings after that query.
marker = arm()
with ThreadPoolExecutor(max_workers=1) as pool:
    listing = pool.submit(s3.list_parts, **args, PartNumberMarker=999)
    try:
        wait(lambda: marker.with_suffix('.hit').exists())
        s3.abort_multipart_upload(**args)
        gc()
        assert scalar('SELECT count(*) FROM parts WHERE upload_id=%s', (upload,)) == 0
    finally:
        marker.unlink(missing_ok=True)
    assert [p['PartNumber'] for p in listing.result(timeout=10)['Parts']] == [1000, 1001]
error('NoSuchUpload')
print('PASS captured page survives concurrent abort; subsequent listing returns NoSuchUpload', flush=True)

for algorithm in ('CRC32', 'SHA1'):
    check_args = {'Bucket': bucket, 'Key': key + algorithm}
    check_args['UploadId'] = s3.create_multipart_upload(**check_args, ChecksumAlgorithm=algorithm,
                                                       ChecksumType='COMPOSITE')['UploadId']
    body = b'checksum listing'
    out = s3.upload_part(**check_args, PartNumber=7, Body=body, ChecksumAlgorithm=algorithm)
    value = zlib.crc32(body).to_bytes(4, 'big') if algorithm == 'CRC32' else hashlib.sha1(body).digest()
    expected = base64.b64encode(value).decode()
    listed = s3.list_parts(**check_args, PartNumberMarker=2)
    assert listed['ChecksumAlgorithm'] == algorithm and listed['ChecksumType'] == 'COMPOSITE'
    assert len(listed['Parts']) == 1 and listed['Parts'][0]['PartNumber'] == 7
    assert listed['Parts'][0]['Checksum' + algorithm] == out['Checksum' + algorithm] == expected
    s3.abort_multipart_upload(**check_args)
print('PASS sparse part numbers and CRC32/SHA1/SHA256 metadata', flush=True)
db.close()
