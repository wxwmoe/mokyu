"""Read paging and cache-lock regression checks in an isolated deployment.

Uses the integration/state_checks environment. PostgreSQL must preload
pg_stat_statements. Cache checks additionally require a fault-injection build,
MEDIA_GATEWAY_TEST_FAULT_DIR in the gateway, and MGW_TEST_FAULT_DIR here pointing
at the same directory. Never use a deployment containing user data.
"""
import json
import math
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MGW_TEST_CREDENTIALS']).read_text())
bucket, endpoint = credential['bucket'], os.environ['MGW_TEST_ENDPOINT']
db = psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text(), autocommit=True)
db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
prefix = 'read-path-' + os.urandom(4).hex() + '/'


def put(key, data):
    s3.put_object(Bucket=bucket, Key=prefix + key, Body=data, ACL='public-read')


def read(key, headers=None, timeout=30):
    response = requests.get(f'{endpoint}/{bucket}/{prefix}{key}', headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.content


def status():
    return json.loads(subprocess.check_output([os.environ['MGW_TEST_BINARY'], '--config',
        os.environ['MGW_TEST_CONFIG'], 'cli', 'status'], text=True))


def mapping_calls():
    prefixes = ('SELECT * FROM chunks WHERE id=', 'SELECT * FROM extents WHERE stream_id=',
                'SELECT e.*,c.* FROM (SELECT * FROM extents', 'SELECT offset_bytes FROM extents')
    return sum(calls for query, calls in db.execute('''SELECT query,calls FROM pg_stat_statements
        WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())''')
               if query.startswith(prefixes))


def evict(key):
    rows = db.execute('''SELECT c.storage_id FROM objects o JOIN buckets b ON b.id=o.bucket_id
        JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id
        WHERE b.name=%s AND o.key=%s''', (bucket, prefix + key)).fetchall()
    for (storage_id,) in rows:
        name = storage_id.hex
        for path in (Path(os.environ['MGW_TEST_DATA']) / 'chunks' / name[:2]).glob(name + '.*'):
            path.unlink()


payload = os.urandom(128 * 1024 * 1024)
put('large', payload)
before = status()['backend_gets']
assert read('large') == payload
assert status()['backend_gets'] == before
print('PASS newly uploaded chunks serve the first read without backend GETs', flush=True)
evict('large')
stream = db.execute('''SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id
    WHERE b.name=%s AND o.key=%s''', (bucket, prefix + 'large')).fetchone()[0]
extents = db.execute('SELECT offset_bytes,length FROM extents WHERE stream_id=%s ORDER BY offset_bytes',
                     (stream,)).fetchall()
assert len(extents) > 64, len(extents)
before = mapping_calls()
assert read('large') == payload
assert mapping_calls() - before == math.ceil(len(extents) / 64) + 1
print('PASS one metadata query per 64 mappings plus initial Range seek', flush=True)

for start, end in [(0, 1), (len(payload) - 37, len(payload)),
                   (extents[63][0] + 7, extents[65][0] + 13)]:
    before = mapping_calls()
    assert read('large', {'Range': f'bytes={start}-{end - 1}'}) == payload[start:end]
    assert mapping_calls() - before == 2
assert read('large', {'Range': 'bytes=-71'}) == payload[-71:]
assert read('large') == payload
del payload
print('PASS cold/warm full reads and Range across page boundaries', flush=True)

upload = s3.create_multipart_upload(Bucket=bucket, Key=prefix + 'multipart', ACL='public-read')['UploadId']
parts, bodies = [], [os.urandom(6 * 1024 * 1024), os.urandom(320 * 1024)]
for number, data in enumerate(bodies, 1):
    result = s3.upload_part(Bucket=bucket, Key=prefix + 'multipart', UploadId=upload, PartNumber=number, Body=data)
    parts.append({'PartNumber': number, 'ETag': result['ETag']})
s3.complete_multipart_upload(Bucket=bucket, Key=prefix + 'multipart', UploadId=upload, MultipartUpload={'Parts': parts})
before = status()['backend_gets']
assert read('multipart') == b''.join(bodies)
assert status()['backend_gets'] == before
print('PASS completed multipart first read uses locally cached final chunks', flush=True)

# Published extents can refer to a subrange of a physical chunk.
raw = os.urandom(65536)
put('slice', raw)
stream = db.execute('''SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id
    WHERE b.name=%s AND o.key=%s''', (bucket, prefix + 'slice')).fetchone()[0]
with db.transaction():
    db.execute('UPDATE extents SET source_offset=17,length=%s WHERE stream_id=%s', (len(raw) - 34, stream))
    db.execute('UPDATE streams SET size=%s WHERE id=%s', (len(raw) - 34, stream))
assert read('slice') == raw[17:-17]
assert read('slice', {'Range': 'bytes=9-29'}) == raw[26:47]
chunk = db.execute('SELECT chunk_id FROM extents WHERE stream_id=%s', (stream,)).fetchone()[0]
try:
    db.execute("UPDATE chunks SET state='failed' WHERE id=%s", (chunk,))
    try:
        read('slice')
    except requests.RequestException:
        pass
    else:
        raise AssertionError('must reject unavailable chunk even with a valid cached copy')
finally:
    db.execute("UPDATE chunks SET state='ready' WHERE id=%s", (chunk,))
assert read('slice') == raw[17:-17]
print('PASS source offsets and fail closed on missing ready metadata', flush=True)

faults = Path(os.environ['MGW_TEST_FAULT_DIR'])
faults.mkdir(parents=True, exist_ok=True)
put('warm', raw)
assert read('warm') == raw
for boundary in ['cache-reserved', 'cache-written', 'cache-published']:
    cold = os.urandom(65536)
    put(boundary, cold)
    evict(boundary)
    marker = faults / boundary
    reached = marker.with_suffix('.hit')
    reached.unlink(missing_ok=True)
    marker.touch()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(read, boundary)
        try:
            deadline = time.monotonic() + 15
            while not reached.exists():
                assert time.monotonic() < deadline, f'{boundary} not reached'
                time.sleep(.02)
            assert not pending.done()
            assert read('warm', timeout=3) == raw
            assert marker.exists(), 'warm hit must finish while the write remains paused'
        finally:
            marker.unlink(missing_ok=True)
        assert pending.result(timeout=15) == cold
    assert read(boundary) == cold
    reached.unlink(missing_ok=True)
cache = Path(os.environ['MGW_TEST_DATA']) / 'chunks'
assert not list(cache.glob('*/*.tmp'))
assert status()['local_bytes'][1] == sum(p.stat().st_size for p in cache.glob('*/*') if p.is_file())
print('PASS warm reads during every cache write boundary and exact disk accounting', flush=True)

db.close()
