"""Read paging and cache-lock regression checks in an isolated deployment.

Uses the integration/state_checks environment. PostgreSQL must preload
pg_stat_statements. Cache checks additionally require a fault-injection build,
MOKYU_TEST_FAULT_DIR in the gateway, and MOKYU_TEST_FAULT_DIR here pointing
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

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket, endpoint = credential['bucket'], os.environ['MOKYU_TEST_ENDPOINT']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
prefix = 'read-path-' + os.urandom(4).hex() + '/'


def put(key, data):
    s3.put_object(Bucket=bucket, Key=prefix + key, Body=data, ACL='public-read')


def read(key, headers=None, timeout=30):
    # This suite exercises the physical cache; complete response reuse has its own contract.
    response = requests.get(f'{endpoint}/{bucket}/{prefix}{key}', headers={'Cache-Control': 'no-store', **(headers or {})}, timeout=timeout)
    response.raise_for_status()
    return response.content


def status():
    if url := os.environ.get('MOKYU_TEST_STATUS_URL'):
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    return json.loads(subprocess.check_output([os.environ['MOKYU_TEST_BINARY'], '--config',
        os.environ['MOKYU_TEST_CONFIG'], 'cli', 'status'], text=True))


def mapping_calls():
    prefixes = ('SELECT * FROM chunks WHERE id=', 'SELECT * FROM extents WHERE stream_id=',
                'SELECT e.*,c.id,', 'SELECT offset_bytes FROM extents')
    return sum(calls for query, calls in db.execute('''SELECT query,calls FROM pg_stat_statements
        WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())''')
               if query.startswith(prefixes))


def cache_paths(key):
    rows = db.execute('''SELECT l.encoding_id FROM objects o JOIN buckets b ON b.id=o.bucket_id
        JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id
        JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id
        WHERE b.name=%s AND o.object_key=%s''', (bucket, prefix + key)).fetchall()
    return [path for (location,) in rows
            for path in (Path(os.environ['MOKYU_TEST_DATA']) / 'chunks' / location.hex[-2:]).glob(location.hex + '.*')]


def cache_idle():
    deadline = time.monotonic() + 15
    while status()['cache_jobs_active']:
        assert time.monotonic() < deadline, 'cache work did not finish'
        time.sleep(.02)


def evict(key):
    cache_idle()
    for path in cache_paths(key):
        path.unlink()


payload = os.urandom(128 * 1024 * 1024)
put('large', payload)
before = status()['backend_gets']
assert read('large') == payload
assert status()['backend_gets'] == before
print('PASS newly uploaded chunks serve the first read without backend GETs', flush=True)
evict('large')
stream = db.execute('''SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id
    WHERE b.name=%s AND o.object_key=%s''', (bucket, prefix + 'large')).fetchone()[0]
extents = db.execute('SELECT offset_bytes,length FROM extents WHERE stream_id=%s ORDER BY offset_bytes',
                     (stream,)).fetchall()
assert len(extents) > 64, len(extents)
before = mapping_calls()
assert read('large') == payload
assert mapping_calls() - before == math.ceil(len(extents) / 64)
print('PASS one metadata query per 64 mappings without a full-read seek', flush=True)

for start, end in [(0, 1), (len(payload) - 37, len(payload)),
                   (extents[63][0] + 7, extents[65][0] + 13)]:
    before = mapping_calls()
    assert read('large', {'Range': f'bytes={start}-{end - 1}'}) == payload[start:end]
    assert mapping_calls() - before == (1 if start == 0 else 2)
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
    WHERE b.name=%s AND o.object_key=%s''', (bucket, prefix + 'slice')).fetchone()[0]
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

faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
faults.mkdir(parents=True, exist_ok=True)
put('warm', raw)
assert read('warm') == raw
for boundary in ['cache-reserved', 'cache-written', 'cache-published']:
    cache_idle()
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
            assert pending.result(timeout=3) == cold, 'verified read waited for cache publication'
            assert read('warm', timeout=3) == raw
            assert marker.exists(), 'warm hit must finish while the write remains paused'
        finally:
            marker.unlink(missing_ok=True)
        assert pending.result(timeout=15) == cold
    assert read(boundary) == cold
    cache_idle()
    reached.unlink(missing_ok=True)

put('corrupt', raw[::-1])
cache_idle()
path, = cache_paths('corrupt')
changed = bytearray(path.read_bytes())
changed[len(changed) // 2] ^= 1
path.write_bytes(changed)
marker = faults / 'cache-delete'
reached = marker.with_suffix('.hit')
reached.unlink(missing_ok=True)
marker.touch()
try:
    assert read('corrupt', timeout=3) == raw[::-1]
    deadline = time.monotonic() + 10
    while not reached.exists():
        assert time.monotonic() < deadline, 'cache cleanup did not reach barrier'
        time.sleep(.02)
    for _ in range(3):
        assert read('warm', timeout=3) == raw
        assert read('corrupt', timeout=3) == raw[::-1]
    assert status()['cache_jobs_active'] == 1, 'duplicate cleanup jobs were not coalesced'
finally:
    marker.unlink(missing_ok=True)
cache_idle()
assert read('corrupt') == raw[::-1]
cache_idle()
reached.unlink(missing_ok=True)
cache = Path(os.environ['MOKYU_TEST_DATA']) / 'chunks'
assert not list(cache.glob('*/*.tmp'))
assert status()['local_bytes'][1] == sum(p.stat().st_size for p in cache.glob('*/*') if p.is_file())
print('PASS nonblocking cache fills/cleanup, coalesced jobs, warm reads and exact disk accounting', flush=True)

# Metadata-only responses must still work when every body slot is occupied.
etag = s3.head_object(Bucket=bucket, Key=prefix + 'warm')['ETag']
urls = [(f'{endpoint}/{bucket}/{prefix}warm', {})]
if public := os.environ.get('MOKYU_TEST_PUBLIC_WEB'):
    urls.append((public + '/' + prefix + 'warm', {'Host': os.environ['MOKYU_TEST_PUBLIC_HOST']}))
marker = faults / 'read-body'
marker.with_suffix('.hit').unlink(missing_ok=True)
marker.touch()
slots = status()['read_slots_available']
with ThreadPoolExecutor(max_workers=slots) as pool:
    pending = [pool.submit(read, 'warm') for _ in range(slots)]
    try:
        deadline = time.monotonic() + 10
        while status()['read_slots_available']:
            assert time.monotonic() < deadline, 'read slots were not occupied'
            time.sleep(.02)
        before, backend = mapping_calls(), status()['backend_gets']
        for url, extra in urls:
            response = requests.head(url, headers=extra, timeout=3)
            assert response.status_code == 200 and not response.content
            assert int(response.headers['Content-Length']) == len(raw)
            response = requests.get(url, headers={**extra, 'If-None-Match': etag}, timeout=3)
            assert response.status_code == 304 and not response.content
        assert mapping_calls() == before
        assert status()['backend_gets'] == backend
    finally:
        marker.unlink(missing_ok=True)
    assert all(result.result(timeout=15) == raw for result in pending)
marker.with_suffix('.hit').unlink(missing_ok=True)
print('PASS HEAD/304 under saturated read slots without extent or backend reads', flush=True)

for url, extra in urls:
    for change in ('acl', 'overwrite'):
        marker = faults / 'read-metadata'
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(requests.get, url, headers={**extra, 'If-Match': etag}, timeout=15)
            try:
                deadline = time.monotonic() + 10
                while not marker.with_suffix('.hit').exists():
                    assert time.monotonic() < deadline, 'conditional read missed metadata barrier'
                    time.sleep(.02)
                if change == 'acl':
                    s3.put_object_acl(Bucket=bucket, Key=prefix + 'warm', ACL='private')
                else:
                    put('warm', raw[::-1])
            finally:
                marker.unlink(missing_ok=True)
            assert pending.result(timeout=15).status_code == (403 if change == 'acl' else 412)
        put('warm', raw)
marker.with_suffix('.hit').unlink(missing_ok=True)
print('PASS conditional body reads recheck ACL and version after metadata lookup', flush=True)

db.close()
