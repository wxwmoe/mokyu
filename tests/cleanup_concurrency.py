"""Cleanup/read/write concurrency with deterministic pauses in an isolated gateway.

Uses cleanup.py's environment plus MOKYU_TEST_FAULT_DIR and a fault-injection
build. Set cleanup.interval=1s and leave other retention settings at defaults.
"""
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
creds = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = creds['bucket']
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=creds['access_key'], aws_secret_access_key=creds['secret_key'],
                  config=Config(read_timeout=3, retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
data = Path(os.environ['MOKYU_TEST_DATA'])
prefix = 'cleanup-concurrency-' + uuid.uuid4().hex + '/'


def cli(*args):
    return json.loads(subprocess.check_output([os.environ['MOKYU_TEST_BINARY'], '--config',
        os.environ['MOKYU_TEST_CONFIG'], 'cli', *args], text=True, timeout=10))


def wait(predicate):
    deadline = time.monotonic() + 8
    while not predicate():
        assert time.monotonic() < deadline, 'condition timed out'
        time.sleep(.02)


def scalar(query, args=()):
    return db.execute(query, args).fetchone()[0]


def put(key, body):
    s3.put_object(Bucket=bucket, Key=prefix + key, Body=body)


def read(key):
    return s3.get_object(Bucket=bucket, Key=prefix + key)['Body'].read()


def stream(key):
    return scalar('SELECT stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.key=%s', (bucket, prefix + key))


def arm(name):
    marker = faults / name
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    return marker


raw = os.urandom(200000)
put('warm', raw)
put('pinned', raw)
sid = stream('pinned')
storage = scalar('SELECT c.storage_id FROM chunks c JOIN extents e ON e.chunk_id=c.id WHERE e.stream_id=%s', (sid,))
for extension in ('raw', 'zst'):
    (data / 'chunks' / storage.hex[:2] / (storage.hex + '.' + extension)).unlink(missing_ok=True)
marker = arm('cache-reserved')
try:
    with ThreadPoolExecutor(max_workers=1) as pool:
        reader = pool.submit(read, 'pinned')
        try:
            wait(lambda: marker.with_suffix('.hit').exists())
            s3.delete_object(Bucket=bucket, Key=prefix + 'pinned')
            cli('gc', 'run')
            assert scalar('SELECT count(*) FROM extents WHERE stream_id=%s', (sid,)) > 0
            assert not reader.done()
        finally:
            marker.unlink(missing_ok=True)
        assert reader.result(timeout=5) == raw
finally:
    marker.unlink(missing_ok=True)
wait(lambda: scalar('SELECT count(*) FROM streams WHERE id=%s', (sid,)) == 0)
print('PASS retired stream mappings survive an in-flight reader and clean after its pin releases', flush=True)

put('orphan', os.urandom(200000))
sid = stream('orphan')
marker = arm('cleanup-stream-claimed')
try:
    s3.delete_object(Bucket=bucket, Key=prefix + 'orphan')
    wait(lambda: marker.with_suffix('.hit').exists())
    assert scalar('SELECT state FROM streams WHERE id=%s', (sid,)) == 'abandoned'
    assert read('warm') == raw
    put('during-cleanup', b'write while local cleanup is paused')
    assert read('during-cleanup') == b'write while local cleanup is paused'
    task = uuid.uuid4()
    db.execute("INSERT INTO tasks(id,kind,state,updated_at) VALUES(%s,'sweep','completed',now()-interval '31 days')", (task,))
    wait(lambda: scalar('SELECT count(*) FROM tasks WHERE id=%s', (task,)) == 0)
    assert marker.exists()
finally:
    marker.unlink(missing_ok=True)
wait(lambda: scalar('SELECT count(*) FROM streams WHERE id=%s', (sid,)) == 0)
print('PASS orphan cleanup releases the global lock; reads, writes and periodic history cleanup continue', flush=True)

uid = s3.create_multipart_upload(Bucket=bucket, Key=prefix + 'pending')['UploadId']
marker = arm('fragment-persisted')
try:
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(s3.upload_part, Bucket=bucket, Key=prefix + 'pending', UploadId=uid, PartNumber=1, Body=raw)
        try:
            wait(lambda: marker.with_suffix('.hit').exists())
            fragment = scalar("SELECT f.id FROM fragments f JOIN streams s ON s.id=f.owner_stream WHERE s.object_key=%s AND NOT f.sealed", (prefix + 'pending',))
            cli('gc', 'run')
            assert (data / 'multipart' / fragment.hex).exists()
            assert scalar('SELECT count(*) FROM fragments WHERE id=%s', (fragment,)) == 1
        finally:
            marker.unlink(missing_ok=True)
        part = pending.result(timeout=5)
finally:
    marker.unlink(missing_ok=True)
s3.complete_multipart_upload(Bucket=bucket, Key=prefix + 'pending', UploadId=uid,
                             MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
assert read('pending') == raw
print('PASS durable multipart fragment survives cleanup before its extent is committed', flush=True)
db.close()
