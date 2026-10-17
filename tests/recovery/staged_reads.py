"""A shared staging lookup may race publication and deletion on another node."""
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credentials = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credentials['bucket']
endpoints = json.loads(os.environ['MOKYU_TEST_DATA_ENDPOINTS'])
assert len(endpoints) >= 2
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
reader = boto3.client('s3', endpoint_url=endpoints[0], region_name='us-east-1',
    aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
    config=Config(retries={'max_attempts': 0}, request_checksum_calculation='when_required', response_checksum_validation='when_required'))
writer = boto3.client('s3', endpoint_url=endpoints[1], region_name='us-east-1',
    aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
    config=Config(retries={'max_attempts': 0}, request_checksum_calculation='when_required'))
upload_faults = [Path(path) / 'upload-before-work' for path in json.loads(os.environ['MOKYU_TEST_UPLOAD_FAULT_DIRS'])]
assert upload_faults
read_fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'read-pending-source'
assert all(not path.exists() for path in [*upload_faults, read_fault])

def wait(predicate):
    for _ in range(200):
        if predicate(): return
        time.sleep(.05)
    raise AssertionError('staging publication did not reach the expected boundary')

for signed in [False, True]:
    key = 'staged-read-' + uuid.uuid4().hex
    raw = os.urandom(4096)
    for fault in upload_faults:
        fault.touch()
    try:
        writer.put_object(Bucket=bucket, Key=key, Body=raw, ACL='public-read')
        chunk, staging = db.execute('SELECT e.chunk_id,u.staging_object_key FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id JOIN pending_uploads u ON u.chunk_id=e.chunk_id WHERE b.name=%s AND o.object_key=%s',(bucket,key)).fetchone()

        def read():
            if signed:
                return reader.get_object(Bucket=bucket,Key=key)['Body'].read()
            response = requests.get(f'{endpoints[0]}/{bucket}/{key}',headers={'Accept-Encoding':'identity','Cache-Control':'no-store'},timeout=15)
            assert response.status_code == 200
            return response.content

        if not signed:
            try:
                db.execute('UPDATE pending_uploads SET staging_object_key=%s WHERE chunk_id=%s',(staging+'-missing',chunk))
                try:
                    read()
                except requests.RequestException:
                    pass
                else:
                    raise AssertionError('a missing sole source must still fail')
            finally:
                db.execute('UPDATE pending_uploads SET staging_object_key=%s WHERE chunk_id=%s',(staging,chunk))
            print('PASS a missing sole staging source is not masked by source fallback',flush=True)

        read_fault.with_suffix('.hit').unlink(missing_ok=True)
        read_fault.touch()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(read)
            try:
                wait(read_fault.with_suffix('.hit').exists)
                for fault in upload_faults:
                    fault.unlink()
                wait(lambda: db.execute('SELECT count(*) FROM pending_uploads WHERE chunk_id=%s',(chunk,)).fetchone()[0] == 0)
                assert db.execute("SELECT count(*) FROM chunk_encodings WHERE chunk_id=%s AND state='ready'",(chunk,)).fetchone()[0] == 1
                assert db.execute('SELECT count(*) FROM staging_objects WHERE object_key=%s',(staging,)).fetchone()[0] == 0
            finally:
                read_fault.unlink(missing_ok=True)
            assert pending.result(timeout=15) == raw
        print('PASS deleted staging reselects the published source for '+('signed' if signed else 'public')+' reads',flush=True)
    finally:
        for fault in [*upload_faults, read_fault]:
            fault.unlink(missing_ok=True)
            fault.with_suffix('.hit').unlink(missing_ok=True)
        writer.delete_object(Bucket=bucket,Key=key)
db.close()
