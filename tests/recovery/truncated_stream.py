"""A missing final extent must abort full and suffix reads, even after warming caches.

Requires an isolated PostgreSQL database, credentials, endpoint(s), and DATABASE_FILE.
The synthetic 513-extent object deliberately spans metadata pages with nonzero offsets.
"""
import hashlib
import json
import os
import uuid
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
nodes = json.loads(os.environ.get('MOKYU_TEST_ENDPOINTS', 'null')) or [os.environ['MOKYU_TEST_ENDPOINT']]
client = boto3.client('s3', endpoint_url=nodes[0], region_name='us-east-1',
                      aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                      config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}))
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
prefix = 'truncated-' + uuid.uuid4().hex
raw = bytes(range(256)) * 16
client.put_object(Bucket=bucket, Key=prefix + '-seed', Body=raw, ACL='public-read')
stream = uuid.uuid4()
segment = raw[17:-17]
payload = segment * 513
key = prefix + '-pages'
try:
    original, bucket_id = db.execute('SELECT stream_id,b.id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND object_key=%s', (bucket, prefix + '-seed')).fetchone()
    chunk, = db.execute('SELECT chunk_id FROM extents WHERE stream_id=%s', (original,)).fetchone()
    with db.transaction():
        db.execute("""INSERT INTO streams SELECT (jsonb_populate_record(NULL::streams,to_jsonb(s)||jsonb_build_object(
            'id',%s::text,'object_key',%s::text,'size',%s::bigint,'etag',%s::text,'checksums','{}'::jsonb))).*
            FROM streams s WHERE id=%s""", (str(stream), key, len(payload), hashlib.md5(payload).hexdigest(), original))
        db.execute('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT %s,i*%s,%s,%s,17 FROM generate_series(0,512) i', (stream, len(segment), len(segment), chunk))
        db.execute('INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES(%s,%s,%s,%s)', (bucket_id, key, stream, stream))
    for node in nodes:
        response = requests.get(f'{node}/{bucket}/{key}', headers={'Accept-Encoding': 'identity'}, timeout=20)
        assert response.status_code == 200 and response.content == payload
    removed = db.execute('DELETE FROM extents WHERE stream_id=%s AND offset_bytes=%s RETURNING offset_bytes,length,chunk_id,source_offset', (stream, 512 * len(segment))).fetchone()
    try:
        for cache in [{}, {'Cache-Control': 'no-store'}]:
            for span in [None, f'bytes={len(payload) - 32}-']:
                for node in nodes:
                    headers = {'Accept-Encoding': 'identity', **cache, **({'Range': span} if span else {})}
                    try:
                        response = requests.get(f'{node}/{bucket}/{key}', headers=headers, timeout=20)
                        assert response.status_code >= 500, (span, response.status_code, len(response.content))
                    except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError):
                        pass
        print('PASS missing final extent: full/Range abort on every node with warm or bypassed response cache', flush=True)
    finally:
        db.execute('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(%s,%s,%s,%s,%s)', (stream, *removed))
    for node in nodes:
        response = requests.get(f'{node}/{bucket}/{key}', headers={'Accept-Encoding': 'identity'}, timeout=20)
        assert response.status_code == 200 and response.content == payload
    print('PASS restored final extent: complete bytes on every node', flush=True)
finally:
    client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key}, {'Key': prefix + '-seed'}]})
    db.close()
    client.close()
