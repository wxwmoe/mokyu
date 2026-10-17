"""Complete public body reuse, fresh authorization and metadata validity in an isolated service."""
import gzip
import json
import os
import subprocess
import time
from pathlib import Path

import boto3
import brotli
import requests
import zstandard
from urllib3.exceptions import ProtocolError
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credentials = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket, endpoint = credentials['bucket'], os.environ['MOKYU_TEST_ENDPOINT']
client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1', aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'], config=Config(signature_version='s3v4', retries={'max_attempts': 0}, s3={'addressing_style': 'path'}, request_checksum_calculation='when_required', response_checksum_validation='when_required'))
prefix = 'response-cache-' + os.urandom(5).hex() + '/'
text = b'Immutable public response bytes share a bounded memory budget.\n' * 2000
noise = os.urandom(len(text))
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))
def status():
    if url := os.environ.get('MOKYU_TEST_STATUS_URL'):
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    return cli('status')
def put(name, data, **metadata):
    client.put_object(Bucket=bucket, Key=prefix + name, Body=data, ACL='public-read', ContentType='text/plain', **metadata)
def read(name, encoding='identity', web=False, method='GET', **headers):
    target = endpoint + '/' + bucket + '/' + prefix + name
    if web:
        target = os.environ['MOKYU_TEST_PUBLIC_WEB'] + '/' + prefix + name
        headers['Host'] = os.environ['MOKYU_TEST_PUBLIC_HOST']
    response = requests.request(method, target, headers={'Accept-Encoding': encoding, **headers}, stream=True, timeout=15)
    data = response.raw.read(decode_content=False)
    response.close()
    return response, data
def warm(name, data, encoding='identity'):
    decode = {'identity': lambda data: data, 'zstd': lambda data: zstandard.ZstdDecompressor().stream_reader(data).read(), 'br': brotli.decompress, 'gzip': gzip.decompress}[encoding]
    for _ in range(3):
        response, encoded = read(name, encoding)
        assert response.status_code == 200 and decode(encoded) == data
    return encoded

assert status()['response_cache']['limit_bytes'] > 0
put('text', text)
put('noise', noise)
for name, original in [('text', text), ('noise', noise)]:
    for encoding in (['zstd', 'br', 'gzip', 'identity'] if name == 'text' else ['identity']):
        encoded = warm(name, original, encoding)
        before = status()
        response, data = read(name, encoding, web=bool(os.environ.get('MOKYU_TEST_PUBLIC_WEB')))
        after = status()
        assert data == encoded and int(response.headers['content-length']) == len(encoded)
        assert after['response_cache']['hits'] == before['response_cache']['hits'] + 1, (encoding, before['response_cache'], after['response_cache'])
        assert after['backend_gets'] == before['backend_gets'] and after['cache_hits'] == before['cache_hits']
        head, data = read(name, encoding, method='HEAD')
        assert not data and int(head.headers['content-length']) == len(encoded)
        for _ in range(100):
            if status()['active_streams'] == 0:
                break
            time.sleep(.02)
        assert status()['active_streams'] == 0
before = status()['response_cache']['hits']
response, data = read('noise', Range='bytes=3-99')
assert response.status_code == 206 and data == noise[3:100]
assert status()['response_cache']['hits'] == before + 1
response, _ = read('text', encoding='gzip, br, zstd')
assert response.headers['content-encoding'] == 'zstd'

before = status()['response_cache']['hits']
assert client.get_object(Bucket=bucket, Key=prefix + 'text')['Body'].read() == text
assert status()['response_cache']['hits'] == before
if os.environ.get('MOKYU_TEST_PUBLIC_WEB') and not os.environ.get('MOKYU_TEST_STATUS_URL'):
    alias = prefix.rstrip('/').replace('/', '-') + '.test'
    cli('domain', 'set', alias, bucket)
    target = os.environ['MOKYU_TEST_PUBLIC_WEB'] + '/' + prefix + 'text'
    try:
        for _ in range(3):
            response = requests.get(target, headers={'Host': alias, 'Accept-Encoding': 'gzip'}, timeout=10)
            assert response.content == text
    finally:
        cli('domain', 'delete', alias)
    assert requests.get(target, headers={'Host': alias}, timeout=10).status_code == 403
for policy in ['private, max-age=60', 'public, no-store']:
    name = policy.split(',')[0]
    put(name, text, CacheControl=policy)
    before = status()['response_cache']['entries']
    warm(name, text, 'zstd')
    assert status()['response_cache']['entries'] == before

client.put_object_acl(Bucket=bucket, Key=prefix + 'text', ACL='private')
assert read('text', 'zstd')[0].status_code == 403
if os.environ.get('MOKYU_TEST_PUBLIC_WEB'):
    assert read('text', 'zstd', web=True)[0].status_code == 403
client.put_object_acl(Bucket=bucket, Key=prefix + 'text', ACL='public-read')
assert zstandard.ZstdDecompressor().stream_reader(read('text', 'zstd')[1]).read() == text
replacement = text[::-1]
put('text', replacement)
assert zstandard.ZstdDecompressor().stream_reader(read('text', 'zstd')[1]).read() == replacement
client.copy_object(Bucket=bucket, Key=prefix + 'copy', CopySource={'Bucket': bucket, 'Key': prefix + 'noise'}, ACL='public-read')
assert read('copy')[1] == noise
client.delete_object(Bucket=bucket, Key=prefix + 'noise')
assert read('noise')[0].status_code == 404

if os.environ.get('MOKYU_TEST_DATABASE_FILE'):
    import psycopg
    with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True) as db:
        put('unavailable', text)
        warm('unavailable', text, 'zstd')
        chunk = db.execute('SELECT e.chunk_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s LIMIT 1', (bucket, prefix + 'unavailable')).fetchone()[0]
        try:
            db.execute("UPDATE chunks SET state='deleting' WHERE id=%s", (chunk,))
            assert read('unavailable', 'zstd')[0].status_code == 500
        finally:
            db.execute("UPDATE chunks SET state='ready' WHERE id=%s", (chunk,))
        warm('unavailable', text, 'zstd')
        before = status()['response_cache']['hits']
        row = db.execute('SELECT e.stream_id,e.offset_bytes,e.source_offset FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s LIMIT 1', (bucket, prefix + 'unavailable')).fetchone()
        try:
            db.execute('UPDATE extents SET source_offset=source_offset+1 WHERE stream_id=%s AND offset_bytes=%s', row[:2])
            try:
                response, data = read('unavailable', 'zstd')
                assert response.status_code == 500
            except (requests.RequestException, OSError, ProtocolError):
                pass
            assert status()['response_cache']['hits'] == before
        finally:
            db.execute('UPDATE extents SET source_offset=%s WHERE stream_id=%s AND offset_bytes=%s', (row[2], row[0], row[1]))
        put('fragmented', text)
        stream, chunk = db.execute('SELECT e.stream_id,e.chunk_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s LIMIT 1', (bucket, prefix + 'fragmented')).fetchone()
        with db.transaction():
            db.execute('DELETE FROM extents WHERE stream_id=%s', (stream,))
            for number in range(65):
                offset = number * 1000
                length = 1000 if number < 64 else len(text) - offset
                db.execute('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(%s,%s,%s,%s,%s)', (stream, offset, length, chunk, offset))
        before = status()['response_cache']['hits']
        warm('fragmented', text)
        assert status()['response_cache']['hits'] == before

if nodes := os.environ.get('MOKYU_TEST_DATA_ENDPOINTS'):
    nodes = json.loads(nodes)
    put('shared', text)
    def node_read(node, encoding='gzip'):
        response = requests.get(node + '/' + bucket + '/' + prefix + 'shared', headers={'Accept-Encoding': encoding}, timeout=15)
        return response
    for node in nodes:
        for _ in range(3):
            assert node_read(node).content == text
    client.put_object_acl(Bucket=bucket, Key=prefix + 'shared', ACL='private')
    assert all(node_read(node).status_code == 403 for node in nodes)
    put('shared', text[::-1])
    assert all(node_read(node).content == text[::-1] for node in nodes)
    client.delete_object(Bucket=bucket, Key=prefix + 'shared')
    assert all(node_read(node).status_code == 404 for node in nodes)
    print('PASS independent data-node caches observe ACL, overwrite and deletion immediately')
print('PASS complete body reuse, cross-listener variants, cached lengths, raw Range, auth exclusions, ACL, overwrite, copy, delete and invalid source metadata')
