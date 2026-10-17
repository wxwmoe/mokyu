"""Public Web/S3 representation contract; requires an isolated deployment and public domain."""
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode

import boto3
import brotli
import requests
import zstandard
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket, endpoint = credential['bucket'], os.environ['MOKYU_TEST_ENDPOINT']
client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                      aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                      config=Config(signature_version='s3v4', retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
                                    request_checksum_calculation='when_required', response_checksum_validation='when_required'))
prefix = 'http-compression-' + os.urandom(6).hex() + '/'
original = b'Public object representation, byte integrity and conditional caching.\n' * 4000
client.put_object(Bucket=bucket, Key=prefix + 'text', Body=original, ACL='public-read', ContentType='text/plain')
client.put_object(Bucket=bucket, Key=prefix + 'private', Body=original, ContentType='text/plain')
client.put_object(Bucket=bucket, Key=prefix + 'untouched', Body=original, ACL='public-read', ContentType='text/plain', CacheControl='public, no-transform')
stored = gzip.compress(original)
client.put_object(Bucket=bucket, Key=prefix + 'encoded', Body=stored, ACL='public-read', ContentType='text/plain', ContentEncoding='gzip')
client.put_object(Bucket=bucket, Key=prefix + 'small', Body=b'small', ACL='public-read', ContentType='text/plain')
sources = [(endpoint + '/' + bucket + '/', {})]
if os.environ.get('MOKYU_TEST_PUBLIC_WEB'):
    sources.append((os.environ['MOKYU_TEST_PUBLIC_WEB'] + '/', {'Host': os.environ['MOKYU_TEST_PUBLIC_HOST']}))
decoders = {'zstd': lambda value: zstandard.ZstdDecompressor().stream_reader(value).read(), 'br': brotli.decompress, 'gzip': gzip.decompress}

def wire(source, key='text', encoding='zstd, br, gzip', method='GET', query='', **headers):
    response = requests.request(method, source[0] + prefix + key + query, headers={**source[1], 'Accept-Encoding': encoding, **headers}, stream=True, timeout=40)
    data = response.raw.read(decode_content=False)
    response.close()
    return response, data

for source in sources:
    identity, data = wire(source, encoding='identity')
    assert identity.status_code == 200 and data == original
    tag = identity.headers['etag']
    assert not tag.startswith('W/')
    with ThreadPoolExecutor(max_workers=2) as pool:
        variants = dict(zip(decoders, pool.map(lambda encoding: wire(source, encoding=encoding), decoders)))
    for encoding, (response, data) in variants.items():
        assert response.status_code == 200 and response.headers.get('content-encoding') == encoding, (source, encoding, response.status_code, dict(response.headers), data[:120])
        assert decoders[encoding](data) == original
        assert len(data) < len(original) and response.headers['etag'] == 'W/' + tag
        assert 'accept-ranges' not in response.headers
        assert 'accept-encoding' in response.headers['vary'].lower()
        if 'content-length' in response.headers:
            assert int(response.headers['content-length']) == len(data)
        head, data = wire(source, encoding=encoding, method='HEAD')
        assert head.status_code == 200 and not data and head.headers.get('content-encoding') == encoding, (encoding, dict(head.headers))
        # Learned lengths do not replace an actually held representation.
        assert 'content-length' not in head.headers, dict(head.headers)
        cached, data = wire(source, encoding=encoding, **{'If-None-Match': tag})
        assert cached.status_code == 304 and not data and cached.headers['etag'] == 'W/' + tag, (source, encoding, cached.status_code, dict(cached.headers), len(data))
        assert 'content-encoding' not in cached.headers and 'content-length' not in cached.headers
        cached, data = wire(source, encoding='identity', **{'If-None-Match': 'W/' + tag})
        assert cached.status_code == 304 and not data and cached.headers['etag'] == tag
    response, _ = wire(source)
    assert response.headers['content-encoding'] == 'zstd'
    response, _ = wire(source, encoding='zstd;q=0.1, br;q=0.9, gzip;q=1')
    assert response.headers['content-encoding'] == 'gzip'
    response, _ = wire(source, encoding='*;q=0')
    assert response.status_code == 406
    for headers in [{'If-Match': tag}, {'Cache-Control': 'no-transform'}]:
        response, data = wire(source, **headers)
        assert response.status_code == 200 and data == original and 'content-encoding' not in response.headers
    response, _ = wire(source, encoding='zstd, identity;q=0', **{'If-Match': tag})
    assert response.status_code == 412
    response, data = wire(source, Range='bytes=13-99')
    assert response.status_code == 206 and data == original[13:100] and response.headers['etag'] == tag
    assert response.headers['content-range'] == f'bytes 13-99/{len(original)}' and 'content-encoding' not in response.headers
    response, data = wire(source, Range='bytes=13-99', **{'If-Range': tag})
    assert response.status_code == 206 and data == original[13:100]
    for validator in ['W/' + tag, '"different"', identity.headers['last-modified'], 'Sun, 01 Jan 2090 00:00:00 GMT']:
        response, data = wire(source, Range='bytes=13-99', **{'If-Range': validator})
        assert response.status_code == 200 and response.headers.get('content-encoding') == 'zstd' and decoders['zstd'](data) == original
    response, data = wire(source, encoding='zstd, identity;q=0', Range='bytes=13-99')
    assert response.status_code == 200 and decoders['zstd'](data) == original
    response, _ = wire(source, Range=f'bytes={len(original)}-')
    assert response.status_code == 416 and response.headers['content-range'] == f'bytes */{len(original)}'
    response, _ = wire(source, Range='bytes=0-1,4-5')
    assert response.status_code in (400, 416), (source, response.status_code)
    response, data = wire(source, key='untouched')
    assert response.status_code == 200 and data == original and 'content-encoding' not in response.headers
    response, data = wire(source, key='encoded')
    assert response.status_code == 200 and data == stored and response.headers['content-encoding'] == 'gzip' and not response.headers['etag'].startswith('W/')
    response, data = wire(source, key='small')
    assert data == b'small' and 'content-encoding' not in response.headers
    assert wire(source, key='private')[0].status_code == 403

client.meta.events.register('before-sign.s3.GetObject', lambda request, **kwargs: request.headers.__setitem__('Accept-Encoding', 'zstd, br, gzip'))
signed = client.get_object(Bucket=bucket, Key=prefix + 'text')
assert signed['Body'].read() == original and not signed.get('ContentEncoding') and signed['ETag'] == tag
presigned = client.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': prefix + 'text'}, ExpiresIn=60)
response = requests.get(presigned, headers={'Accept-Encoding': 'zstd, br, gzip'})
assert response.content == original and 'content-encoding' not in response.headers, (response.status_code, dict(response.headers), response.content[:150])
assert requests.get(presigned + '0').status_code in (400, 403)
response, data = wire(sources[0], **{'X-Amz-Checksum-Mode': 'ENABLED'})
assert data == original and 'content-encoding' not in response.headers
response, data = wire(sources[0], query='?' + urlencode({'response-cache-control': 'no-transform'}))
assert response.status_code == 200 and data == original and 'content-encoding' not in response.headers

if os.environ.get('MOKYU_TEST_DATABASE_FILE'):
    import psycopg
    with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True) as db:
        def summary():
            return db.execute('SELECT s.http_compression FROM streams s JOIN objects o ON o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s', (bucket, prefix + 'text')).fetchone()[0]
        for _ in range(100):
            learned = summary()
            if all(learned.get(encoding, 0) > 0 for encoding in decoders):
                break
            time.sleep(.05)
        assert learned['version'] == 1 and 0 < learned['estimated_size'] < len(original), learned
        assert all(learned.get(encoding, 0) > 0 for encoding in decoders), learned
        # Excluding one measured codec must leave the other candidates usable.
        db.execute("UPDATE streams SET http_compression=jsonb_set(http_compression,'{zstd}','0') WHERE id=(SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s)", (bucket, prefix + 'text'))
        assert wire(sources[0])[0].headers['content-encoding'] == 'br'
        assert wire(sources[0], encoding='zstd')[0].headers.get('content-encoding') is None
        assert wire(sources[0], encoding='zstd, identity;q=0')[0].status_code == 406
        # A blocked advisory write cannot hold up the verified response or its local read lock.
        stream = db.execute('SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s', (bucket, prefix + 'text')).fetchone()[0]
        db.execute("UPDATE streams SET http_compression=http_compression-'gzip' WHERE id=%s", (stream,))
        with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text()) as blocker:
            blocker.execute('SELECT 1 FROM streams WHERE id=%s FOR SHARE', (stream,))
            with ThreadPoolExecutor(max_workers=1) as pool:
                response, data = pool.submit(wire, sources[0], encoding='gzip').result(timeout=1.5)
            assert response.status_code == 200 and gzip.decompress(data) == original
            time.sleep(2.2)
            assert 'gzip' not in summary()
        assert gzip.decompress(wire(sources[0], encoding='gzip')[1]) == original
        noise = os.urandom(256 * 1024)
        client.put_object(Bucket=bucket, Key=prefix + 'legacy', Body=noise, ACL='public-read', ContentType='text/plain')
        legacy = db.execute('SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s', (bucket, prefix + 'legacy')).fetchone()[0]
        db.execute("UPDATE streams SET http_compression='{}' WHERE id=%s", (legacy,))
        response, data = wire(sources[0], key='legacy', encoding='gzip')
        assert response.headers['content-encoding'] == 'gzip' and gzip.decompress(data) == noise
        for _ in range(100):
            learned = db.execute('SELECT http_compression FROM streams WHERE id=%s', (legacy,)).fetchone()[0]
            if learned.get('gzip') == 0:
                break
            time.sleep(.05)
        assert learned['gzip'] == 0 and 'br' not in learned
        response, data = wire(sources[0], key='legacy', encoding='gzip')
        assert data == noise and 'content-encoding' not in response.headers
        if os.environ.get('MOKYU_TEST_FAULT_DIR'):
            client.put_object(Bucket=bucket, Key=prefix + 'cancel', Body=original, ACL='public-read', ContentType='text/plain')
            cancel = db.execute('SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name=%s AND o.object_key=%s', (bucket, prefix + 'cancel')).fetchone()[0]
            fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'http-compression-finish'
            hit = fault.with_suffix('.hit')
            hit.unlink(missing_ok=True)
            fault.touch()
            try:
                response = requests.get(sources[0][0] + prefix + 'cancel', headers={**sources[0][1], 'Accept-Encoding': 'gzip'}, stream=True, timeout=10)
                assert response.raw.read(32, decode_content=False)
                for _ in range(100):
                    if hit.exists():
                        break
                    time.sleep(.02)
                assert hit.exists(), 'encoder must reach the paused finish boundary'
                response.close()
                time.sleep(.3)
            finally:
                fault.unlink(missing_ok=True)
            time.sleep(.3)
            assert 'gzip' not in db.execute('SELECT http_compression FROM streams WHERE id=%s', (cancel,)).fetchone()[0]
print('PASS public object codecs, wire bytes, learning, signatures, conditions, ranges, HEAD, existing encoding and no-transform')
