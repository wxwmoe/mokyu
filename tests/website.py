"""Public website and management authorization checks in an isolated test bucket.

Uses the integration.py MGW_TEST_* variables plus MGW_TEST_WEB,
MGW_TEST_PASSWORD (tester user), MGW_TEST_PUBLIC and MGW_TEST_PUBLIC_HOST.
"""
import json
import os
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

credentials = json.loads(Path(os.environ['MGW_TEST_CREDENTIALS']).read_text())
bucket = credentials['bucket']
s3 = boto3.client('s3', endpoint_url=os.environ['MGW_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                  config=Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                                request_checksum_calculation='when_required'))
manage = os.environ['MGW_TEST_WEB']
public = os.environ['MGW_TEST_PUBLIC']
session = requests.Session()
reply = session.post(manage + '/api/login', headers={'Origin': manage},
                     json={'username': 'tester', 'password': os.environ['MGW_TEST_PASSWORD']})
reply.raise_for_status()
headers = {'Origin': manage, 'X-CSRF-Token': reply.json()['csrf_token']}
bucket_id = next(b['id'] for b in session.get(manage + '/api/buckets').json() if b['name'] == bucket)
path = manage + '/api/buckets/' + bucket_id + '/website'
original = session.get(path).json()
settings = {'website_enabled': True, 'index_document': 'index.html', 'error_document': '404.html'}
keys = []


def put(key, body, acl='public-read'):
    keys.append(key)
    return s3.put_object(Bucket=bucket, Key=key, Body=body, ACL=acl, ContentType='text/html')


def get(path, method='GET', **extra):
    return requests.request(method, public + path,
                            headers={'Host': os.environ['MGW_TEST_PUBLIC_HOST'], **extra}, allow_redirects=False)


def save(**values):
    settings.update(values)
    reply = session.put(path, headers=headers, json=settings)
    assert reply.status_code == 200, reply.text
    assert reply.json() == settings


try:
    assert requests.get(path).status_code == 403
    assert requests.put(path, headers=headers, json=settings).status_code == 403
    assert session.put(path, json=settings).status_code == 403
    assert session.put(path, headers={**headers, 'Origin': 'https://wrong.test'}, json=settings).status_code == 403
    assert session.put(path, headers={**headers, 'X-CSRF-Token': 'wrong'}, json=settings).status_code == 403
    for name, value in [('index_document', ''), ('index_document', '../index.html'),
                        ('index_document', 'folder/index.html'), ('error_document', '/404.html'),
                        ('error_document', '../404.html'), ('error_document', 'a\\b'),
                        ('error_document', 'a\x00b'), ('error_document', 'x' * 1025)]:
        assert session.put(path, headers=headers, json={**settings, name: value}).status_code == 400
    assert session.get(path).json() == original
    root = b'<h1>root</h1>'
    nested = b'<h1>nested</h1>'
    error = b'<h1>custom missing</h1>'
    etag = put('index.html', root)['ETag']
    put('404.html', error)
    put('website-r2/docs/index.html', nested)
    put('website-r2/private/index.html', nested)
    put('website-r2/private', b'secret', 'private')
    put('website-r2/private-index/index.html', b'secret index', 'private')
    put('website-r2/exact', b'exact')
    put('website-r2/exact/index.html', b'not exact')
    put('/website-r2/double/index.html', nested)
    put('website-r2/中文 +%2F/index.html', nested)
    save(website_enabled=False)
    assert get('/').status_code == 404
    save(website_enabled=True)
    assert get('/').content == root
    assert get('/', method='HEAD').content == b''
    assert get('/', method='HEAD').headers['Content-Length'] == str(len(root))
    assert get('/', **{'If-None-Match': etag}).status_code == 304
    assert get('/', Range='bytes=0-3').status_code == 206
    assert get('/', Range='bytes=0-3').content == root[:4]
    redirect = get('/website-r2/docs?lang=en')
    assert redirect.status_code == 308 and redirect.headers['Location'] == '/website-r2/docs/?lang=en'
    assert get('/website-r2/docs/').content == nested
    assert get('/website-r2/exact').content == b'exact'
    assert get('/website-r2/private').status_code == 403
    assert get('/website-r2/private-index/').status_code == 403
    assert get('/website-r2/private-index').status_code == 403
    assert get('//website-r2/double').headers['Location'] == '/%2Fwebsite-r2/double/'
    assert get('/%2Fwebsite-r2/double').headers['Location'] == '/%2Fwebsite-r2/double/'
    assert get('/%2Fwebsite-r2/double/').content == nested
    assert get('/website-r2/%E4%B8%AD%E6%96%87%20%2B%252F/').content == nested
    missing = get('/website-r2/missing', Range='bytes=0-2', **{'If-None-Match': '*'})
    assert missing.status_code == 404 and missing.content == error
    assert missing.headers['Cache-Control'] == 'no-store' and 'Content-Range' not in missing.headers
    assert 'ETag' not in missing.headers
    assert get('/website-r2/missing', method='HEAD').content == b''
    # Website interpretation is absent from the S3 endpoint.
    try:
        s3.get_object(Bucket=bucket, Key='website-r2/docs/')
        raise AssertionError('S3 unexpectedly resolved an index')
    except ClientError as exc:
        assert exc.response['Error']['Code'] == 'NoSuchKey'
    assert get('/', Authorization='anything').status_code == 403
    assert get('/?X-Amz-Signature=anything').status_code == 403
    s3.put_object_acl(Bucket=bucket, Key='404.html', ACL='private')
    fallback = get('/website-r2/missing')
    assert fallback.status_code == 404 and b'custom missing' not in fallback.content
    save(error_document='does-not-exist.html')
    assert get('/website-r2/missing').status_code == 404
    save(index_document='home.html', error_document='')
    put('home.html', b'new homepage')
    assert get('/').content == b'new homepage'
    assert get('/website-r2/missing').status_code == 404
    print('PASS website: index/redirect/404, exact/private priority, encoded keys, HEAD/Range/conditions, CSRF and S3 isolation')
finally:
    session.put(path, headers=headers, json=original).raise_for_status()
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
