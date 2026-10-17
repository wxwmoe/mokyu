"""Empty and one-byte Range contracts on signed S3, public S3/Web and panel reads.

Requires isolated-only authorization, credentials and endpoint. PUBLIC_WEB/HOST
enable public Web checks; WEB and PASSWORD_FILE enable authenticated panel reads.
"""
import json
import os
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode

import boto3
import requests
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket, endpoint = credential['bucket'], os.environ['MOKYU_TEST_ENDPOINT'].rstrip('/')
auth = Credentials(credential['access_key'], credential['secret_key'])
client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                      aws_access_key_id=auth.access_key, aws_secret_access_key=auth.secret_key,
                      config=Config(signature_version='s3v4', retries={'max_attempts': 0},
                                    s3={'addressing_style': 'path'}))
prefix = 'empty-ranges-' + uuid.uuid4().hex + '/'
session = requests.Session()
sources = [('signed S3', endpoint + '/' + bucket + '/', {}, True, session),
           ('public S3', endpoint + '/' + bucket + '/', {}, False, session)]
if os.environ.get('MOKYU_TEST_PUBLIC_WEB'):
    sources.append(('public Web', os.environ['MOKYU_TEST_PUBLIC_WEB'].rstrip('/') + '/',
                    {'Host': os.environ['MOKYU_TEST_PUBLIC_HOST']}, False, session))

try:
    for name, data in [('empty', b''), ('one', b'x')]:
        client.put_object(Bucket=bucket, Key=prefix + name, Body=data, ACL='public-read')
    client.copy_object(Bucket=bucket, Key=prefix + 'copy',
                       CopySource={'Bucket': bucket, 'Key': prefix + 'empty'})
    with client.get_object(Bucket=bucket, Key=prefix + 'copy')['Body'] as body:
        assert body.read() == b''
    panel_urls = {}
    if os.environ.get('MOKYU_TEST_PASSWORD_FILE'):
        web = os.environ['MOKYU_TEST_WEB'].rstrip('/')
        panel = requests.Session()
        response = panel.post(web + '/api/v1/auth/login', headers={'Origin': web},
                              json={'username': 'tester', 'password': Path(os.environ['MOKYU_TEST_PASSWORD_FILE']).read_text().strip()}, timeout=20)
        response.raise_for_status()
        response = panel.get(web + '/api/v1/buckets', timeout=20)
        response.raise_for_status()
        bucket_id = next(row['id'] for row in response.json() if row['name'] == bucket)
        for name in ['empty', 'one']:
            response = panel.get(f'{web}/api/v1/buckets/{bucket_id}/objects',
                                 params={'q': prefix + name, 'mode': 'exact', 'search_in': 'path', 'recursive': 'true'}, timeout=20)
            response.raise_for_status()
            item, = response.json()['items']
            panel_urls[name] = f'{web}/api/v1/buckets/{bucket_id}/object/content?' + urlencode({'key': prefix + name, 'version': item['id']})
        sources.append(('panel', '', {}, False, panel))

    for label, base, initial, signed, connection in sources:
        def request(name, method='GET', **extra):
            url = panel_urls[name] if label == 'panel' else base + quote(prefix + name, safe='/')
            headers = {'Accept-Encoding': 'identity', **initial, **extra}
            if signed:
                req = AWSRequest(method=method, url=url, headers=headers)
                S3SigV4Auth(auth, 's3', 'us-east-1').add_auth(req)
                headers = dict(req.headers)
            reply = connection.request(method, url, headers=headers, timeout=20)
            return reply

        for name, data in [('empty', b''), ('one', b'x')]:
            response = request(name)
            assert response.status_code == 200 and response.content == data, (label, response.status_code, response.content[:256])
            tag = response.headers['etag']
            assert response.headers['content-length'] == str(len(data))
            for method in ['GET', 'HEAD']:
                response = request(name, method)
                assert response.status_code == 200 and response.content == (data if method == 'GET' else b'')
                assert response.headers['content-length'] == str(len(data))
                for value in ['bytes=-1', 'bytes=-99', 'bytes=0-', 'bytes=0-0']:
                    response = request(name, method, Range=value)
                    if not data:
                        assert response.status_code == 416, (label, method, value, response.status_code)
                        assert response.headers['content-range'] == 'bytes */0', dict(response.headers)
                    else:
                        assert response.status_code in ((200, 206) if method == 'HEAD' else (206,))
                        assert response.headers['content-length'] == '1'
                        if method == 'GET':
                            assert response.content == data and response.headers['content-range'] == 'bytes 0-0/1'
                response = request(name, method, Range='bytes=-1', **{'If-None-Match': tag})
                assert response.status_code == 304 and not response.content, (label, method, response.status_code)
                response = request(name, method, Range='bytes=-1', **{'If-Match': '"different"'})
                assert response.status_code == 412, (label, method, response.status_code)
            for validator in ['"different"', 'W/' + tag]:
                response = request(name, Range='bytes=-1', **{'If-Range': validator})
                assert response.status_code == 200 and response.content == data
                assert 'content-range' not in response.headers
            response = request(name, Range='bytes=-1', **{'If-Range': tag})
            assert response.status_code == (206 if data else 416)
            response = request(name, Range='bytes=-0')
            assert response.status_code in (400, 416), (label, response.status_code)
        print(f'PASS {label}: empty/single byte, suffix/open/closed Range, HEAD, conditions and If-Range', flush=True)
finally:
    client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': prefix + name} for name in ['empty', 'one', 'copy']]})
    for connection in {source[-1] for source in sources}:
        connection.close()
