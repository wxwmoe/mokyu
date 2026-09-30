"""Bucket CORS and anonymous Host=Bucket checks using integration.py's isolated environment."""
import json
import os
from pathlib import Path
from urllib.parse import quote

import boto3
import requests
from botocore.config import Config

credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
endpoint = os.environ['MOKYU_TEST_ENDPOINT']
manage = os.environ['MOKYU_TEST_WEB']
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                                request_checksum_calculation='when_required'))
session = requests.Session()
reply = session.post(manage + '/api/login', headers={'Origin': manage},
                     json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
reply.raise_for_status()
auth = {'Origin': manage, 'X-CSRF-Token': reply.json()['csrf_token']}
bucket_id = next(b['id'] for b in session.get(manage + '/api/buckets').json() if b['name'] == bucket)
settings = manage + '/api/buckets/' + bucket_id + '/cors'
original = session.get(settings).json()
key = 'cors-check/\u4e2d\u6587 +%2F.bin'
data = b'public cors and range fixture'
origin = 'https://browser.test'
rules = [{'origins': [origin], 'methods': ['GET', 'HEAD', 'PUT'],
          'headers': ['range', 'x-special'], 'expose': ['ETag', 'Content-Range'], 'max_age': 600}]
routes = [
    (endpoint + '/' + bucket, {}),
    (endpoint, {'Host': bucket.upper() + ':9000'}),
    (os.environ['MOKYU_TEST_PUBLIC'], {'Host': os.environ['MOKYU_TEST_PUBLIC_HOST']}),
]
if domain := os.environ.get('MOKYU_TEST_S3_DOMAIN'):
    routes.append((endpoint, {'Host': bucket + '.' + domain + ':9000'}))


def save(value):
    response = session.put(settings, headers=auth, json=value)
    assert response.status_code == 200, response.text
    assert response.json() == value


def get(route, name=key, method='GET', **headers):
    base, host = route
    return requests.request(method, base + '/' + quote(name, safe='/'),
                            headers={**host, **headers}, allow_redirects=False, timeout=15)


try:
    assert requests.get(settings).status_code == 403
    assert session.put(settings, json=[]).status_code == 403
    for overrides in [{'Origin': 'https://wrong.test'}, {'X-CSRF-Token': 'wrong'}]:
        assert session.put(settings, headers={**auth, **overrides}, json=[]).status_code == 403
    for invalid in [{}, [{'origins': [], 'methods': ['GET']}],
                    [{**rules[0], 'methods': ['MOVE']}], [{**rules[0], 'headers': ['x\nbad']}],
                    [{**rules[0], 'max_age': -1}], [{**rules[0], 'unexpected': True}], rules * 101]:
        assert session.put(settings, headers=auth, json=invalid).status_code in (400, 413)
    assert session.get(settings).json() == original
    etag = s3.put_object(Bucket=bucket, Key=key, Body=data, ACL='public-read')['ETag']
    s3.put_object(Bucket=bucket, Key='cors-check/private', Body=b'secret', ACL='private')
    save([])
    for route in routes:
        response = get(route, Origin=origin)
        assert response.content == data and 'Access-Control-Allow-Origin' not in response.headers
        assert get(route, method='OPTIONS', Origin=origin,
                   **{'Access-Control-Request-Method': 'GET'}).status_code == 403
    alias = routes[1]
    for name in ['cors-check/private', 'cors-check/missing', '']:
        response = get(alias, name)
        assert response.status_code == 403 and '<Code>AccessDenied</Code>' in response.text
    assert get(alias, Authorization='invalid-signature').status_code >= 400
    for query in ['acl', 'X-Amz-Signature=invalid', 'AWSAccessKeyId=bad&Signature=bad']:
        response = requests.get(endpoint + '/' + quote(key, safe='/') + '?' + query, headers=alias[1])
        assert response.status_code >= 400 and response.content != data
    signed = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
    assert requests.get(signed).content == data
    assert requests.get(signed, headers=alias[1]).status_code >= 400
    save(rules)
    for route in routes:
        response = get(route, Origin=origin, Range='bytes=2-8')
        assert response.status_code == 206 and response.content == data[2:9]
        assert response.headers['Access-Control-Allow-Origin'] == origin
        assert response.headers['Content-Range'] == f'bytes 2-8/{len(data)}'
        assert 'Origin' in response.headers['Vary']
        assert 'Origin' in get(route).headers['Vary']
        assert get(route, Origin=origin, **{'If-None-Match': etag}).status_code == 304
        assert get(route, Origin=origin, **{'If-None-Match': etag}).headers['Access-Control-Allow-Origin'] == origin
        response = get(route, method='HEAD', Origin=origin)
        assert response.status_code == 200 and not response.content
        assert int(response.headers['Content-Length']) == len(data)
        for name, statuses in [('cors-check/private', (403,)), ('cors-check/missing', (403, 404))]:
            response = get(route, name, Origin=origin)
            assert response.status_code in statuses and response.headers['Access-Control-Allow-Origin'] == origin
        response = get(route, method='OPTIONS', Origin=origin,
                       **{'Access-Control-Request-Method': 'GET', 'Access-Control-Request-Headers': 'Range, X-Special'})
        assert response.status_code == 204
        assert response.headers['Access-Control-Allow-Headers'] == 'Range, X-Special'
        assert 'Access-Control-Request-Headers' in response.headers['Vary']
        for extra in [{'Access-Control-Request-Headers': 'authorization'},
                      {'Access-Control-Request-Method': 'DELETE'}, {'Origin': 'https://other.test'}]:
            response = get(route, method='OPTIONS', **{'Origin': origin, 'Access-Control-Request-Method': 'GET', **extra})
            assert response.status_code == 403 and 'Access-Control-Allow-Origin' not in response.headers
        assert get(route, method='OPTIONS', Origin=origin).status_code == 403
        assert 'Access-Control-Allow-Origin' not in get(route, Origin='https://other.test').headers
        assert get(route, Origin=origin, **{'Access-Control-Request-Method': 'DELETE'}).headers['Access-Control-Allow-Origin'] == origin
    save([{**rules[0], 'methods': ['PUT']}])
    assert 'Access-Control-Allow-Origin' not in get(routes[0], Origin=origin, **{'Access-Control-Request-Method': 'PUT'}).headers
    assert get(routes[0], method='OPTIONS', Origin=origin, **{'Access-Control-Request-Method': 'PUT'}).status_code == 204
    for route in routes[1:3]:
        assert get(route, method='OPTIONS', Origin=origin, **{'Access-Control-Request-Method': 'PUT'}).status_code == 403
    save([{'origins': ['*'], 'methods': ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
           'headers': ['*'], 'expose': ['*'], 'max_age': 86400}])
    for route in routes:
        response = get(route, Origin=origin)
        assert response.headers['Access-Control-Allow-Origin'] == '*'
        assert response.headers['Access-Control-Expose-Headers'] == '*'
        assert response.headers['Access-Control-Max-Age'] == '86400'
        assert 'Access-Control-Allow-Credentials' not in response.headers
        response = get(route, method='OPTIONS', Origin=origin,
                       **{'Access-Control-Request-Method': 'GET', 'Access-Control-Request-Headers': 'authorization'})
        assert response.status_code == 204 and response.headers['Access-Control-Allow-Headers'] == 'authorization'
    assert get(routes[0], 'cors-check/private', Origin=origin).status_code == 403
    assert get(routes[0], method='PUT', Origin=origin).status_code == 403
    print('PASS CORS management, preflights, error/conditional responses, Host=Bucket ACL and signature isolation')
finally:
    save(original)
    for name in [key, 'cors-check/private']:
        s3.delete_object(Bucket=bucket, Key=name)
