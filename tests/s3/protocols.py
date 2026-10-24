"""HTTP/1.1 and h2c contracts on the S3, public and management listeners.

Requires the ordinary S3 credential fixture, MOKYU_TEST_PUBLIC and a configured
MOKYU_TEST_PUBLIC_HOST, MOKYU_TEST_WEB, and Python h2, boto3 and requests.
"""
import json
import os
from pathlib import Path
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlsplit
import uuid

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
import requests

from tests.http2 import Client


def check(reply, status, body=None):
    assert 'reset' not in reply, reply
    assert int(reply['headers'][':status']) == status, reply['headers']
    if body is not None:
        assert reply['body'] == body, (len(reply['body']), len(body))
    assert 'connection' not in reply['headers'], reply['headers']
    assert reply['headers'].get('x-request-id'), reply['headers']


def slow_preface(endpoint, prefix):
    target = urlsplit(endpoint)
    started = time.monotonic()
    with socket.create_connection((target.hostname, target.port or 80), timeout=20) as connection:
        connection.sendall(prefix)
        try:
            while connection.recv(65536):
                assert time.monotonic() - started < 20, 'listener did not expire an incomplete first request'
        except ConnectionResetError:
            pass
    elapsed = time.monotonic() - started
    assert 10 <= elapsed < 20, elapsed


def run():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    saved = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
    endpoint = os.environ['MOKYU_TEST_ENDPOINT']
    public = os.environ['MOKYU_TEST_PUBLIC']
    web = os.environ['MOKYU_TEST_WEB']
    host = os.environ['MOKYU_TEST_PUBLIC_HOST']
    bucket = saved['bucket']
    credential = Credentials(saved['access_key'], saved['secret_key'])
    config = Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                    request_checksum_calculation='when_required', response_checksum_validation='when_required')
    s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1', config=config,
                      aws_access_key_id=saved['access_key'], aws_secret_access_key=saved['secret_key'])
    key = 'protocol-' + uuid.uuid4().hex + '/space +%2F-\u4e2d'
    path = '/' + bucket + '/' + quote(key, safe='/')
    data = os.urandom(4 * 1024 * 1024 + 17)

    def signed(method, target=path, body=b'', headers=None):
        request = AWSRequest(method=method, url=endpoint + target, data=body, headers=headers or {})
        if method == 'PUT':
            request.headers['Content-Length'] = str(len(body))
        S3SigV4Auth(credential, 's3', 'us-east-1').add_auth(request)
        return list(request.headers.items())

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=data, ACL='public-read')
        info = requests.get(web + '/api/v1/info', timeout=10)
        assert info.status_code == 200 and info.json()['product'] == 'Mokyu'
        reply = requests.get(public + '/' + quote(key, safe='/'), headers={'Host': host, 'Accept-Encoding': 'identity'}, timeout=20)
        assert reply.status_code == 200 and reply.content == data
        with Client(web) as control:
            reply = control.request('GET', '/api/v1/info')
            check(reply, 200)
            assert json.loads(reply['body'])['product'] == 'Mokyu'
        with Client(public, host) as reader:
            check(reader.request('GET', '/' + quote(key, safe='/'), [('accept-encoding', 'identity')]), 200, data)
            check(reader.request('HEAD', '/' + quote(key, safe='/')), 200, b'')
            check(reader.request('HEAD', '/missing-' + uuid.uuid4().hex), 404, b'')
        with Client(endpoint) as client:
            check(client.request('GET', path, signed('GET')), 200, data)
            check(client.request('HEAD', path, signed('HEAD')), 200, b'')
            headers = signed('GET', headers={'Range': 'bytes=101-700'})
            check(client.request('GET', path, headers), 206, data[101:701])
            replacement = data[::-1]
            check(client.request('PUT', path, signed('PUT', body=replacement), replacement), 200)
            check(client.request('GET', path, signed('GET')), 200, replacement)
            check(client.request('GET', path), 403)
            streams = [client.start('GET', path, signed('GET')) for _ in range(6)]
            for reply in client.collect(streams):
                check(reply, 200, replacement)

            # A cancelled upload must not close sibling streams or publish partial bytes.
            cancelled = path + '-cancelled'
            stream = client.start('PUT', cancelled, signed('PUT', cancelled, data), end=False)
            client.connection.send_data(stream, data[:128])
            client.connection.reset_stream(stream, error_code=8)
            client.flush()
            check(client.request('GET', path, signed('GET')), 200, replacement)
            check(client.request('HEAD', cancelled, signed('HEAD', cancelled)), 404, b'')

            # Early rejection with an unread body leaves other HTTP/2 streams usable.
            stream = client.start('PUT', path, [('content-length', '1048576')], end=False)
            client.connection.send_data(stream, b'x' * 16)
            client.flush()
            check(client.collect([stream])[0], 403)
            check(client.request('GET', path, signed('GET')), 200, replacement)
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda target: slow_preface(target, b'PRI * HTTP/2.0\r\n'), [endpoint, public, web]))
        print('PASS HTTP/1.1+h2c on three listeners, signed URI, authority, Range, flow control, concurrent reads, RST_STREAM and slow preface', flush=True)
    finally:
        s3.delete_object(Bucket=bucket, Key=key)
        s3.delete_object(Bucket=bucket, Key=key + '-cancelled')
        s3.close()


if __name__ == '__main__':
    run()
