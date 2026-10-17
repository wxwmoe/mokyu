"""Early S3 rejection must close unread-body connections or safely reuse drained ones."""
import http.client
import json
import os
import socket
import uuid
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
endpoint = os.environ['MOKYU_TEST_ENDPOINT']
target = urlsplit(endpoint)
assert target.scheme == 'http' and target.path in ('', '/'), 'run directly against an isolated HTTP listener'
client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                      aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                      config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}))
key = 'connections-' + uuid.uuid4().hex
expected = b'Complete public response after a rejected write.\n' * 128
client.put_object(Bucket=bucket, Key=key, Body=expected, ACL='public-read')
try:
    for variant, body in [('expect', b''), ('partial', b'x' * 16), ('full', b'x' * 128)]:
        outcomes = Counter()
        for _ in range(20):
            with socket.create_connection((target.hostname, target.port or 80), timeout=5) as connection:
                request = f'PUT /{bucket}/{key} HTTP/1.1\r\nHost: {target.netloc}\r\nContent-Length: 128\r\nConnection: keep-alive\r\n'
                if variant == 'expect':
                    request += 'Expect: 100-continue\r\n'
                connection.sendall((request + '\r\n').encode() + body)
                response = http.client.HTTPResponse(connection, method='PUT')
                response.begin()
                response.read()
                assert response.status == 403, response.status
                response.close()
                try:
                    connection.sendall(f'GET /{bucket}/{key} HTTP/1.1\r\nHost: {target.netloc}\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n'.encode())
                    response = http.client.HTTPResponse(connection, method='GET')
                    response.begin()
                    assert response.status == 200 and response.read() == expected
                    response.close()
                    outcome = 'reused'
                except (ConnectionResetError, BrokenPipeError, http.client.RemoteDisconnected):
                    outcome = 'closed'
                assert outcome == ('reused' if variant == 'full' else 'closed'), (variant, outcome)
                outcomes[outcome] += 1
        print(json.dumps({'variant': variant, 'rounds': 20, 'outcomes': outcomes}), flush=True)
finally:
    client.delete_object(Bucket=bucket, Key=key)
    client.close()
