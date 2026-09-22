"""Real SigV4 chunk chains and checksum trailers; use only an isolated test bucket.

MGW_TEST_ENDPOINT and MGW_TEST_CREDENTIALS match integration.py.
"""
import base64
import hashlib
import hmac
import json
import os
import zlib
from pathlib import Path
import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.config import Config

endpoint = os.environ['MGW_TEST_ENDPOINT']
credential = json.loads(Path(os.environ['MGW_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
credentials = Credentials(credential['access_key'], credential['secret_key'])
client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                      aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key,
                      config=Config(retries={'max_attempts': 0}, s3={'addressing_style': 'path'}))


def sha(value):
    return hashlib.sha256(value).hexdigest()


def sign(key, value):
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


def upload(mode, bad=None):
    payload = b'aws streaming boundary\x00' * 4097
    trailer = mode != 'signed'
    signed = mode != 'unsigned-trailer'
    marker = 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' if signed else 'STREAMING-UNSIGNED-PAYLOAD'
    if trailer:
        marker += '-TRAILER'
    key = 'streaming-' + mode
    client.put_object(Bucket=bucket, Key=key, Body=b'old verified value')
    headers = {'Content-Encoding': 'aws-chunked', 'X-Amz-Content-SHA256': marker,
               'X-Amz-Decoded-Content-Length': str(len(payload)), 'Content-Type': 'application/octet-stream'}
    if trailer:
        headers['X-Amz-Trailer'] = 'x-amz-checksum-crc32'
    request = AWSRequest(method='PUT', url=f'{endpoint}/{bucket}/{key}', headers=headers)
    SigV4Auth(credentials, 's3', 'us-east-1').add_auth(request)
    headers = dict(request.headers)
    timestamp = headers['X-Amz-Date']
    scope = timestamp[:8] + '/us-east-1/s3/aws4_request'
    signing = ('AWS4' + credentials.secret_key).encode()
    for piece in [timestamp[:8], 'us-east-1', 's3', 'aws4_request']:
        signing = sign(signing, piece)
    previous = headers['Authorization'].split('Signature=')[1]
    wire = bytearray()
    chunks = [payload[:65536], payload[65536:], b'']
    for i, chunk in enumerate(chunks):
        if signed:
            string = '\n'.join(['AWS4-HMAC-SHA256-PAYLOAD', timestamp, scope, previous, sha(b''), sha(chunk)])
            previous = sign(signing, string).hex()
            signature = '0' * 64 if bad == 'signature' and i == 1 else previous
            wire.extend(f'{len(chunk):x};chunk-signature={signature}\r\n'.encode())
        else:
            wire.extend(f'{len(chunk):x}\r\n'.encode())
        wire.extend(chunk + b'\r\n')
    if trailer and bad != 'missing':
        checksum = base64.b64encode((zlib.crc32(payload) & 0xffffffff).to_bytes(4, 'big')).decode()
        if bad == 'checksum':
            checksum = 'AAAAAA=='
        canonical = 'x-amz-checksum-crc32:' + checksum + '\n'
        wire.extend(canonical.replace('\n', '\r\n').encode())
        if signed:
            signature = sign(signing, '\n'.join(['AWS4-HMAC-SHA256-TRAILER', timestamp, scope, previous, sha(canonical.encode())])).hex()
            wire.extend(('x-amz-trailer-signature:' + signature + '\r\n').encode())
        wire.extend(b'\r\n')
    if bad == 'length':
        wire = wire[:-10]
    response = requests.put(request.url, data=bytes(wire), headers=headers, timeout=30)
    if bad:
        assert response.status_code >= 400, (mode, bad, response.status_code)
        expected = b'old verified value'
    else:
        assert response.status_code == 200, (mode, response.status_code, response.text[:400])
        expected = payload
    assert client.get_object(Bucket=bucket, Key=key)['Body'].read() == expected


for mode in ['signed', 'signed-trailer', 'unsigned-trailer']:
    upload(mode)
    upload(mode, 'signature' if mode == 'signed' else 'checksum')
    if mode != 'signed':
        upload(mode, 'missing')
upload('signed', 'length')
print('PASS signed chunks, signed/unsigned checksum trailers, bad signature/checksum/missing trailer/truncation preserve prior generation')
