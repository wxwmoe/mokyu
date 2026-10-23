"""Independent raw response caches on two actual PostgreSQL data nodes."""
import gzip
import os
from urllib.parse import urlsplit

import boto3
import brotli
import requests
import zstandard
from botocore.config import Config

from tests.storage.source_quarantine import Fixture


def main():
    fixture = Fixture()
    assert fixture.backend, 'cluster fixture requires an isolated S3 backend'
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, pack_range_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    domain = fixture.name + '.example.test'
    fixture.cli('domain', 'set', domain, fixture.name)
    fixture.cli('bucket', 'cors', fixture.name, '-', value=[dict(origins=['https://client.test'],
        methods=['GET', 'HEAD'], headers=['range'], expose=['ETag'], max_age=60)])
    endpoints = [os.environ['MOKYU_TEST_ENDPOINT'], os.environ['MOKYU_TEST_PEER_ENDPOINT']]
    nodes = [urlsplit(endpoint).hostname for endpoint in endpoints]
    clients = [boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
        aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key=fixture.credentials['secret_key'],
        config=Config(signature_version='s3v4', retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
            request_checksum_calculation='when_required', response_checksum_validation='when_required')) for endpoint in endpoints]
    body = b'Independent data nodes share authority but own their cached response bytes.\n' * 1600
    fixture.client.put_object(Bucket=fixture.name, Key='cached', Body=body, ACL='public-read', ContentType='text/plain')

    def status(node):
        response = requests.get(f'http://{nodes[node]}:9002/status', timeout=15)
        response.raise_for_status()
        return response.json()

    def read(node, encoding='identity', web=False, method='GET', **headers):
        base = f'http://{nodes[node]}:9001' if web else endpoints[node] + '/' + fixture.name
        if web:
            headers['Host'] = domain
        response = requests.request(method, base + '/cached', stream=True,
            headers={'Accept-Encoding': encoding, 'Origin': 'https://client.test', **headers}, timeout=15)
        data = response.raw.read(decode_content=False)
        response.close()
        codec = response.headers.get('Content-Encoding', 'identity')
        if data:
            data = {'identity': lambda value: value, 'gzip': gzip.decompress, 'br': brotli.decompress,
                'zstd': lambda value: zstandard.ZstdDecompressor().stream_reader(value).read()}[codec](data)
        return response, data

    for node in range(2):
        assert status(node)['response_cache']['limit_bytes'] == 4 * 1024 * 1024
        for _ in range(3):
            assert clients[node].get_object(Bucket=fixture.name, Key='cached')['Body'].read() == body
        for encoding in ('identity', 'gzip', 'br', 'zstd'):
            source_before = status(node)
            for _ in range(3):
                reply, data = read(node, encoding)
                assert reply.status_code == 200 and data == body, (node, encoding, reply.status_code, reply.headers)
            source_after = status(node)
            assert source_after['backend_gets'] == source_before['backend_gets'], 'node reread an identity source for encoding'
            assert source_after['cache_hits'] == source_before['cache_hits'], 'node reread a disk cache for encoding'
            before = status(node)
            reply, data = read(node, encoding, web=True)
            assert data == body and reply.headers['Access-Control-Allow-Origin'] == 'https://client.test'
            after = status(node)
            assert after['response_cache']['hits'] == before['response_cache']['hits'] + 1
            assert after['backend_gets'] == before['backend_gets'] and after['cache_hits'] == before['cache_hits']
            head, data = read(node, encoding, method='HEAD')
            assert not data and head.headers['Content-Length'] == reply.headers['Content-Length']
        reply, data = read(node, Range='bytes=5-79')
        assert reply.status_code == 206 and data == body[5:80]
    print('PASS both data nodes reuse raw identity and encoded bodies across S3/Web with CORS, HEAD and Range', flush=True)

    fixture.client.put_object_acl(Bucket=fixture.name, Key='cached', ACL='private')
    for node in range(2):
        assert read(node)[0].status_code == 403 and read(node, 'gzip', web=True)[0].status_code == 403
        assert clients[node].get_object(Bucket=fixture.name, Key='cached')['Body'].read() == body
    signed = [client.generate_presigned_url('get_object', Params={'Bucket': fixture.name, 'Key': 'cached'}) for client in clients]
    fixture.cli('cred', 'disable', fixture.credentials['access_key'])
    assert all(requests.get(url, timeout=15).status_code == 403 for url in signed)
    fixture.cli('cred', 'enable', fixture.credentials['access_key'])
    replacement = body[::-1]
    fixture.client.put_object(Bucket=fixture.name, Key='cached', Body=replacement, ACL='public-read', ContentType='text/plain')
    for node in range(2):
        assert read(node)[1] == replacement and read(node, 'gzip', web=True)[1] == replacement
    fixture.client.delete_object(Bucket=fixture.name, Key='cached')
    assert all(read(node)[0].status_code == 404 for node in range(2))
    print('PASS cached data on both nodes follows current ACL, credential revocation, overwrite and delete', flush=True)


if __name__ == '__main__':
    main()
