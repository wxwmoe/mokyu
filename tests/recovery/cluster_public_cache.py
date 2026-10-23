"""Explicit public-authority windows on two actual PostgreSQL data nodes."""
import os
import time
from urllib.parse import urlsplit

import boto3
import requests
from botocore.config import Config

from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture = Fixture()
    assert fixture.backend
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, pack_range_enabled=False, upload_cache=False,
                   public_cache_validation='background', public_cache_refresh_interval='3s', public_cache_max_age='3s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    domain = fixture.name + '.public.test'
    fixture.cli('domain', 'set', domain, fixture.name)
    endpoints = [os.environ['MOKYU_TEST_ENDPOINT'], os.environ['MOKYU_TEST_PEER_ENDPOINT']]
    hosts = [urlsplit(endpoint).hostname for endpoint in endpoints]
    clients = [boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
        aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key=fixture.credentials['secret_key'],
        config=Config(signature_version='s3v4', retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
            request_checksum_calculation='when_required', response_checksum_validation='when_required')) for endpoint in endpoints]
    body = b'Cluster public snapshots retain explicit expiry and current signed authority.\n' * 1200
    fixture.client.put_object(Bucket=fixture.name, Key='cached', Body=body, ACL='public-read')

    def status(node):
        reply = requests.get(f'http://{hosts[node]}:9002/status', timeout=10)
        reply.raise_for_status()
        return reply.json()

    def read(node, web=False, method='GET', **headers):
        base = f'http://{hosts[node]}:9001' if web else endpoints[node] + '/' + fixture.name
        if web:
            headers['Host'] = domain
        return requests.request(method, base + '/cached', headers={'Accept-Encoding': 'identity', **headers}, timeout=15)

    for node in range(2):
        def loaded():
            replies = [read(node), read(node, web=True)]
            return all(reply.status_code == 200 and reply.content == body for reply in replies) and status(node)['public_read_cache']['entries'] >= 2
        wait(loaded, 'data node did not load the public policy', seconds=45)
        for _ in range(3):
            assert read(node).content == body
        before = status(node)
        assert read(node).content == body and read(node, web=True).content == body
        after = status(node)
        assert after['public_read_cache']['hits'] >= before['public_read_cache']['hits'] + 2
        assert after['response_cache']['hits'] >= before['response_cache']['hits'] + 2
        assert after['backend_gets'] == before['backend_gets']
    print('PASS each data node reuses public metadata and immutable response bytes across S3/Web', flush=True)

    # Refresh the peer's snapshot at the start of its full, explicitly configured window.
    time.sleep(3.1)
    assert read(1).content == body and read(1, web=True).content == body
    fixture.client.put_object_acl(Bucket=fixture.name, Key='cached', ACL='private')
    assert read(0).status_code == 403
    assert read(1).status_code == 200 and read(1, web=True).status_code == 200
    assert clients[1].get_object(Bucket=fixture.name, Key='cached')['Body'].read() == body
    signed = clients[1].generate_presigned_url('get_object', Params={'Bucket': fixture.name, 'Key': 'cached'})
    fixture.cli('cred', 'disable', fixture.credentials['access_key'])
    assert requests.get(signed, timeout=15).status_code == 403
    fixture.cli('cred', 'enable', fixture.credentials['access_key'])
    time.sleep(3.1)
    assert read(1).status_code == 403 and read(1, web=True).status_code == 403
    print('PASS writer-node invalidation is immediate, peer expiry is hard, signed revocation stays immediate', flush=True)

    replacement = body[::-1]
    fixture.client.put_object(Bucket=fixture.name, Key='cached', Body=replacement, ACL='public-read')
    assert read(0).content == replacement
    time.sleep(3.1)
    assert read(1).content == replacement
    fixture.client.delete_object(Bucket=fixture.name, Key='cached')
    assert read(0).status_code == 404
    time.sleep(3.1)
    assert read(1).status_code == 404
    print('PASS overwrite and deletion converge within the peer hard deadline with leases on body misses', flush=True)


if __name__ == '__main__':
    main()
