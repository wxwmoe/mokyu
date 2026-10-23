"""Explicit public-authority windows on two actual PostgreSQL data nodes."""
import os
import time
from urllib.parse import urlsplit

import boto3
import requests
from botocore.config import Config

from tests.storage.source_quarantine import Fixture, wait
from tests.recovery.source_repair import Database


def main():
    fixture = Fixture()
    assert fixture.backend
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, pack_range_enabled=False, upload_cache=False,
                   public_cache_validation='background', public_cache_refresh_interval='3s', public_cache_max_age='3s',
                   read_source_validation='background', source_validation_max_age='3s')
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

    key = 'cached'

    def read(node, web=False, method='GET', **headers):
        base = f'http://{hosts[node]}:9001' if web else endpoints[node] + '/' + fixture.name
        if web:
            headers['Host'] = domain
        return requests.request(method, base + '/' + key, headers={'Accept-Encoding': 'identity', **headers}, timeout=15)

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

    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=True, public_cache_validation='strict')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    body = b'Cluster source snapshots retain read leases and hard health expiry.\n' * 512
    fixture.client.put_object(Bucket=fixture.name, Key='cached', Body=body, ACL='public-read', CacheControl='no-store')
    def source_stats(value):
        if isinstance(value, dict):
            if 'source_cache' in value:
                return value['source_cache']
            for child in value.values():
                if result := source_stats(child):
                    return result
        return None
    for node in range(2):
        def loaded_source():
            assert read(node).content == body
            assert read(node, web=True).content == body
            return source_stats(status(node))['hits'] > 0
        wait(loaded_source, 'data node did not cache its protected chunk source', seconds=45)
    database = Database()
    chunk, = database.chunks(fixture)
    database.query("UPDATE chunk_encodings SET quarantine_code='hash_mismatch',quarantined_at=created_at WHERE encoding_id=%s", chunk['current_encoding_id'])
    try:
        time.sleep(3.2)
        for node in range(2):
            try:
                assert read(node).status_code >= 400, 'data node served an expired health snapshot'
            except requests.exceptions.ChunkedEncodingError:
                pass
    finally:
        database.query('UPDATE chunk_encodings SET quarantine_code=NULL,quarantined_at=NULL WHERE encoding_id=%s', chunk['current_encoding_id'])
    assert all(read(node).content == body for node in range(2))
    print('PASS both data nodes cache protected chunk sources, enforce health expiry and recover after state restoration', flush=True)

    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, pack_range_enabled=False, read_integrity='off',
                   read_source_validation='strict', public_cache_validation='strict')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    key = 'unchecked'
    body = b'Optional read digests stay behind per-node profile generations.\n' * 1000
    fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read')
    def raw_path():
        import uuid
        row, = database.query('SELECT r.id FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s', uuid.UUID(fixture.bucket_id), key)
        identity = str(row['id']).replace('-', '')
        return fixture.prefix + 'raw/' + identity[-2:] + '/' + identity
    physical = raw_path()
    bad = bytes([body[0] ^ 1]) + body[1:]
    fixture.bytes(physical, bad)
    for node in range(2):
        def weak_ready():
            try:
                return read(node).content == bad
            except requests.exceptions.ChunkedEncodingError:
                return False
        wait(weak_ready, 'data node did not apply optional digest policy', seconds=45)
        for _ in range(3):
            assert read(node).content == bad
        before = status(node)['response_cache']['hits']
        assert read(node).content == bad
        assert status(node)['response_cache']['hits'] > before
    fixture.bytes(physical, body)
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile['read_integrity'] = 'inline'
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    for node in range(2):
        wait(lambda: read(node).content == body, 'new reliable profile reused an unchecked peer cache', seconds=45)
    print('PASS both data nodes fence unchecked response caches when the reliable profile arrives', flush=True)

    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile['read_integrity'] = 'background'
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    body = body[::-1]
    fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read', CacheControl='no-store')
    for node in range(2):
        before = status(node)['read_checks']['verified']
        def background_ready():
            assert read(node).content == body
            return status(node)['read_checks']['verified'] > before
        wait(background_ready, 'data node did not apply background digests', seconds=45)
        wait(lambda: status(node)['read_checks']['active'] == 0, 'peer checks did not drain')
    physical = raw_path()
    bad = bytes([body[0] ^ 1]) + body[1:]
    fixture.bytes(physical, bad)
    assert read(0).content == bad
    wait(lambda: fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=raw')['items'],
         'cluster background check did not isolate confirmed damage')
    for node in range(2):
        assert read(node).status_code >= 400
    print('PASS cluster background detection confirms the backend and commits shared source isolation', flush=True)



if __name__ == '__main__':
    main()
