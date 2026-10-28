"""Public cache notifications and expiry on two actual API data nodes."""
import os
import time
import uuid
from pathlib import Path
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
                   public_cache_validation='background', public_cache_refresh_interval='30s', public_cache_max_age='60s',
                   read_source_validation='background', source_validation_max_age='60s')
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
            assert read(node).content == body and read(node, web=True).content == body
        before = status(node)
        assert read(node).content == body and read(node, web=True).content == body
        after = status(node)
        assert after['public_read_cache']['hits'] >= before['public_read_cache']['hits'] + 2
        assert after['response_cache']['hits'] >= before['response_cache']['hits'] + 2, (before['response_cache'], after['response_cache'])
        assert after['io']['backend']['read']['started'] == before['io']['backend']['read']['started']
    print('PASS each data node reuses public metadata and immutable response bytes across S3/Web', flush=True)

    database = Database()
    peer = uuid.UUID(status(1)['node_id'])
    before = database.query('SELECT clock_timestamp() AS stamp')[0]['stamp']
    fixture.client.put_object_acl(Bucket=fixture.name, Key='cached', ACL='private')
    assert read(0).status_code == 403
    def applied():
        rows = database.query("SELECT bool_and(t.applied_at IS NOT NULL) AS applied FROM notification_targets t JOIN notifications n ON n.id=t.notification_id WHERE t.node_id=%s AND n.scope='object' AND n.resource_id=%s AND n.object_key='cached' AND n.created_at>=%s", peer, uuid.UUID(fixture.bucket_id), before)
        return rows[0]['applied'] is True
    wait(applied, 'peer did not confirm the ACL notification', seconds=5)
    assert read(1).status_code == 403 and read(1, web=True).status_code == 403
    assert clients[1].get_object(Bucket=fixture.name, Key='cached')['Body'].read() == body
    signed = clients[1].generate_presigned_url('get_object', Params={'Bucket': fixture.name, 'Key': 'cached'})
    fixture.cli('cred', 'disable', fixture.credentials['access_key'])
    assert requests.get(signed, timeout=15).status_code == 403
    fixture.cli('cred', 'enable', fixture.credentials['access_key'])
    print('PASS peer notification applies before the long cache deadline; signed revocation stays immediate', flush=True)

    roots = os.environ['MOKYU_TEST_CONTROL_FAULT_DIRS'].split(',')
    assert len(roots) >= 2, 'the expiry scenario requires every control fault directory'
    markers = [Path(root) / 'notifications-claimed' for root in roots]
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(public_cache_refresh_interval='3s', public_cache_max_age='3s', source_validation_max_age='3s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    fixture.client.put_object_acl(Bucket=fixture.name, Key='cached', ACL='public-read')
    wait(lambda: read(1).status_code == 200 and read(1, web=True).status_code == 200,
         'peer did not restore public authority')
    time.sleep(3.1)
    assert read(1).content == body and read(1, web=True).content == body
    try:
        for marker in markers:
            marker.with_suffix('.hit').unlink(missing_ok=True)
            marker.touch()
        fixture.client.put_object_acl(Bucket=fixture.name, Key='cached', ACL='private')
        wait(lambda: any(marker.with_suffix('.hit').exists() for marker in markers),
             'notification delivery did not pause', seconds=2)
        assert read(1).status_code == 200 and read(1, web=True).status_code == 200
        time.sleep(3.1)
        assert read(1).status_code == 403 and read(1, web=True).status_code == 403
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
            marker.with_suffix('.hit').unlink(missing_ok=True)
    print('PASS deliberately delayed notifications retain the configured hard expiry', flush=True)

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
    for node in range(2):
        before = status(node)['rpc']['summary']['requests']
        assert read(node).content == body
        assert read(node, web=True).content == body
        assert status(node)['rpc']['summary']['requests'] >= before + 2
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
    print('PASS body misses confirm current chunk sources through RPC and recover after health restoration', flush=True)

    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, pack_range_enabled=False, read_integrity='off',
                   read_source_validation='strict', public_cache_validation='strict')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    key = 'unchecked'
    body = b'Optional read digests stay behind per-node profile generations.\n' * 1000
    fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read')
    def raw_path():
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
        def checked():
            assert read(node).content == body
            return status(node)['read_checks']['verified'] > before
        wait(checked, 'data node did not complete its background digest', seconds=45)
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
