"""Key replacement with in-flight writes, delayed notifications and an offline peer.

Use two isolated PostgreSQL data nodes and the existing source fixture. The
runner acknowledges stop-peer/start-peer files by stopping/starting its peer.
"""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.exceptions import EndpointConnectionError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    assert fixture.backend and database.type == 'postgres'
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_PEER_ENDPOINT'], region_name='us-east-1',
                        aws_access_key_id=fixture.credentials['access_key'],
                        aws_secret_access_key=fixture.credentials['secret_key'], config=fixture.client.meta.config)
    roots = [Path(value) for value in os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')]
    control = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'replacement-control'
    control.mkdir()
    source = fixture.cli('key', 'create', fixture.name + '-key', 'block')
    fixture.cli('storage', 'key', 'set', fixture.name, source['id'])
    old = os.urandom(8192)
    fixture.put('old', old)
    bucket = uuid.UUID(fixture.bucket_id)

    def keys(name):
        return {str(row['block_key_id']) for row in database.query('''SELECT l.block_key_id FROM objects o
            JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id
            JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, name)}

    def replace(key):
        preview = fixture.api('/api/v1/keys/' + key + '/replace-preview', {})
        result = fixture.api('/api/v1/key-replacements/' + preview['id'], {})
        assert result['cluster'] and not result['pending']
        return preview['target']

    def settled(client, target, name):
        def write():
            client.put_object(Bucket=fixture.name, Key=name, Body=os.urandom(4096))
            return keys(name) == {target}
        wait(write, 'data node did not activate the new key', seconds=30)

    def peer_action(name):
        (control / name).touch()
        wait(lambda: (control / (name + '.done')).exists(), 'runner did not ' + name, seconds=60)

    markers = []
    try:
        marker = fixture.arm('source-before-publish')
        markers.extend(marker.paths)
        body = os.urandom(16384)
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(fixture.put, 'inflight', body)
            wait(lambda: marker.with_suffix('.hit').exists() or pending.done(), 'write did not pin its original encoding')
            assert not pending.done(), pending.exception()
            target = replace(source['id'])
            marker.unlink()
            pending.result(timeout=30)
        assert keys('inflight') == {source['id']}
        fixture.read('inflight', body)
        for client in [fixture.client, peer]:
            settled(client, target, 'settled')
            with client.get_object(Bucket=fixture.name, Key='old')['Body'] as result:
                assert result.read() == old
        print('PASS both nodes activate the new key while an in-flight write retains its original encoding', flush=True)

        for root in roots:
            root.mkdir(parents=True, exist_ok=True)
            marker = root / 'storage-profile-notification'
            marker.with_suffix('.hit').unlink(missing_ok=True)
            marker.touch()
            markers.append(marker)
        target = replace(target)
        wait(lambda: all((root / 'storage-profile-notification.hit').exists() for root in roots), 'profile notifications were not intercepted')
        for index, client in enumerate([fixture.client, peer]):
            settled(client, target, 'periodic-' + str(index))
        for root in roots:
            (root / 'storage-profile-notification').unlink()
        print('PASS periodic refresh activates the key with notification delivery blocked', flush=True)

        peer_action('stop-peer')
        target = replace(target)
        peer_action('start-peer')
        def ready():
            try:
                return peer.list_buckets() is not None
            except EndpointConnectionError:
                return False
        wait(ready, 'peer did not restart', seconds=60)
        settled(peer, target, 'restarted')
        with peer.get_object(Bucket=fixture.name, Key='old')['Body'] as result:
            assert result.read() == old
        assert keys('old') == {source['id']}
        print('PASS an offline peer loads the replacement on startup and still decrypts old data', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
