"""A draining data node persists only its own cache and keeps its lease alive."""
import json
import os
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    assert fixture.backend and database.type == 'postgres'
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_PEER_ENDPOINT'],
                        region_name='us-east-1', aws_access_key_id=fixture.credentials['access_key'],
                        aws_secret_access_key=fixture.credentials['secret_key'],
                        config=fixture.client.meta.config.merge(Config(retries={'total_max_attempts': 1})))
    roots = [Path(value) for value in os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')]
    nodes = [uuid.UUID(json.loads((root.parent / 'data' / 'node.json').read_text())['node_id']) for root in roots]
    signals = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'replacement-control'
    signals.mkdir(exist_ok=True)
    storage = uuid.UUID(fixture.store['id'])
    markers = []

    def configure(**changes):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(changes)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(node, name):
        path = roots[node] / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.with_suffix('.hit').unlink(missing_ok=True)
        path.touch()
        markers.append(path)
        return path

    def signal(name):
        (signals / (name + '.done')).unlink(missing_ok=True)
        (signals / name).touch()

    def done(name):
        wait(lambda: (signals / (name + '.done')).exists(), 'runner did not ' + name, seconds=40)
        (signals / name).unlink()

    def start():
        signal('start-peer')
        done('start-peer')
        def ready():
            try:
                return peer.head_object(Bucket=fixture.name, Key='peer')['ContentLength'] == len(data)
            except Exception:
                return False
        wait(ready, 'peer did not restart')

    def pending(node):
        return database.query('SELECT count(*) AS n FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.storage_id=%s AND u.node_id=%s', storage, nodes[node])[0]['n']

    try:
        configure(cdc_enabled=False, pack_range_enabled=False, upload_cache=True,
                  cache_enabled=True, deduplication_enabled=False)
        data = os.urandom(256 * 1024)
        primary = arm(0, 'raw-upload-before-process')
        held = arm(1, 'raw-upload-before-process')
        fixture.put('primary', data)
        peer.put_object(Bucket=fixture.name, Key='peer', Body=data)
        for marker in (primary, held):
            wait(marker.with_suffix('.hit').exists, 'owner worker did not pause')
        draining = arm(1, 'runtime-draining')
        signal('stop-peer')
        wait(draining.with_suffix('.hit').exists, 'peer did not enter shutdown')
        heartbeat = database.query('SELECT last_seen_at FROM nodes WHERE id=%s', nodes[1])[0]['last_seen_at']
        draining.unlink()
        wait(lambda: database.query('SELECT last_seen_at FROM nodes WHERE id=%s', nodes[1])[0]['last_seen_at'] > heartbeat,
             'shutdown stopped renewing node membership', seconds=15)
        assert not (signals / 'stop-peer.done').exists() and pending(1) == 1
        held.unlink()
        done('stop-peer')
        assert pending(1) == 0 and pending(0) == 1, 'node shutdown waited for or consumed another owner cache'
        fixture.read('peer', data)
        print('PASS node shutdown renews membership, persists its own raw and excludes other owners', flush=True)
        start()
        primary.unlink()
        fixture.cli('cache', 'flush', '--wait', '--timeout', '20s')
        assert pending(0) == 0

        configure(cdc_enabled=True, pack_enabled=True, pack_max_size='8MiB', pack_upload_flush_timeout='300s')
        chunked = os.urandom(3 * 1024 * 1024)
        peer.put_object(Bucket=fixture.name, Key='chunks', Body=chunked)
        count = database.query('SELECT count(*) AS n FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.storage_id=%s AND u.node_id=%s', storage, nodes[1])[0]['n']
        assert count > 0
        signal('stop-peer')
        done('stop-peer')
        assert database.query('SELECT count(*) AS n FROM pending_uploads u JOIN chunks c ON c.id=u.chunk_id WHERE c.storage_id=%s AND u.node_id=%s', storage, nodes[1])[0]['n'] == 0
        fixture.read('chunks', chunked)
        start()
        print('PASS node shutdown bypasses pack batching and publishes final chunks', flush=True)

        configure(cdc_enabled=False, pack_enabled=False)
        held = arm(1, 'remote-part-before-send')
        upload = peer.create_multipart_upload(Bucket=fixture.name, Key='multipart')['UploadId']
        part = peer.upload_part(Bucket=fixture.name, Key='multipart', UploadId=upload, PartNumber=1, Body=data)
        wait(held.with_suffix('.hit').exists, 'native remote part did not pause')
        draining = arm(1, 'runtime-draining')
        signal('stop-peer')
        wait(draining.with_suffix('.hit').exists, 'part owner did not enter shutdown')
        draining.unlink()
        time.sleep(.2)
        held.unlink()
        done('stop-peer')
        row = database.query('SELECT remote_etag FROM parts WHERE upload_id=%s AND part_number=1', uuid.UUID(upload))[0]
        assert row['remote_etag']
        fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=upload,
                                                MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
        fixture.read('multipart', data)
        start()
        assert not database.query("SELECT object_key FROM staging_objects WHERE object_key NOT LIKE %s", '__mokyu/multipart/fragment/%')
        print('PASS shutdown persists native remote Multipart parts without shared body staging', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
