"""Background raw deduplication across two data nodes, leases and pinned reads.

Use the raw_cluster fixture, with both data fault directories in
MOKYU_TEST_SOURCE_FAULT_DIRS. Only an isolated PostgreSQL database is supported.
"""
import os
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fault, Fixture, wait


def main():
    fixture = Fixture()
    database = Database()
    assert database.type == 'postgres' and fixture.backend is not None
    roots = [Path(value) for value in os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')]
    assert len(roots) == 2
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_PEER_ENDPOINT'], region_name='us-east-1',
        aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key=fixture.credentials['secret_key'],
        config=fixture.client.meta.config)
    bucket = uuid.UUID(fixture.bucket_id)
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, deduplication_enabled=True, pack_range_enabled=False, write_processing='background')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(name):
        paths = [root / name for root in roots]
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.with_suffix('.hit').unlink(missing_ok=True)
            path.touch()
        return Fault(paths)

    def mapping(key):
        rows = database.query('''SELECT o.stream_id,r.id,s.payload_revision FROM objects o
            JOIN streams s ON s.id=o.stream_id JOIN raw_ranges e ON e.stream_id=s.id
            JOIN raw_sources r ON r.id=e.source_id WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, key)
        assert len(rows) == 1, rows
        return rows[0]

    def drained():
        return not database.query('SELECT w.stream_id FROM raw_processing w JOIN streams s ON s.id=w.stream_id WHERE s.bucket_id=%s', bucket)

    data = b'cluster background raw content' * 4096
    try:
        marker = arm('raw-processing-before-hash')
        fixture.put('first', data)
        peer.put_object(Bucket=fixture.name, Key='second', Body=data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'shared work did not start')
        assert mapping('first')['id'] != mapping('second')['id']
        with peer.get_object(Bucket=fixture.name, Key='first')['Body'] as body:
            assert body.read() == data
        fixture.read('second', data)
        marker.unlink()
        wait(drained, 'shared processing did not drain')
        assert mapping('first')['id'] == mapping('second')['id']
        print('PASS shared durable responses are readable before background cross-node deduplication', flush=True)

        marker = arm('raw-processing-before-switch')
        peer.put_object(Bucket=fixture.name, Key='reading', Body=data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'switch did not pause')
        before = mapping('reading')
        read_marker = fixture.arm('read-body')
        with ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(fixture.read, 'reading', data)
            try:
                wait(lambda: read_marker.with_suffix('.hit').exists() or reader.done(), 'cross-node reader did not pin the source')
                assert not reader.done(), reader.exception()
                marker.unlink()
                wait(lambda: database.query('SELECT token FROM raw_processing WHERE stream_id=%s', before['stream_id'])[0]['token'] is None,
                     'shared read lease did not postpone switching')
                assert mapping('reading') == before
            finally:
                marker.unlink(missing_ok=True)
                read_marker.unlink(missing_ok=True)
            reader.result(timeout=30)
        wait(drained, 'shared switch did not resume after the reader')
        assert mapping('reading')['id'] == mapping('first')['id']
        print('PASS shared read leases prevent background source replacement until readers finish', flush=True)

        marker = arm('raw-processing-before-switch')
        fixture.put('fenced', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'fenced work did not pause')
        before = mapping('fenced')
        old_token = database.query('SELECT token FROM raw_processing WHERE stream_id=%s', before['stream_id'])[0]['token']
        owner = next(path for path in marker.paths if path.with_suffix('.hit').exists())
        database.query("UPDATE raw_processing SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE stream_id=%s", before['stream_id'])
        for path in marker.paths:
            if path != owner:
                path.unlink()
        wait(drained, 'peer did not recover expired ownership')
        after = mapping('fenced')
        assert after['stream_id'] == before['stream_id'] and after['payload_revision'] == before['payload_revision'] + 1
        assert after['id'] == mapping('first')['id']
        owner.unlink(missing_ok=True)
        fixture.read('fenced', data)
        assert mapping('fenced') == after
        assert old_token is not None
        print('PASS expired processing is recovered by the peer and a late owner cannot switch twice', flush=True)

        marker = arm('raw-processing-before-switch')
        fixture.put('unleased', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'unleased work did not pause')
        before = mapping('unleased')
        owner = next(path for path in marker.paths if path.with_suffix('.hit').exists())
        database.query('UPDATE raw_processing SET lease_expires_at=NULL WHERE stream_id=%s', before['stream_id'])
        for path in marker.paths:
            if path != owner:
                path.unlink()
        wait(drained, 'single-mode ownership without an expiry was not recovered')
        owner.unlink(missing_ok=True)
        assert mapping('unleased')['id'] == mapping('first')['id']
        print('PASS shared workers recover a persisted claim without a cluster lease', flush=True)

        marker = arm('raw-processing-before-switch')
        fixture.put('disabled-owner', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'disabled-owner work did not pause')
        before = mapping('disabled-owner')
        owner = next(path for path in marker.paths if path.with_suffix('.hit').exists())
        node = uuid.UUID(json.loads((owner.parent.parent / 'data/node.json').read_text())['node_id'])
        database.query('UPDATE nodes SET enabled=false WHERE id=%s', node)
        try:
            owner.unlink()
            wait(lambda: database.query('SELECT last_error FROM raw_processing WHERE stream_id=%s', before['stream_id'])[0]['last_error'],
                 'disabled owner was not rejected')
            assert mapping('disabled-owner') == before
            for path in marker.paths:
                path.unlink(missing_ok=True)
            database.query('UPDATE raw_processing SET next_attempt_at=clock_timestamp() WHERE stream_id=%s', before['stream_id'])
            wait(drained, 'healthy peer did not resume disabled-owner work')
            assert mapping('disabled-owner')['id'] == mapping('first')['id']
        finally:
            database.query('UPDATE nodes SET enabled=true WHERE id=%s', node)
        print('PASS disabled nodes cannot commit background changes and a healthy peer resumes work', flush=True)

        import psycopg
        marker = arm('raw-processing-before-switch')
        fixture.put('connection-wait', data)
        wait(lambda: marker.with_suffix('.hit').exists(), 'connection-wait work did not pause')
        before = mapping('connection-wait')
        time.sleep(25)
        settings = database.settings
        with psycopg.connect(host=settings['host'], port=settings.get('port', 5432), user=settings['user'],
                            password=settings['password'], dbname=settings['name']) as blocker:
            blocker.execute('SELECT stream_id FROM raw_processing WHERE stream_id=%s FOR UPDATE', (before['stream_id'],))
            marker.unlink()
            time.sleep(10)
            blocker.commit()
        wait(drained, 'renewal blocked progress while the processing transaction held the database connection', seconds=5)
        fixture.read('connection-wait', data)
        assert mapping('connection-wait')['id'] == mapping('first')['id']
        print('PASS lease renewal and a blocked transaction progress concurrently with one available query connection', flush=True)
    finally:
        for root in roots:
            for name in ('raw-processing-before-hash', 'raw-processing-before-switch', 'read-body'):
                (root / name).unlink(missing_ok=True)


if __name__ == '__main__':
    main()
