"""Node-owned local write caches, bounded cross-node GET and read-cache retention."""
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, Fault, wait


def main():
    fixture, database = Fixture(), Database()
    assert fixture.backend and database.type == 'postgres'
    config = fixture.client.meta.config.merge(Config(retries={'total_max_attempts': 1}, read_timeout=30))
    def client(endpoint):
        return boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                            aws_access_key_id=fixture.credentials['access_key'],
                            aws_secret_access_key=fixture.credentials['secret_key'], config=config)
    fixture.client = client(os.environ['MOKYU_TEST_ENDPOINT'])
    peer = client(os.environ['MOKYU_TEST_PEER_ENDPOINT'])
    roots = [Path(value) for value in os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')]
    signals = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'replacement-control'
    signals.mkdir(exist_ok=True)
    bucket, storage = uuid.UUID(fixture.bucket_id), uuid.UUID(fixture.store['id'])
    markers = []
    domain = fixture.name + '.example.test'
    fixture.cli('domain', 'set', domain, fixture.name)
    settings = fixture.api('/api/v1/buckets/' + fixture.bucket_id + '/settings')
    values = {name: settings['bucket'][name] for name in ('revision', 'cors', 'website_enabled', 'index_document', 'error_document', 'public_base_url', 'uploads_paused')}
    values['domains'] = settings['domains']
    values['cors'] = [{'origins': ['https://client.test'], 'methods': ['GET', 'HEAD'], 'headers': ['*'], 'expose': ['Retry-After'], 'max_age': 300}]
    saved = fixture.session.put(fixture.web + '/api/v1/buckets/' + fixture.bucket_id + '/settings', json=values, headers=fixture.headers, timeout=30)
    assert saved.status_code == 200, saved.text

    def configure(**changes):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(changes)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(name):
        paths = [root / name for root in roots]
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.with_suffix('.hit').unlink(missing_ok=True)
            path.touch()
            markers.append(path)
        return Fault(paths)

    def mapping(key):
        return database.query('''SELECT o.stream_id,r.id,r.pending_upload FROM objects o
            JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', bucket, key)[0]

    def path(source, cached=False):
        value = source.hex
        return roots[0].parent / 'data' / ('cache' if cached else 'pending') / 'raw' / value[-2:] / value

    def queued():
        return database.query('SELECT u.* FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id '
                              'WHERE r.storage_id=%s', storage)

    def drain():
        wait(lambda: not queued(), 'local raw uploads did not drain', seconds=150)

    def read(selected, key, expected):
        with selected.get_object(Bucket=fixture.name, Key=key)['Body'] as body:
            assert body.read() == expected

    def pending(call):
        start = time.monotonic()
        try:
            call()
        except ClientError as error:
            response = error.response
            assert response['Error']['Code'] == 'SlowDown', response
            assert response['ResponseMetadata']['HTTPStatusCode'] == 503, response
            headers = response['ResponseMetadata']['HTTPHeaders']
            assert headers['retry-after'] == '60' and headers['cache-control'] == 'no-store', headers
        else:
            raise AssertionError('a non-owner served a pending body')
        assert 2.5 <= time.monotonic() - start < 8

    def action(name):
        (signals / name).unlink(missing_ok=True)
        (signals / (name + '.done')).unlink(missing_ok=True)
        (signals / name).touch()
        wait(lambda: (signals / (name + '.done')).exists(), 'runner did not ' + name, seconds=60)

    try:
        configure(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False,
                  write_processing='inline', upload_cache=True, upload_cache_size='32MiB',
                  cache_enabled=True, cache_soft_limit='64MiB')
        original = os.urandom(1200 * 1024)
        held = arm('raw-upload-before-process')
        fixture.client.put_object(Bucket=fixture.name, Key='cached', Body=original, ACL='public-read')
        source = mapping('cached')['id']
        first = uuid.UUID(json.loads((roots[0].parent / 'data' / 'node.json').read_text())['node_id'])
        assert mapping('cached')['pending_upload']
        assert path(source).read_bytes() == original
        assert queued()[0]['node_id'] == first
        assert not fixture.keys(), 'early ACK uploaded a final object'
        assert not database.query("SELECT object_key FROM staging_objects WHERE object_key NOT LIKE %s", '__mokyu/multipart/fragment/%')
        read(fixture.client, 'cached', original)
        start = time.monotonic()
        assert peer.head_object(Bucket=fixture.name, Key='cached')['ContentLength'] == len(original)
        assert time.monotonic() - start < 2
        with ThreadPoolExecutor(max_workers=4) as pool:
            requests_waiting = [pool.submit(pending, lambda: peer.get_object(Bucket=fixture.name, Key='cached')) for _ in range(4)]
            for request in requests_waiting:
                request.result(timeout=15)
        reply = requests.get(os.environ['MOKYU_TEST_PEER_ENDPOINT'] + '/' + fixture.name + '/cached', headers={'Origin': 'https://client.test'}, timeout=10)
        assert reply.status_code == 503 and reply.headers['Retry-After'] == '60', reply.text
        assert reply.headers['Cache-Control'] == 'no-store'
        assert reply.headers['Access-Control-Allow-Origin'] == 'https://client.test'
        public = requests.get(os.environ['MOKYU_TEST_PEER_ENDPOINT'].replace(':9000', ':9001') + '/cached', headers={'Host': domain, 'Origin': 'https://client.test'}, timeout=10)
        assert public.status_code == 503 and public.headers['Retry-After'] == '60', public.text
        assert public.headers['Access-Control-Allow-Origin'] == 'https://client.test'
        print('PASS local ACK, owner read, metadata-only HEAD and bounded signed/anonymous cross-node GET', flush=True)

        action('kill-primary')
        database.query("UPDATE raw_uploads SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE source_id=%s", source)
        held.unlink(missing_ok=True)
        time.sleep(2)
        assert queued()[0]['node_id'] == first and mapping('cached')['pending_upload']
        assert not fixture.keys(), 'peer claimed a file it does not own'
        action('start-primary')
        drain()
        wait(lambda: path(source, cached=True).exists(), 'pending file did not enter the read cache')
        assert path(source, cached=True).read_bytes() == original
        for selected in (fixture.client, peer):
            read(selected, 'cached', original)
        assert not mapping('cached')['pending_upload']
        print('PASS owner-only recovery and write-to-read cache retention', flush=True)

        held = arm('raw-upload-before-process')
        value = os.urandom(256 * 1024)
        fixture.put('ready-soon', value)
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(read, peer, 'ready-soon', value)
            time.sleep(.4)
            held.unlink(missing_ok=True)
            result.result(timeout=15)
        drain()
        print('PASS waiting GET observes final publication', flush=True)

        held = arm('raw-upload-before-process')
        fixture.client.put_object(Bucket=fixture.name, Key='private-soon', Body=value, ACL='public-read')
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(requests.get, os.environ['MOKYU_TEST_PEER_ENDPOINT'] + '/' + fixture.name + '/private-soon', timeout=15)
            time.sleep(.5)
            fixture.client.put_object_acl(Bucket=fixture.name, Key='private-soon', ACL='private')
            held.unlink(missing_ok=True)
            assert result.result(timeout=20).status_code == 403
        drain()
        print('PASS public permission is checked again after waiting', flush=True)

        configure(cdc_enabled=True, upload_cache=True, pack_enabled=True)
        held = arm('upload-before-work')
        chunked = os.urandom(3 * 1024 * 1024 + 47)
        fixture.put('chunks', chunked)
        pending_rows = database.query('''SELECT p.*,c.storage_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id
            WHERE c.storage_id=%s''', storage)
        assert pending_rows and all(row['node_id'] == first for row in pending_rows)
        assert all(not row['staging_object_key'].startswith('__mokyu/') for row in pending_rows)
        read(fixture.client, 'chunks', chunked)
        pending(lambda: peer.get_object(Bucket=fixture.name, Key='chunks'))
        retained = [roots[0].parent / 'data' / 'pending' / 'chunks' / uuid.UUID(row['staging_object_key'].split('.')[0]).hex[-2:] /
                    (uuid.UUID(row['staging_object_key'].split('.')[0]).hex + '.' + row['staging_object_key'].split('.')[1]) for row in pending_rows]
        held.unlink(missing_ok=True)
        wait(lambda: not database.query('SELECT p.chunk_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE c.storage_id=%s', storage), 'chunks did not drain', seconds=150)
        read(peer, 'chunks', chunked)
        wait(lambda: all(not file.exists() for file in retained), 'released chunks remained pending')
        assert list((roots[0].parent / 'data' / 'cache' / 'chunks').glob('*/*'))
        assert not database.query("SELECT object_key FROM staging_objects WHERE object_key NOT LIKE %s", '__mokyu/multipart/fragment/%')
        print('PASS chunks use owner-scoped local upload/pack work and retain ordinary read-cache files', flush=True)

        configure(cdc_enabled=False, pack_enabled=False)
        held = arm('raw-upload-before-process')
        fixture.put('flush-primary', b'primary flush')
        peer.put_object(Bucket=fixture.name, Key='flush-peer', Body=b'peer flush')
        wait(lambda: len(queued()) == 2, 'both nodes did not queue their local files')
        changed = database.query('SELECT clock_timestamp() AS at')[0]['at']
        fixture.cli('maintenance', 'enable')
        wait(lambda: database.query("SELECT bool_and(r.status->>'work'='maintenance' AND r.sampled_at>=%s) AS ready FROM node_reports r JOIN nodes n ON n.id=r.node_id WHERE n.enabled", changed)[0]['ready'], 'nodes did not enter maintenance', seconds=45)
        task = fixture.cli('cache', 'flush')['task_id']
        held.unlink(missing_ok=True)
        wait(lambda: fixture.cli('task', 'show', task)['state'] in ('completed', 'failed'), 'cluster cache flush did not finish', seconds=120)
        assert fixture.cli('task', 'show', task)['state'] == 'completed'
        assert not queued()
        changed = database.query('SELECT clock_timestamp() AS at')[0]['at']
        fixture.cli('maintenance', 'disable')
        wait(lambda: database.query("SELECT bool_and(r.status->>'work'='normal' AND r.sampled_at>=%s) AS ready FROM node_reports r JOIN nodes n ON n.id=r.node_id WHERE n.enabled", changed)[0]['ready'], 'nodes did not leave maintenance', seconds=45)
        read(peer, 'flush-primary', b'primary flush')
        read(fixture.client, 'flush-peer', b'peer flush')
        print('PASS maintenance flush drains each owner before reporting global completion', flush=True)
        configure(cdc_enabled=True, upload_cache=False, pack_enabled=False)
        parts = [os.urandom(7 * 1024 * 1024 + 17), os.urandom(128 * 1024)]
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='tails')['UploadId']
        manifest = []
        for number, data in enumerate(parts, 1):
            reply = fixture.client.upload_part(Bucket=fixture.name, Key='tails', UploadId=upload,
                                               PartNumber=number, Body=data)
            manifest.append({'PartNumber': number, 'ETag': reply['ETag']})
        tails = database.query('''SELECT f.size,f.staging_object_key FROM fragments f
            JOIN extents e ON e.fragment_id=f.id JOIN parts p ON p.stream_id=e.stream_id
            WHERE p.upload_id=%s''', uuid.UUID(upload))
        assert tails and sum(row['size'] for row in tails) < 8 * 1024 * 1024
        assert all(row['staging_object_key'].startswith('__mokyu/multipart/fragment/') for row in tails)
        assert not database.query('SELECT p.chunk_id FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE c.storage_id=%s', storage)
        action('kill-primary')
        peer.complete_multipart_upload(Bucket=fixture.name, Key='tails', UploadId=upload,
                                       MultipartUpload={'Parts': manifest})
        read(peer, 'tails', b''.join(parts))
        action('start-primary')
        print('PASS final chunks and shared CDC tails complete on a peer without the source node', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
