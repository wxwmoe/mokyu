"""Native final-backend MPU, node-local ACK, fencing, restart and cleanup."""
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    assert fixture.backend and database.type == 'postgres'
    config = fixture.client.meta.config.merge(Config(retries={'total_max_attempts': 1}, read_timeout=180))
    def client(endpoint):
        return boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
            aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key=fixture.credentials['secret_key'], config=config)
    first, peer = client(os.environ['MOKYU_TEST_ENDPOINT']), client(os.environ['MOKYU_TEST_PEER_ENDPOINT'])
    fixture.client = first
    roots = [Path(value) for value in os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')]
    signals = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'replacement-control'
    signals.mkdir(exist_ok=True)
    for root in roots:
        root.mkdir(parents=True, exist_ok=True)
    held = []

    def configure(**settings):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(settings)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(node, name):
        marker = roots[node] / name
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        held.append(marker)
        return marker

    def action(name):
        (signals / name).unlink(missing_ok=True)
        (signals / (name + '.done')).unlink(missing_ok=True)
        (signals / name).touch()
        wait(lambda: (signals / (name + '.done')).exists(), 'runner did not ' + name, seconds=60)
        if name.startswith('start-'):
            selected = first if name == 'start-primary' else peer
            def ready():
                try:
                    return selected.head_bucket(Bucket=fixture.name)['ResponseMetadata']['HTTPStatusCode'] == 200
                except Exception:
                    return False
            wait(ready, 'restarted data listener did not become ready', seconds=45)

    def create(key):
        return dict(Bucket=fixture.name, Key=key, UploadId=first.create_multipart_upload(Bucket=fixture.name, Key=key)['UploadId'])

    def put(selected, args, number, data):
        reply = selected.upload_part(**args, PartNumber=number, Body=data)
        return dict(PartNumber=number, ETag=reply['ETag'])

    def remote(args):
        return database.query('SELECT u.state,u.raw_source_id,u.completion_token,u.output_stream,r.upload_id FROM uploads u LEFT JOIN raw_sources r ON r.id=u.raw_source_id WHERE u.id=%s', uuid.UUID(args['UploadId']))[0]

    def parts(args):
        return database.query('SELECT stream_id,remote_node_id,remote_etag,remote_token FROM parts WHERE upload_id=%s ORDER BY part_number', uuid.UUID(args['UploadId']))

    def done(args):
        return parts(args) and all(row['remote_etag'] for row in parts(args))

    def read(selected, key, expected):
        with selected.get_object(Bucket=fixture.name, Key=key)['Body'] as body:
            assert body.read() == expected

    def complete(selected, args, manifest):
        return selected.complete_multipart_upload(**args, MultipartUpload={'Parts': manifest})

    def local_only(args):
        rows = database.query("SELECT f.staging_storage_id,f.file_id,m.node_id FROM parts p JOIN extents e ON e.stream_id=p.stream_id JOIN fragments f ON f.id=e.fragment_id LEFT JOIN multipart_files m ON m.id=f.file_id WHERE p.upload_id=%s", uuid.UUID(args['UploadId']))
        assert rows and all(row['staging_storage_id'] is None and row['file_id'] and row['node_id'] for row in rows), rows
        assert not database.query("SELECT object_key FROM staging_objects WHERE object_key NOT LIKE '__mokyu/multipart/fragment/%%'")

    try:
        configure(cdc_enabled=False, deduplication_enabled=True, upload_cache=False, pack_range_enabled=False, upload_retention='1h')
        args = create('native')
        left, right = os.urandom(6 * 1024 * 1024 + 17), os.urandom(19000)
        last = put(peer, args, 3, right)
        start = put(first, args, 1, left)
        assert done(args)
        local_only(args)
        source = remote(args)
        key = fixture.prefix + 'raw/' + source['raw_source_id'].hex[-2:] + '/' + source['raw_source_id'].hex
        listed = fixture.backend.list_parts(Bucket=fixture.remote_bucket, Key=key, UploadId=source['upload_id'])['Parts']
        assert [part['PartNumber'] for part in listed] == [1, 3]
        action('kill-primary')
        complete(peer, args, [start, last])
        read(peer, args['Key'], left + right)
        action('start-primary')
        fixture.put('same-content', left + right)
        identities = database.query("SELECT source_id FROM raw_ranges e JOIN objects o ON o.stream_id=e.stream_id WHERE o.bucket_id=%s AND o.object_key IN ('native','same-content')", uuid.UUID(fixture.bucket_id))
        assert len(identities) == 2 and identities[0]['source_id'] == identities[1]['source_id']
        print('PASS native final-backend parts, sparse numbering, no shared raw staging, owner-independent Complete and whole-object dedup', flush=True)

        configure(upload_cache=True, upload_cache_size='32MiB', cache_enabled=False)
        args = create('cached-restart')
        marker = arm(0, 'remote-part-before-send')
        part = put(first, args, 1, left)
        wait(marker.with_suffix('.hit').exists, 'owner did not start native transfer')
        assert parts(args)[0]['remote_etag'] is None
        local_only(args)
        action('kill-primary')
        database.query("UPDATE parts SET remote_expires_at=clock_timestamp()-interval '1 second' WHERE upload_id=%s", uuid.UUID(args['UploadId']))
        marker.unlink()
        with ThreadPoolExecutor(max_workers=1) as pool:
            completion = pool.submit(complete, peer, args, [part])
            wait(lambda: remote(args)['state'] == 'completing', 'peer did not freeze completion')
            time.sleep(1)
            assert not completion.done() and parts(args)[0]['remote_etag'] is None
            action('start-primary')
            assert completion.result(timeout=90)['ResponseMetadata']['HTTPStatusCode'] == 200
        read(peer, args['Key'], left)
        print('PASS local ACK, owner-only pending part, local disk restart recovery and waiting Complete', flush=True)

        args = create('late-part')
        marker = arm(0, 'remote-part-before-send')
        old = put(first, args, 1, b'old generation')
        wait(marker.with_suffix('.hit').exists, 'old part did not pause')
        old_stream = parts(args)[0]['stream_id']
        fresh = put(peer, args, 1, b'new generation')
        wait(lambda: done(args), 'replacement was not uploaded')
        assert database.query("SELECT id FROM stream_leases WHERE stream_id=%s AND lease_type='read' AND expires_at>clock_timestamp()", old_stream)
        database.query("UPDATE streams SET touched_at=clock_timestamp()-interval '2 hours' WHERE id=%s", old_stream)
        fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        assert database.query('SELECT stream_id FROM extents WHERE stream_id=%s', old_stream)
        source = remote(args)
        key = fixture.prefix + 'raw/' + source['raw_source_id'].hex[-2:] + '/' + source['raw_source_id'].hex
        marker.unlink()
        def overwritten():
            rows = fixture.backend.list_parts(Bucket=fixture.remote_bucket, Key=key, UploadId=source['upload_id'])['Parts']
            return rows and rows[0]['ETag'] == old['ETag']
        wait(overwritten, 'late old upload did not reproduce the upstream overwrite')
        assert parts(args)[0]['remote_etag'] == fresh['ETag']
        complete(first, args, [fresh])
        read(peer, args['Key'], b'new generation')
        print('PASS stale UploadPart cannot acknowledge a new generation and InvalidPart safely retransmits the selected source', flush=True)

        configure(upload_cache=False)
        args = create('lost-complete')
        part = put(first, args, 1, b'completed before publication')
        marker = arm(1, 'remote-completed')
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(complete, peer, args, [part])
            wait(marker.with_suffix('.hit').exists, 'upstream completion did not finish')
            action('kill-peer')
            try:
                request.result(timeout=15)
            except Exception:
                pass
        original = remote(args)['completion_token']
        database.query("UPDATE uploads SET completion_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", uuid.UUID(args['UploadId']))
        marker.unlink()
        complete(first, args, [part])
        assert remote(args)['completion_token'] != original
        read(first, args['Key'], b'completed before publication')
        action('start-peer')
        print('PASS lost Complete response recovers by verifying final bytes before fenced publication', flush=True)

        configure(upload_cache=True, upload_cache_size='1MiB', upload_cache_overflow='reject')
        args = create('overflow')
        try:
            put(first, args, 1, b'x' * (2 * 1024 * 1024))
            raise AssertionError('native part bypassed the upload cache limit')
        except ClientError as error:
            assert error.response['Error']['Code'] == 'SlowDown' and error.response['ResponseMetadata']['HTTPStatusCode'] == 503, error.response
        configure(upload_cache_overflow='fallback')
        part = put(first, args, 1, b'x' * (2 * 1024 * 1024))
        assert done(args)
        complete(peer, args, [part])
        read(peer, args['Key'], b'x' * (2 * 1024 * 1024))
        print('PASS native Multipart cache overflow rejects or waits for backend confirmation', flush=True)

        args = create('abort')
        put(first, args, 1, b'abort this incomplete upload')
        source = remote(args)
        key = fixture.prefix + 'raw/' + source['raw_source_id'].hex[-2:] + '/' + source['raw_source_id'].hex
        first.abort_multipart_upload(**args)
        started = time.monotonic()
        try:
            put(peer, args, 2, b'cannot resume an aborted upload')
            raise AssertionError('aborted native MPU accepted another part')
        except ClientError as error:
            assert error.response['Error']['Code'] == 'NoSuchUpload', error.response
        assert time.monotonic() - started < 3
        wait(lambda: remote(args)['raw_source_id'] is None, 'native aborted MPU was not cleaned', seconds=45)
        try:
            fixture.backend.list_parts(Bucket=fixture.remote_bucket, Key=key, UploadId=source['upload_id'])
            raise AssertionError('native Abort retained backend multipart data')
        except ClientError as error:
            assert error.response['Error']['Code'] == 'NoSuchUpload', error.response
        print('PASS Abort releases the upstream MPU and its protected candidate', flush=True)

        configure(upload_cache=False)
        args = create('empty')
        part = put(first, args, 1, b'')
        complete(peer, args, [part])
        read(peer, args['Key'], b'')
        print('PASS empty native Multipart preserves the complete object contract', flush=True)

        args = create('profile-change')
        part = put(first, args, 1, left)
        configure(cdc_enabled=True)
        tail = put(peer, args, 2, right)
        local_only(args)
        complete(peer, args, [part, tail])
        read(first, args['Key'], left + right)
        configure(cdc_enabled=False)
        print('PASS an initialized native MPU keeps its raw layout across profile changes', flush=True)

        configure(upload_cache=True, upload_cache_size='32MiB')
        args = create('maintenance-flush')
        marker = arm(0, 'remote-part-before-send')
        part = put(first, args, 1, left)
        wait(marker.with_suffix('.hit').exists, 'native transfer did not pause before maintenance')
        fixture.cli('maintenance', 'enable')
        task = fixture.cli('cache', 'flush')
        marker.unlink()
        fixture.finished(task)
        assert done(args)
        fixture.cli('maintenance', 'disable')
        wait(lambda: database.query("SELECT bool_and(r.status->>'work'='normal') AS ready FROM node_reports r JOIN nodes n ON n.id=r.node_id WHERE n.enabled")[0]['ready'], 'nodes did not leave maintenance', seconds=45)
        complete(peer, args, [part])
        read(peer, args['Key'], left)
        print('PASS maintenance cache flush drains node-local native Multipart work', flush=True)

        configure(upload_cache=False)
        args = create('low-space')
        part = put(first, args, 1, b'preserve this part')
        original = parts(args)[0]['stream_id']
        path = roots[0].parent / 'config.toml'
        saved = path.read_text()
        limited, count = re.subn(r'^min_free_space\s*=.*$', 'min_free_space = "1000000GiB"', saved, flags=re.MULTILINE)
        if not count:
            limited = saved.replace('[instance]\n', '[instance]\nmin_free_space = "1000000GiB"\n', 1)
        assert limited != saved
        action('kill-primary')
        try:
            path.write_text(limited)
            action('start-primary')
            try:
                put(first, args, 1, b'new part must not fit')
                raise AssertionError('native Multipart bypassed local free space admission')
            except ClientError as error:
                assert error.response['Error']['Code'] == 'SlowDown' and error.response['ResponseMetadata']['HTTPStatusCode'] == 503, error.response
            assert parts(args)[0]['stream_id'] == original
        finally:
            action('kill-primary')
            path.write_text(saved)
            action('start-primary')
        complete(peer, args, [part])
        read(peer, args['Key'], b'preserve this part')
        print('PASS local space rejection preserves the accepted generation for completion', flush=True)

        configure(upload_cache=False, multipart_idle_timeout='1s')
        args = create('expired')
        put(first, args, 1, b'expire this upload')
        source = remote(args)
        key = fixture.prefix + 'raw/' + source['raw_source_id'].hex[-2:] + '/' + source['raw_source_id'].hex
        database.query("UPDATE uploads SET touched_at=clock_timestamp()-interval '8 days' WHERE id=%s", uuid.UUID(args['UploadId']))
        fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        wait(lambda: remote(args)['state'] == 'aborted' and remote(args)['raw_source_id'] is None, 'expired native MPU was not cleaned', seconds=45)
        try:
            fixture.backend.list_parts(Bucket=fixture.remote_bucket, Key=key, UploadId=source['upload_id'])
            raise AssertionError('expired native MPU retained backend data')
        except ClientError as error:
            assert error.response['Error']['Code'] == 'NoSuchUpload', error.response
        print('PASS per-storage inactivity expiry releases native upstream Multipart data', flush=True)
    finally:
        for marker in held:
            marker.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
