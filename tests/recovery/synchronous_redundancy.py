"""Check strict visibility while a native replica has durable bytes but no receipt."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import BotoCoreError, ClientError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def state(fixture, key):
    obj = fixture.client.get_object(Bucket=fixture.name, Key=key)
    with obj['Body'] as body:
        data = body.read()
    grants = fixture.client.get_object_acl(Bucket=fixture.name, Key=key)['Grants']
    public = any(g['Permission'] == 'READ' and g['Grantee'].get('URI', '').endswith('/AllUsers') for g in grants)
    return data, obj['ETag'], public, obj['Metadata'].get('revision')


def abandoned_admission(source, target, restart):
    db = Database()
    key = 'abandoned-admission'
    marker = source.faults / 'source-before-publish'
    hit = marker.with_suffix('.hit')
    assert not marker.exists() and not hit.exists()
    marker.touch()
    try:
        with ThreadPoolExecutor(max_workers=1) as clients:
            request = clients.submit(source.client.put_object, Bucket=source.name, Key=key, Body=b'never published')
            try:
                wait(hit.exists, 'write did not reach the admitted checkpoint', seconds=90)
                stored_key = key if db.type == 'postgres' else key.encode()
                row = db.query('SELECT id,phase,source_pin FROM replica_publications WHERE object_key=%s', stored_key)[0]
                assert row['phase'] == 'admitted' and row['source_pin'] is not None
                restart(marker.unlink)
            finally:
                marker.unlink(missing_ok=True)
            try:
                request.result(timeout=60)
            except (ClientError, BotoCoreError):
                pass
            else:
                raise AssertionError('interrupted admission returned success')
        def reaped():
            rows = db.query('SELECT phase,source_pin FROM replica_publications WHERE id=%s', row['id'])
            return rows and rows[0]['phase'] == 'aborted' and rows[0]['source_pin'] is None
        wait(reaped, 'abandoned admission retained the source', seconds=180)
        targets = db.query('SELECT phase,candidate FROM replica_targets WHERE publication_id=%s', row['id'])
        assert len(targets) == 1 and targets[0]['phase'] == 'aborted' and targets[0]['candidate'] is None
        for fixture in (source, target):
            try:
                fixture.client.head_object(Bucket=fixture.name, Key=key)
            except ClientError as error:
                assert error.response['ResponseMetadata']['HTTPStatusCode'] == 404, error.response
            else:
                raise AssertionError('abandoned admission installed an object')
    finally:
        marker.unlink(missing_ok=True)
        hit.unlink(missing_ok=True)


def multipart_restart(source, target, marker, hit, restart):
    db = Database()
    key = 'strict-multipart'
    source.client.put_object(Bucket=source.name, Key=key, Body=b'original', ACL='public-read')
    old = state(source, key)
    assert old == state(target, key)
    args = dict(Bucket=source.name, Key=key)
    upload = source.client.create_multipart_upload(**args, ACL='private', Metadata={'revision': 'multipart'})['UploadId']
    args['UploadId'] = upload
    part = source.client.upload_part(**args, PartNumber=1, Body=b'multipart bytes')
    manifest = dict(Parts=[dict(PartNumber=1, ETag=part['ETag'])])
    handle = uuid.UUID(upload)
    marker.touch()
    with ThreadPoolExecutor(max_workers=1) as clients:
        request = clients.submit(source.client.complete_multipart_upload, **args, MultipartUpload=manifest)
        try:
            wait(hit.exists, 'multipart did not reach the replica checkpoint', seconds=90)
            assert not request.done() and state(source, key) == state(target, key) == old
            try:
                source.client.abort_multipart_upload(**args)
            except ClientError as error:
                assert error.response['Error']['Code'] == 'OperationAborted', error.response
            else:
                raise AssertionError('abort discarded an admitted multipart publication')
            def expire():
                row = db.query('SELECT state,output_stream FROM uploads WHERE id=%s', handle)[0]
                assert row['state'] == 'completing' and row['output_stream']
                past = '0' if db.type == 'sqlite' else "'2001-01-01 00:00:00'"
                db.query(f"UPDATE uploads SET completion_expires_at={past} WHERE id=%s AND output_stream=%s",
                         handle, row['output_stream'])
                marker.unlink()
            restart(expire)
        finally:
            marker.unlink(missing_ok=True)
        try:
            request.result(timeout=60)
        except (ClientError, BotoCoreError):
            pass
    wait(lambda: db.query('SELECT state FROM uploads WHERE id=%s', handle)[0]['state'] == 'completed',
         'accepted multipart publication did not recover', seconds=180)
    reply = source.client.complete_multipart_upload(**args, MultipartUpload=manifest)
    assert state(source, key) == state(target, key) == (b'multipart bytes', reply['ETag'], False, 'multipart')
    try:
        source.client.complete_multipart_upload(**args, MultipartUpload=dict(Parts=[dict(PartNumber=1, ETag='"other"')]))
    except ClientError as error:
        assert 400 <= error.response['ResponseMetadata']['HTTPStatusCode'] < 500, error.response
    else:
        raise AssertionError('completed upload accepted another manifest')
    hit.unlink()


def run(restart):
    source, target = Fixture(), Fixture()
    marker = source.faults / 'raw-transfer-written'
    hit = marker.with_suffix('.hit')
    assert not marker.exists() and not hit.exists()
    key = 'strict-publication'
    try:
        for fixture in (source, target):
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(cdc_enabled=False, deduplication_enabled=False,
                           upload_cache=False, cache_enabled=False, write_processing='inline')
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        source.client.put_object(Bucket=source.name, Key=key, Body=b'before', ACL='public-read',
                                 Metadata={'revision': 'one'})
        base = '/api/v1/buckets/' + source.bucket_id + '/redundancy'
        settings = dict(target_bucket=target.bucket_id, mode='synchronous')
        source.api(base + '/preview', {**settings, 'target_external': True}, 400)
        link = source.api(base, settings, 202)['task_id']
        wait(lambda: source.api(base + '/' + link)['status'] == 'healthy', 'initial replica not ready', seconds=180)
        abandoned_admission(source, target, restart)
        multipart_restart(source, target, marker, hit, restart)
        old = state(source, key)
        assert old == state(target, key) and old[2]
        marker.touch()
        with ThreadPoolExecutor(max_workers=1) as clients:
            request = clients.submit(source.client.put_object, Bucket=source.name, Key=key, Body=b'next',
                                     ACL='private', Metadata={'revision': 'two'})
            try:
                wait(hit.exists, 'replica did not reach its physical checkpoint', seconds=90)
                assert not request.done()
                assert state(source, key) == state(target, key) == old
                current = source.api(base + '/' + link)
                assert current['pending'] > 0 and current['status'] != 'healthy'
                source.api(base + '/' + link + '/command', dict(revision=current['revision'], action='pause'), 202)
                assert not request.done()
            finally:
                marker.unlink(missing_ok=True)
            ack = request.result(timeout=180)
        assert state(source, key) == state(target, key) == (b'next', ack['ETag'], False, 'two')
        wait(lambda: source.api(base + '/' + link)['status'] == 'paused', 'link did not drain', seconds=180)
        source.client.put_object(Bucket=source.name, Key=key, Body=b'after pause')
        assert state(target, key)[0] == b'next'
        return ['interrupted admission releases its source and target without publishing',
                'accepted multipart recovers after process death and completion expiry with its original manifest',
                'strict publication holds both mappings until the physical receipt and drains the admitted write on pause']
    finally:
        marker.unlink(missing_ok=True)
        hit.unlink(missing_ok=True)
        for fixture in (source, target):
            fixture.client.close()
            fixture.session.close()
            if fixture.backend:
                fixture.backend.close()
