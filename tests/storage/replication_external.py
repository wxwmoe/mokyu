"""Verify external jobs, permissions and durable multipart recovery on isolated S3 endpoints."""
import json
import os
import random
from pathlib import Path
import uuid

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def run(restart):
    if os.environ.get('MOKYU_TEST_IMPORT_MODE'):
        from tests.storage.replication_import import run as native_import
        return native_import(restart)
    fixture = Fixture()
    database = Database()
    project = fixture.api('/api/v1/projects')[0]['id']
    remote = os.environ.get('MOKYU_TEST_EXTERNAL_PROVIDER', 'mokyu') != 'mokyu'
    export = os.environ.get('MOKYU_TEST_EXPORT_MODE', '')
    assert export in ('', 'raw', 'chunks', 'pack')
    if export:
        assert not remote, 'native source fixture uses its ordinary S3 endpoint'
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=export != 'raw', deduplication_enabled=export != 'raw',
            pack_enabled=export == 'pack', compression_enabled=export == 'pack', pack_range_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    client = fixture.backend if remote else fixture.client
    bucket = fixture.remote_bucket if remote else fixture.name
    backend = json.loads(Path(os.environ['MOKYU_TEST_BACKEND']).read_text()) if remote else {}
    credentials = {name: backend[name] for name in ('access_key', 'secret_key')} if remote else {
        name: fixture.credentials[name] for name in ('access_key', 'secret_key')}
    endpoint = json.loads(Path(os.environ['MOKYU_TEST_BACKEND']).read_text())['endpoint'] if remote else os.environ['MOKYU_TEST_ENDPOINT']
    scopes = (fixture.name + '/source/', fixture.name + '/target/')
    resources = [fixture.api('/api/v1/external-buckets', dict(project_id=project,
        name='transfer-' + uuid.uuid4().hex[:12], connection=dict(endpoint_url=endpoint,
            region=backend['region'] if remote else 'us-east-1', bucket=bucket, prefix=scope.rstrip('/'), allow_http=True),
        **credentials), expected=201) for scope in scopes]

    def create(scope, **changes):
        scope = dict(scope)
        if export:
            if scope['selection'] == 'prefix':
                scope['prefix'] = scopes[0] + scope['prefix']
            elif scope['selection'] == 'objects':
                scope['keys'] = [scopes[0] + key for key in scope['keys']]
            else:
                assert False, 'shared fixture requires a bounded native source scope'
        value = dict(source_bucket=fixture.bucket_id if export else resources[0]['id'], target_bucket=resources[1]['id'],
            source_external=not bool(export), target_external=True, operation='copy', scope=scope,
            conflict='report', permissions='ignore' if remote else 'inherit')
        value.update(changes)
        return fixture.api('/api/v1/replications', value, expected=202)['task_id']

    def finish(task, failed=False):
        row = wait(lambda: (row if (row := fixture.api('/api/v1/replications/' + task))['phase']
            in ('complete', 'cancelled') or row['state'] == 'failed' else None),
            'external task did not settle', seconds=240)
        assert row['state'] == ('failed' if failed else 'completed'), row
        return row

    def entries(task):
        return fixture.api('/api/v1/replications/' + task + '/items?limit=100')['items']

    def put(key, body, source=True, public=False):
        client.put_object(Bucket=bucket, Key=scopes[0 if source else 1] + key, Body=body,
            ACL='public-read' if public else 'private', ContentType='application/octet-stream',
            Metadata={'fixture': 'external-transfer'})

    def body(key, source=False):
        with client.get_object(Bucket=bucket, Key=scopes[0 if source else 1] + key)['Body'] as response:
            return response.read()

    def public(key):
        grants = client.get_object_acl(Bucket=bucket, Key=scopes[1] + key)['Grants']
        return any(g['Grantee'].get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers'
            and g['Permission'] == 'READ' for g in grants)

    data = random.Random(813).randbytes(16 * 1024 * 1024) + b'persisted-boundary'
    source_keys = ['in/large', 'in/empty', 'in/a%2fb', 'in/a+b', 'in/unicode-\u3042']
    if not remote:
        source_keys += ['in/a//b', 'in/../keep']
    expected = {key: data if key.endswith('large') else b'' if key.endswith('empty') else key.encode()
        for key in source_keys}
    for key, value in expected.items():
        put(key, value, public=True)
    if export == 'raw':
        key = 'in/multipart'
        upload = client.create_multipart_upload(Bucket=bucket, Key=scopes[0] + key,
            ACL='public-read', ContentType='application/octet-stream', Metadata={'fixture': 'native-parts'})['UploadId']
        parts = []
        length = 5 * 1024 * 1024 + 11
        for start in range(0, len(data), length):
            number = len(parts) + 1
            part = client.upload_part(Bucket=bucket, Key=scopes[0] + key, UploadId=upload,
                PartNumber=number, Body=data[start:start + length])
            parts.append(dict(PartNumber=number, ETag=part['ETag']))
        client.complete_multipart_upload(Bucket=bucket, Key=scopes[0] + key,
            UploadId=upload, MultipartUpload=dict(Parts=parts))
        expected[key] = data
    if export == 'pack':
        fixture.finished(fixture.cli('pack', 'run'))
        key_name = fixture.name + '-key'
        fixture.cli('key', 'create', key_name, 'block', '-a', 'aes-256-gcm')
        preview = fixture.cli('storage', 'rewrite', fixture.name, 'preview', '--key', key_name)
        fixture.finished(fixture.cli('storage', 'rewrite', fixture.name, 'start', '--key', key_name, '--confirm', preview['confirmation']))
        assert database.query("SELECT COUNT(*) AS n FROM packs WHERE storage_id=%s AND state='ready'", uuid.UUID(fixture.store['id']))[0]['n'] > 0
    put('outside', b'keep source')
    put('outside', b'keep target', source=False)
    task = create(dict(selection='prefix', prefix='in/'), target_prefix='out/')
    row = finish(task)
    assert row['progress']['completed'] == len(expected), row
    for key, value in expected.items():
        destination = 'out/' + key[3:]
        assert body(destination) == value
        if not remote:
            assert public(destination)
    assert body('outside') == b'keep target' and body('outside', True) == b'keep source'
    assert database.query('SELECT COUNT(*) AS n FROM transfer_connections WHERE task_id=%s', uuid.UUID(task))[0]['n'] == 0
    result = ['external prefix/key mapping, multipart body/metadata and empty-object verification',
        'provider ACL unsupported: explicit ignore policy' if remote else 'inherited public-read ACL verified']

    same = create(dict(selection='objects', keys=['in/large']), target_key='out/large')
    assert finish(same)['progress']['skipped'] == 1
    assert entries(same)[0]['state'] == 'skipped'
    put('conflict', b'preserve different bytes', source=False)
    conflicting = create(dict(selection='objects', keys=['in/large']), target_key='conflict')
    assert finish(conflicting, failed=True)['progress']['conflicts'] == 1
    assert body('conflict') == b'preserve different bytes'
    if not remote:
        assert not public('conflict')
    overwritten = create(dict(selection='objects', keys=['in/large']), target_key='conflict', conflict='overwrite', permissions='ignore')
    assert finish(overwritten)['progress']['completed'] == 1
    assert body('conflict') == data
    if not remote:
        private = create(dict(selection='objects', keys=['in/empty']), target_key='forced-private', permissions='private')
        finish(private)
        assert body('forced-private') == b'' and not public('forced-private')
    result.append('same-body reuse, conflict preservation and overwrite')

    if not export:
        for key in ('online/a', 'online/z'):
            put(key, b'initial observation', public=not remote)
        marker = fixture.arm('external-finalized')
        try:
            task = create(dict(selection='prefix', prefix='online/'), target_prefix='online-out/')
            wait(lambda: marker.with_suffix('.hit').exists(), 'first observation was not finalized')
            put('online/a', b'new observed body', public=False)
            put('online/-late', b'new key behind the first listing')
            marker.unlink(missing_ok=True)
            final = finish(task)
            assert final['sealed'] and not final['captured'] and final['progress']['completed'] == 3, final
            assert body('online-out/a') == b'new observed body'
            assert body('online-out/-late') == b'new key behind the first listing'
            assert body('online-out/z') == b'initial observation'
            if not remote:
                assert not public('online-out/a')
        finally:
            marker.unlink(missing_ok=True)
        put('online-race', b'first observed body')
        marker = fixture.arm('external-finalized')
        try:
            task = create(dict(selection='objects', keys=['online-race']), target_key='online-race')
            wait(lambda: marker.with_suffix('.hit').exists(), 'target race boundary was not reached')
            put('online-race', b'independent target write', source=False)
            put('online-race', b'new source observation')
            marker.unlink(missing_ok=True)
            assert finish(task, failed=True)['progress']['conflicts'] == 1
            assert body('online-race') == b'independent target write'
        finally:
            marker.unlink(missing_ok=True)
        result.append('repeated external observation catches replacements and earlier keys while preserving an independently replaced target')
    else:
        put('sealed/a', b'bounded native source')
        page, seal = fixture.arm('replication-page'), fixture.arm('replication-sealed')
        try:
            task = create(dict(selection='prefix', prefix='sealed/'), target_prefix='sealed-out/')
            wait(lambda: page.with_suffix('.hit').exists(), 'native export page was not reached')
            put('sealed/z', b'captured native source')
            page.unlink(missing_ok=True)
            wait(lambda: seal.with_suffix('.hit').exists(), 'native export did not seal')
            put('sealed/z', b'newer than the sealed export')
            seal.unlink(missing_ok=True)
            assert finish(task)['progress']['completed'] == 2
            assert body('sealed-out/z') == b'captured native source'
            assert body('sealed/z', True) == b'newer than the sealed export'
        finally:
            page.unlink(missing_ok=True)
            seal.unlink(missing_ok=True)
        result.append('native export catches keys beyond the scan bound and reads its sealed retired source directly')

    for point in ('external-upload-started', 'external-part-uploaded', 'external-upload-completed') if export else ():
        marker = fixture.arm(point)
        destination = 'changed-source-' + point
        task = create(dict(selection='objects', keys=['in/large']), target_key=destination)
        wait(lambda: marker.with_suffix('.hit').exists(), 'source-change fault point not reached', seconds=180)
        assert database.query('SELECT COUNT(*) AS n FROM transfer_items WHERE task_id=%s AND source_pin IS NOT NULL', uuid.UUID(task))[0]['n'] == 1
        put('in/large', b'new source', public=True)
        marker.unlink(missing_ok=True)
        if point == 'external-upload-completed':
            assert finish(task, failed=True)['progress']['conflicts'] == 1
        else:
            assert finish(task)['progress']['completed'] == 1
            assert body(destination) == b'new source' and public(destination)
        assert body('in/large', True) == b'new source'
        assert not client.list_multipart_uploads(Bucket=bucket, Prefix=scopes[1] + destination).get('Uploads')
        assert database.query('SELECT COUNT(*) AS n FROM transfer_items WHERE task_id=%s AND source_pin IS NOT NULL', uuid.UUID(task))[0]['n'] == 0
        put('in/large', data, public=True)
    if export:
        marker = fixture.arm('external-part-uploaded')
        task = create(dict(selection='objects', keys=['in/large']), target_key='changed-while-paused')
        wait(lambda: marker.with_suffix('.hit').exists(), 'pause fault point not reached', seconds=180)
        fixture.api('/api/v1/replications/' + task + '/command', 'pause', expected=202)
        marker.unlink(missing_ok=True)
        wait(lambda: fixture.api('/api/v1/replications/' + task)['state'] == 'paused', 'task did not pause')
        put('in/large', b'changed between work assignments', public=True)
        fixture.api('/api/v1/replications/' + task + '/command', 'resume', expected=202)
        assert finish(task)['progress']['completed'] == 1
        assert body('changed-while-paused') == b'changed between work assignments'
        assert body('in/large', True) == b'changed between work assignments'
        assert not client.list_multipart_uploads(Bucket=bucket, Prefix=scopes[1] + 'changed-while-paused').get('Uploads')
        put('in/large', data, public=True)
        marker = fixture.arm('external-part-uploaded')
        task = create(dict(selection='objects', keys=['in/large']), target_key='changed-acl')
        wait(lambda: marker.with_suffix('.hit').exists(), 'ACL-change fault point not reached', seconds=180)
        client.put_object_acl(Bucket=bucket, Key=scopes[0] + 'in/large', ACL='private')
        marker.unlink(missing_ok=True)
        assert finish(task)['progress']['completed'] == 1
        assert body('changed-acl') == data and not public('changed-acl')
        assert body('in/large', True) == data
        client.put_object_acl(Bucket=bucket, Key=scopes[0] + 'in/large', ACL='public-read')
        result.append('native source changes before sealing catch up and clean known uploads; unconfirmed completed output remains a conflict')

    for point in ('external-part-uploaded', 'external-upload-completed'):
        marker = fixture.arm(point)
        destination = point
        task = create(dict(selection='objects', keys=['in/large']), target_key=destination)
        wait(lambda: marker.with_suffix('.hit').exists(), 'external fault point not reached', seconds=180)
        response = fixture.session.delete(fixture.web + '/api/v1/external-buckets/' + resources[1]['id'],
            headers=fixture.headers, json=dict(revision=resources[1]['revision']), timeout=30)
        assert response.status_code == 409, response.status_code
        saved = database.query('SELECT e.state FROM transfer_external_items e JOIN transfer_items i ON i.id=e.item_id WHERE i.task_id=%s', uuid.UUID(task))[0]['state']
        state = saved if isinstance(saved, dict) else json.loads(saved)
        assert state['upload'] and state['starts'] == 1, state
        count = database.query('SELECT COUNT(*) AS n FROM transfer_external_parts p JOIN transfer_items i ON i.id=p.item_id WHERE i.task_id=%s', uuid.UUID(task))[0]['n']
        assert count == (0 if point.endswith('part-uploaded') else 3), count
        restart(lambda: marker.unlink(missing_ok=True))
        final = finish(task)
        assert final['progress']['completed'] == 1 and body(destination) == data, final
        if not remote:
            assert public(destination)
        saved = database.query('SELECT e.state FROM transfer_external_items e JOIN transfer_items i ON i.id=e.item_id WHERE i.task_id=%s', uuid.UUID(task))[0]['state']
        state = saved if isinstance(saved, dict) else json.loads(saved)
        assert state['starts'] == int(bool(export)) and state['phase'] == 'done' and state['upload'] is None, state
        assert not state['cleanup_pending'], state
        if not export:
            assert state['previous']['object'] == state['output'], 'comparison replaced its confirmed target'
    result.append('process crashes after part upload and remote completion resume persisted receipts without a new upload')
    marker = fixture.arm('external-part-uploaded')
    task = create(dict(selection='objects', keys=['in/large']), target_key='cancelled')
    wait(lambda: marker.with_suffix('.hit').exists(), 'cancellation fault point not reached', seconds=180)
    fixture.api('/api/v1/replications/' + task + '/command', 'cancel', expected=202)
    marker.unlink(missing_ok=True)
    assert finish(task)['phase'] == 'cancelled'
    assert body('in/large', True) == data
    assert not client.list_multipart_uploads(Bucket=bucket, Prefix=scopes[1] + 'cancelled').get('Uploads')
    for resource in resources:
        response = fixture.session.delete(fixture.web + '/api/v1/external-buckets/' + resource['id'],
            headers=fixture.headers, json=dict(revision=resource['revision']), timeout=30)
        assert response.status_code == 204, response.status_code
    if export == 'raw':
        assert database.query('SELECT COUNT(*) AS n FROM transfer_sources WHERE task_id=%s AND source_raw IS NOT NULL', uuid.UUID(task))[0]['n'] == 0
        assert database.query('SELECT COUNT(*) AS n FROM transfer_spans s JOIN transfer_sources u ON u.id=s.unit_id WHERE u.task_id=%s', uuid.UUID(task))[0]['n'] == 0
        result.append('unaligned native multipart ranges use persisted source proofs; cancellation releases proofs and physical protection')
    result.append('active connection deletion is rejected; cancellation aborts known uploads and releases resource references')
    return result
