"""Verify external jobs, permissions and durable multipart recovery on isolated S3 endpoints."""
import json
import os
from pathlib import Path
import uuid

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def run(restart):
    fixture = Fixture()
    database = Database()
    project = fixture.api('/api/v1/projects')[0]['id']
    remote = os.environ.get('MOKYU_TEST_EXTERNAL_PROVIDER', 'mokyu') != 'mokyu'
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
        value = dict(source_bucket=resources[0]['id'], target_bucket=resources[1]['id'],
            source_external=True, target_external=True, operation='copy', scope=scope,
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

    data = bytes(range(256)) * 65536 + b'persisted-boundary'
    source_keys = ['in/large', 'in/empty', 'in/a%2fb', 'in/a+b', 'in/unicode-\u3042']
    if not remote:
        source_keys += ['in/a//b', 'in/../keep']
    expected = {key: data if key.endswith('large') else b'' if key.endswith('empty') else key.encode()
        for key in source_keys}
    for key, value in expected.items():
        put(key, value, public=True)
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
        assert state['starts'] == 1 and state['phase'] == 'done' and state['upload'] is None, state
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
    result.append('active connection deletion is rejected; cancellation aborts known uploads and releases resource references')
    return result
