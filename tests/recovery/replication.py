"""Crash, fencing and GC checks for a disposable replication fixture."""
from concurrent.futures import ThreadPoolExecutor
import os
import secrets
import time
import uuid

import boto3
import requests

from tests.storage.source_quarantine import Fixture, wait
from tests.recovery.source_repair import Database


def run(restart):
    fixture = Fixture()
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(upload_retention='1s', unreferenced_grace='1s', min_storage_duration='0s')
    if os.environ.get('MOKYU_TEST_REPLICATION_MODE') == 'raw':
        profile.update(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    target = fixture.cli('bucket', 'create', fixture.name + '-target', '--storage', fixture.store['id'])
    credentials = fixture.cli('cred', 'create', target['name'])
    client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
        aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
        config=fixture.client.meta.config)
    checks = []

    def start(key, *, operation='copy', identifier=None):
        headers = dict(fixture.headers)
        if identifier:
            headers['Idempotency-Key'] = identifier
        return fixture.api('/api/v1/replications', dict(source_bucket=fixture.bucket_id,
            target_bucket=target['id'], operation=operation, scope=dict(selection='objects', keys=[key])),
            expected=202, headers=headers)['task_id']

    def state(task):
        return fixture.api('/api/v1/replications/' + task)

    def command(task, action):
        fixture.api('/api/v1/replications/' + task + '/command', action, expected=202)

    def finish(task, expected='completed'):
        value = wait(lambda: (row if (row := state(task))['phase'] in ('complete', 'cancelled')
                             or row['state'] == 'failed' else None), 'replication stalled')
        assert value['state'] == expected, (value['state'], value['error'], value['progress'])
        return value

    def reached(marker):
        wait(lambda: marker.with_suffix('.hit').exists(), 'replication boundary was not reached')

    def read(key, expected):
        with client.get_object(Bucket=target['name'], Key=key)['Body'] as body:
            assert body.read() == expected

    payload = b'durable replication source\x00' * 1000
    fixture.put('lost-response', payload)
    identifier = str(uuid.uuid4())
    marker = fixture.arm('replication-created')
    try:
        with ThreadPoolExecutor(1) as executor:
            pending = executor.submit(start, 'lost-response', identifier=identifier)
            reached(marker)
            restart(lambda: marker.unlink(missing_ok=True))
            try:
                pending.result(timeout=20)
            except requests.ConnectionError:
                pass
            else:
                raise AssertionError('creation response was not lost')
        assert start('lost-response', identifier=identifier) == identifier
        assert finish(identifier)['progress']['completed'] == 1
        read('lost-response', payload)
    finally:
        marker.unlink(missing_ok=True)
    checks.append('creation committed before a lost response replays to the same task after restart')

    before = set(fixture.keys())
    fixture.put('changing', b'old unique source')
    protected = set(fixture.keys()) - before
    assert protected
    marker = fixture.arm('replication-batch')
    try:
        task = start('changing', operation='move')
        reached(marker)
        command(task, 'pause')
        marker.unlink(missing_ok=True)
        assert state(task)['state'] == 'paused'
        fixture.put('changing', b'new source after scanning')
        restart(lambda: None)
        assert state(task)['state'] == 'paused'
        time.sleep(1.2)
        fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        assert protected <= set(fixture.keys()), 'paused source was collected'
        command(task, 'resume')
        assert finish(task, 'failed')['progress']['conflicts'] == 1
        fixture.read('changing', b'new source after scanning')
        assert not client.list_objects_v2(Bucket=target['name'], Prefix='changing').get('Contents')
        for _ in range(2):
            time.sleep(1.2)
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        assert not protected.intersection(fixture.keys()), 'terminal source protection leaked'
    finally:
        marker.unlink(missing_ok=True)
    checks.append('pause survives restart and GC; a changed source prevents Move and releases its terminal pin')

    fixture.put('cancelled', b'cancel before publication')
    marker = fixture.arm('replication-batch')
    try:
        task = start('cancelled', operation='move')
        reached(marker)
        command(task, 'cancel')
        marker.unlink(missing_ok=True)
        assert finish(task)['phase'] == 'cancelled'
        fixture.read('cancelled', b'cancel before publication')
        assert not client.list_objects_v2(Bucket=target['name'], Prefix='cancelled').get('Contents')
    finally:
        marker.unlink(missing_ok=True)
    checks.append('cancellation fences the previous executor and preserves an unpublished Move source')

    fixture.put('published', payload)
    marker = fixture.arm('replication-published')
    try:
        task = start('published', operation='move')
        reached(marker)
        restart(lambda: marker.unlink(missing_ok=True))
        assert finish(task)['progress']['completed'] == 1
        entries = fixture.api('/api/v1/replications/' + task + '/items')['items']
        assert len(entries) == 1 and entries[0]['output_version']
        read('published', payload)
        assert not fixture.client.list_objects_v2(Bucket=fixture.name, Prefix='published').get('Contents')
    finally:
        marker.unlink(missing_ok=True)
    checks.append('a crash after atomic publication resumes without repeating the completed Move')

    if os.environ.get('MOKYU_TEST_REPLICATION_MODE') == 'raw':
        database = Database()
        data = bytes(range(256)) * 1200
        fixture.put('paged-mapping', data)
        key = b'paged-mapping' if database.type == 'sqlite' else 'paged-mapping'
        original = database.query('''SELECT o.stream_id,r.source_id FROM objects o
            JOIN raw_ranges r ON r.stream_id=o.stream_id WHERE bucket_id=%s AND object_key=%s''',
            uuid.UUID(fixture.bucket_id), key)[0]
        fixture.cli('maintenance', 'enable')
        try:
            database.query('DELETE FROM raw_ranges WHERE stream_id=%s', original['stream_id'])
            parameters = [value for index in range(300) for value in
                          (original['stream_id'], index * 1024, 1024, original['source_id'], index * 1024)]
            database.query('INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset) VALUES '
                           + ','.join(['(%s,%s,%s,%s,%s)'] * 300), *parameters)
            database.query('UPDATE streams SET payload_revision=payload_revision+1 WHERE id=%s', original['stream_id'])
        finally:
            fixture.cli('maintenance', 'disable')
        task = start('paged-mapping')
        assert finish(task)['progress']['verified'] == 1
        read('paged-mapping', data)
        rows = database.query('''SELECT r.offset_bytes,r.length,r.source_id,r.source_offset FROM objects o
            JOIN raw_ranges r ON r.stream_id=o.stream_id WHERE bucket_id=%s AND object_key=%s ORDER BY r.offset_bytes''',
            uuid.UUID(target['id']), key)
        assert len(rows) == 300 and {row['source_id'] for row in rows} == {original['source_id']}
        assert [(row['offset_bytes'], row['length'], row['source_offset']) for row in rows] == [
            (index * 1024, 1024, index * 1024) for index in range(300)]
        checks.append('a 300-range object crosses reference batches without duplicating its one physical dependency')

    fixture.put('revoked', payload)
    project = next(bucket['project_id'] for bucket in fixture.api('/api/v1/buckets') if bucket['id'] == fixture.bucket_id)
    password = secrets.token_urlsafe(32)
    username = 'replication-' + uuid.uuid4().hex[:12]
    user = fixture.api('/api/v1/users', dict(username=username, password=password, role='member', must_change_password=False), 201)
    membership_path = fixture.web + '/api/v1/projects/' + project + '/members/' + user['id']
    membership = dict(role='writer', scope='selected', grants=[
        dict(bucket_id=fixture.bucket_id, actions=['object.read']), dict(bucket_id=target['id'], actions=['object.write'])])
    response = fixture.session.put(membership_path, headers=fixture.headers, json=membership, timeout=30)
    assert response.status_code == 204, response.text
    member = requests.Session()
    login = member.post(fixture.web + '/api/v1/auth/login', headers={'Origin': fixture.web},
                        json=dict(username=username, password=password), timeout=30)
    assert login.status_code == 200, login.text
    member.headers.update({'Origin': fixture.web, 'X-CSRF-Token': login.json()['csrf_token']})
    marker = fixture.arm('replication-batch')
    try:
        response = member.post(fixture.web + '/api/v1/replications', json=dict(source_bucket=fixture.bucket_id,
            target_bucket=target['id'], operation='copy', scope=dict(selection='objects', keys=['revoked']), permissions='ignore'), timeout=30)
        assert response.status_code == 202, response.text
        task = response.json()['task_id']
        reached(marker)
        assert fixture.session.delete(membership_path, headers=fixture.headers, timeout=30).status_code == 204
    finally:
        marker.unlink(missing_ok=True)
    finish(task, 'failed')
    assert not client.list_objects_v2(Bucket=target['name'], Prefix='revoked').get('Contents')
    fixture.read('revoked', payload)
    assert member.post(fixture.web + '/api/v1/replications/' + task + '/command', json='resume', timeout=30).status_code == 403
    assert member.post(fixture.web + '/api/v1/replications/' + task + '/command', json='cancel', timeout=30).status_code == 202
    assert finish(task)['phase'] == 'cancelled'
    checks.append('revoked grants prevent publication and resumption while the owner can still cancel and release pins')
    return checks
