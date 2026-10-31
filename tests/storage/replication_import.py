"""Verify native import publication and bounded recovery from an external S3 source."""
import json
import os
import random
import secrets
from pathlib import Path
import uuid

import boto3
import requests
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def run(restart):
    fixture = Fixture()
    database = Database()
    mode = os.environ['MOKYU_TEST_IMPORT_MODE']
    assert mode in ('raw', 'chunks', 'pack')
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=mode != 'raw', deduplication_enabled=mode != 'raw',
        pack_enabled=mode == 'pack', compression_enabled=mode == 'pack', pack_range_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    if mode == 'pack':
        key_name = fixture.name + '-key'
        fixture.cli('key', 'create', key_name, 'block', '-a', 'aes-256-gcm')
        preview = fixture.cli('storage', 'rewrite', fixture.name, 'preview', '--key', key_name)
        fixture.finished(fixture.cli('storage', 'rewrite', fixture.name, 'start', '--key', key_name,
            '--confirm', preview['confirmation']))
    project = next(row['project_id'] for row in fixture.api('/api/v1/buckets') if row['id'] == fixture.bucket_id)
    remote = os.environ.get('MOKYU_TEST_EXTERNAL_PROVIDER', 'mokyu') != 'mokyu'
    backend = json.loads(Path(os.environ['MOKYU_TEST_BACKEND']).read_text()) if remote else {}
    source_client = fixture.backend if remote else fixture.client
    source_bucket = fixture.remote_bucket if remote else fixture.name
    source_prefix = fixture.name + '/source/'
    target_prefix = fixture.name + '/target/'
    credentials = backend if remote else fixture.cli('cred', 'create', fixture.name)
    resource = fixture.api('/api/v1/external-buckets', dict(project_id=project,
        name='import-' + uuid.uuid4().hex[:12], connection=dict(
            endpoint_url=backend['endpoint'] if remote else os.environ['MOKYU_TEST_ENDPOINT'],
            region=backend['region'] if remote else 'us-east-1', bucket=source_bucket,
            prefix=source_prefix.rstrip('/'), allow_http=True),
        **{name: credentials[name] for name in ('access_key', 'secret_key')}), expected=201)

    def request(key, target, **changes):
        value = dict(source_bucket=resource['id'], source_external=True,
            target_bucket=fixture.bucket_id, target_external=False, operation='copy',
            scope=dict(selection='objects', keys=[key]), target_key=target_prefix + target,
            conflict='report', permissions='ignore' if remote else 'inherit')
        value.update(changes)
        return value

    def start(key, target, **changes):
        return fixture.api('/api/v1/replications', request(key, target, **changes), expected=202)['task_id']

    def finish(task, failed=False):
        row = wait(lambda: (row if (row := fixture.api('/api/v1/replications/' + task))['phase']
            in ('complete', 'cancelled') or row['state'] == 'failed' else None),
            'native import did not settle', seconds=240)
        assert row['state'] == ('failed' if failed else 'completed'), row
        return row

    def put(key, data, source=True, public=False, label='native-import'):
        client, bucket, prefix = (source_client, source_bucket, source_prefix) if source else (
            fixture.client, fixture.name, target_prefix)
        client.put_object(Bucket=bucket, Key=prefix + key, Body=data,
            ACL='public-read' if public else 'private', ContentType='application/octet-stream',
            CacheControl='max-age=60', ContentLanguage='en', ContentDisposition='inline',
            Metadata={'fixture': label})

    def read(key, expected, public=None, label='native-import'):
        response = fixture.client.get_object(Bucket=fixture.name, Key=target_prefix + key)
        with response['Body'] as stream:
            assert stream.read() == expected
        assert response['Metadata'] == {'fixture': label}
        assert response['ContentType'] == 'application/octet-stream'
        assert response['CacheControl'] == 'max-age=60' and response['ContentLanguage'] == 'en'
        assert response['ContentDisposition'] == 'inline'
        if public is not None:
            grants = fixture.client.get_object_acl(Bucket=fixture.name, Key=target_prefix + key)['Grants']
            actual = any(grant['Grantee'].get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers'
                and grant['Permission'] == 'READ' for grant in grants)
            assert actual == public

    def absent(key):
        rows = fixture.client.list_objects_v2(Bucket=fixture.name, Prefix=target_prefix + key).get('Contents', [])
        assert not any(row['Key'] == target_prefix + key for row in rows), rows

    def imports(task):
        return database.query('''SELECT p.part_stream,i.candidate FROM transfer_imports p
            JOIN transfer_items i ON i.id=p.item_id WHERE i.task_id=%s''', uuid.UUID(task))

    def reached(marker):
        wait(lambda: marker.with_suffix('.hit').exists(), 'import fault point not reached', seconds=180)

    data = random.Random(719).randbytes(16 * 1024 * 1024) + b'native-tail'
    expected = {'in/large': data, 'in/empty': b'', 'in/a%2fb': b'percent',
        'in/a+b': b'plus', 'in/unicode-\u3042': b'unicode'}
    if not remote:
        expected.update({'in/a//b': b'slashes', 'in/../keep': b'dots'})
    for key, value in expected.items():
        put(key, value, public=True)
    if mode == 'pack':
        assert not remote, 'packed reference fixture requires the native source store'
        fixture.finished(fixture.cli('pack', 'run'))
        assert database.query("SELECT COUNT(*) AS n FROM packs WHERE storage_id=%s AND state='ready'", uuid.UUID(fixture.store['id']))[0]['n'] > 0
    put('outside', b'preserve source')
    put('outside', b'preserve target', source=False)
    value = request('unused', 'unused')
    value.update(scope=dict(selection='prefix', prefix='in/'), target_key=None,
        target_prefix=target_prefix + 'out/')
    task = fixture.api('/api/v1/replications', value, expected=202)['task_id']
    assert finish(task)['progress']['completed'] == len(expected)
    for key, value in expected.items():
        read('out/' + key[3:], value, not remote)
    read('outside', b'preserve target', False)
    assert not imports(task)
    result = ['external-to-native prefix/key mapping, multipart bytes, metadata and empty objects']

    same = start('in/large', 'out/large')
    assert finish(same)['progress']['skipped'] == 1
    put('conflict', b'preserve target', source=False)
    conflict = start('in/large', 'conflict')
    assert finish(conflict, True)['progress']['conflicts'] == 1
    read('conflict', b'preserve target', False)
    finish(start('in/large', 'conflict', conflict='overwrite', permissions='ignore'))
    read('conflict', data, False)
    for permission, public in (('private', False), ('public_read', True), ('ignore', False)):
        finish(start('in/empty', permission, permissions=permission))
        read(permission, b'', public)
    result.append('native reuse, conflict preservation, overwrite and independent permission policies')

    def references():
        table, identity = ('raw_ranges', 'source_id') if mode == 'raw' else ('extents', 'chunk_id')
        key = target_prefix + 'conflict'
        return database.query('SELECT e.' + identity + ',e.offset_bytes,e.length FROM ' + table + ''' e
            JOIN objects o ON o.stream_id=e.stream_id WHERE o.bucket_id=%s AND o.object_key=%s ORDER BY e.offset_bytes''',
            uuid.UUID(fixture.bucket_id), key if database.type == 'postgres' else key.encode())

    original = references()
    assert original
    for label, permissions in (('native-import', 'public_read'), ('updated', 'private')):
        if label == 'updated':
            put('in/large', data, public=True, label=label)
        task = start('in/large', 'conflict', permissions=permissions)
        assert finish(task)['progress']['completed'] == 1
        read('conflict', data, permissions == 'public_read', label=label)
        assert references() == original
        assert database.query('''SELECT COUNT(*) AS n FROM transfer_external_parts p
            JOIN transfer_items i ON i.id=p.item_id WHERE i.task_id=%s''', uuid.UUID(task))[0]['n'] == 0
    put('in/large', data, public=True)
    finish(start('in/empty', 'private', permissions='public_read'))
    read('private', b'', True)
    result.append('matching native bodies retain their exact physical references for metadata and ACL changes, including empty objects')
    if mode == 'raw' and not remote:
        paged = bytes(range(256)) * 1200
        put('paged', paged, public=True)
        put('paged', paged, source=False, label='previous')
        original = database.query('''SELECT o.stream_id,r.source_id FROM objects o
            JOIN raw_ranges r ON r.stream_id=o.stream_id WHERE o.bucket_id=%s AND o.object_key=%s''',
            uuid.UUID(fixture.bucket_id), (target_prefix + 'paged') if database.type == 'postgres'
            else (target_prefix + 'paged').encode())[0]
        # Keep one source reference until the isolated fragmented mapping is complete.
        database.query('UPDATE raw_ranges SET length=1024 WHERE stream_id=%s', original['stream_id'])
        for batch_start in range(1, 300, 64):
            values = [(original['stream_id'], index * 1024, 1024, original['source_id'], index * 1024)
                for index in range(batch_start, min(batch_start + 64, 300))]
            database.query('INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset) VALUES ' +
                ','.join(['(%s,%s,%s,%s,%s)'] * len(values)), *(value for row in values for value in row))
        database.query('UPDATE streams SET payload_revision=payload_revision+1 WHERE id=%s', original['stream_id'])
        task = start('paged', 'paged')
        assert finish(task)['progress']['completed'] == 1
        rows = database.query('''SELECT r.offset_bytes,r.length,r.source_id,r.source_offset FROM objects o
            JOIN raw_ranges r ON r.stream_id=o.stream_id WHERE o.bucket_id=%s AND o.object_key=%s
            ORDER BY r.offset_bytes''', uuid.UUID(fixture.bucket_id), (target_prefix + 'paged')
            if database.type == 'postgres' else (target_prefix + 'paged').encode())
        assert len(rows) == 300 and {row['source_id'] for row in rows} == {original['source_id']}
        assert [(row['offset_bytes'], row['length'], row['source_offset']) for row in rows] == [
            (index * 1024, 1024, index * 1024) for index in range(300)]
        read('paged', paged, True)
        result.append('metadata-only import projects 300 raw references across bounded batches without replacing their source')
        output = database.query('SELECT stream_id FROM objects WHERE bucket_id=%s AND object_key=%s',
            uuid.UUID(fixture.bucket_id), (target_prefix + 'paged') if database.type == 'postgres'
            else (target_prefix + 'paged').encode())[0]['stream_id']
        database.query('DELETE FROM raw_ranges WHERE stream_id=%s AND offset_bytes=1024', output)
        task = start('paged', 'paged', permissions='private')
        finish(task, True)
        fixture.api('/api/v1/replications/' + task + '/command', 'cancel', expected=202)
        assert finish(task)['phase'] == 'cancelled'
        database.query('''INSERT INTO raw_ranges(stream_id,offset_bytes,length,source_id,source_offset)
            VALUES(%s,1024,1024,%s,1024)''', output, original['source_id'])
        read('paged', paged, True)
        result.append('invalid target mappings fail visibly during preparation and cancellation preserves the original target')

    for point in ('import-part-written', 'import-part-sealed'):
        marker = fixture.arm(point)
        task = start('in/large', point)
        reached(marker)
        saved = imports(task)[0]
        part = database.query('SELECT etag FROM streams WHERE id=%s', saved['part_stream'])[0]
        assert bool(part['etag']) == point.endswith('sealed')
        sources = database.query('SELECT source_id FROM raw_ranges WHERE stream_id=%s', saved['part_stream']) if mode == 'raw' else []
        fixture.api('/api/v1/account/reauth', dict(password=fixture.password), expected=204)
        response = fixture.session.delete(fixture.web + '/api/v1/external-buckets/' + resource['id'],
            headers=fixture.headers, json=dict(revision=resource['revision']), timeout=30)
        assert response.status_code == 409
        def after_stop():
            marker.unlink(missing_ok=True)
            if point.endswith('written'):
                token = 'owner_lease_token' if database.type == 'postgres' else 'owner_execution_token'
                database.query("UPDATE streams SET state='abandoned',owner_task_id=NULL," + token + '=NULL WHERE id=%s', saved['part_stream'])
        restart(after_stop)
        assert finish(task)['progress']['completed'] == 1
        read(point, data, not remote)
        assert not imports(task)
        key = target_prefix + point
        final = database.query('''SELECT stream_id FROM objects WHERE bucket_id=%s AND object_key=%s''',
            uuid.UUID(fixture.bucket_id), key if database.type == 'postgres' else key.encode())[0]['stream_id']
        assert final == saved['candidate'], 'recovery discarded the root candidate'
        if mode == 'raw' and point.endswith('sealed'):
            retained = database.query('SELECT source_id FROM raw_ranges WHERE stream_id=%s', final)
            assert {row['source_id'] for row in sources} <= {row['source_id'] for row in retained}
    result.append('unsealed part retry and sealed part reuse across real process loss; root candidate preserved')

    for change in ('source', 'target', 'acl'):
        if change == 'acl' and remote:
            continue
        marker = fixture.arm('import-before-final-check')
        destination = 'changed-' + change
        task = start('in/large', destination)
        reached(marker)
        if change == 'source':
            put('in/large', b'replaced source', public=True)
        elif change == 'target':
            put(destination, b'concurrent target', source=False)
        else:
            source_client.put_object_acl(Bucket=source_bucket, Key=source_prefix + 'in/large', ACL='private')
        marker.unlink(missing_ok=True)
        if change == 'target':
            assert finish(task, True)['progress']['conflicts'] == 1
            read(destination, b'concurrent target', False)
        else:
            assert finish(task)['progress']['completed'] == 1
            read(destination, b'replaced source' if change == 'source' else data,
                change == 'source' and not remote)
        assert not imports(task)
        put('in/large', data, public=True)
    result.append('repeated observation catches source replacement and inherited ACL changes; independent target publication remains a conflict')

    marker = fixture.arm('import-before-final-check')
    task = start('in/large', 'quota')
    reached(marker)
    used = database.query("SELECT used_bytes FROM quota_scopes WHERE scope='bucket' AND scope_id=%s", uuid.UUID(fixture.bucket_id))[0]['used_bytes']
    quota_url = fixture.web + '/api/v1/quotas/bucket/' + fixture.bucket_id
    response = fixture.session.put(quota_url, headers=fixture.headers,
        json=dict(logical_bytes_limit=str(used)), timeout=30)
    assert response.status_code == 200, response.text
    marker.unlink(missing_ok=True)
    finish(task, True)
    absent('quota')
    response = fixture.session.put(quota_url, headers=fixture.headers,
        json=dict(logical_bytes_limit=None), timeout=30)
    assert response.status_code == 200, response.text
    fixture.api('/api/v1/replications/' + task + '/command', 'resume', expected=202)
    finish(task)
    read('quota', data, not remote)
    result.append('native logical quota rejects publication and the prepared candidate resumes after quota restoration')

    if not remote:
        probe = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
            aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
            config=fixture.client.meta.config)
        marker = fixture.arm('import-before-final-check')
        task = start('in/large', 'source-denied')
        reached(marker)
        fixture.cli('cred', 'disable', credentials['access_key'])
        def denied():
            try:
                probe.head_object(Bucket=source_bucket, Key=source_prefix + 'in/large')
            except ClientError as error:
                return error.response['ResponseMetadata']['HTTPStatusCode'] in (401, 403)
            return False
        wait(denied, 'source credential disable did not propagate')
        marker.unlink(missing_ok=True)
        finish(task, True)
        absent('source-denied')
        fixture.cli('cred', 'enable', credentials['access_key'])
        wait(lambda: not denied(), 'source credential enable did not propagate')
        fixture.api('/api/v1/replications/' + task + '/command', 'resume', expected=202)
        finish(task)
        read('source-denied', data, True)
        probe.close()
        result.append('source S3 authorization failure settles visibly and resumes the sealed candidate when access returns')

    marker = fixture.arm('import-part-sealed')
    task = start('in/large', 'cancelled')
    reached(marker)
    fixture.api('/api/v1/replications/' + task + '/command', 'cancel', expected=202)
    marker.unlink(missing_ok=True)
    assert finish(task)['phase'] == 'cancelled'
    absent('cancelled')
    assert not imports(task)

    password = secrets.token_urlsafe(32)
    username = 'import-' + uuid.uuid4().hex[:12]
    user = fixture.api('/api/v1/users', dict(username=username, password=password,
        role='member', must_change_password=False), expected=201)
    member_url = fixture.web + '/api/v1/projects/' + project + '/members/' + user['id']
    member = requests.Session()
    login = member.post(fixture.web + '/api/v1/auth/login', headers={'Origin': fixture.web},
        json=dict(username=username, password=password), timeout=30)
    assert login.status_code == 200, login.text
    member.headers.update({'Origin': fixture.web, 'X-CSRF-Token': login.json()['csrf_token']})
    for point in ('import-part-written', 'import-before-final-check'):
        response = fixture.session.put(member_url, headers=fixture.headers, json=dict(role='writer', scope='selected',
            grants=[dict(bucket_id=fixture.bucket_id, actions=['object.write'])],
            external_grants=[dict(bucket_id=resource['id'], actions=['object.read'])]), timeout=30)
        assert response.status_code == 204, response.text
        marker = fixture.arm(point)
        destination = 'revoked-' + point
        response = member.post(fixture.web + '/api/v1/replications',
            json=request('in/large', destination, permissions='ignore'), timeout=30)
        assert response.status_code == 202, response.text
        task = response.json()['task_id']
        reached(marker)
        assert fixture.session.delete(member_url, headers=fixture.headers, timeout=30).status_code == 204
        marker.unlink(missing_ok=True)
        finish(task, True)
        absent(destination)
        assert member.post(fixture.web + '/api/v1/replications/' + task + '/command', json='resume', timeout=30).status_code == 403
        assert member.post(fixture.web + '/api/v1/replications/' + task + '/command', json='cancel', timeout=30).status_code == 202
        assert finish(task)['phase'] == 'cancelled'
        assert not imports(task)
    result.append('cancellation and live membership revocation before part sealing and publication release task candidates')
    if mode == 'pack':
        fixture.finished(fixture.cli('pack', 'run'))
        key = target_prefix + 'out/large'
        assert database.query('''SELECT COUNT(*) AS n FROM objects o JOIN extents e ON e.stream_id=o.stream_id
            JOIN chunks c ON c.id=e.chunk_id JOIN packs p ON p.id=c.pack_id
            WHERE o.bucket_id=%s AND o.object_key=%s AND p.state='ready' ''', uuid.UUID(fixture.bucket_id),
            key if database.type == 'postgres' else key.encode())[0]['n'] > 0
        read('out/large', data, not remote)
        result.append('encrypted packed native targets remain readable after candidate retirement')
    fixture.api('/api/v1/account/reauth', dict(password=fixture.password), expected=204)
    response = fixture.session.delete(fixture.web + '/api/v1/external-buckets/' + resource['id'],
        headers=fixture.headers, json=dict(revision=resource['revision']), timeout=30)
    assert response.status_code == 204, response.text
    return result
