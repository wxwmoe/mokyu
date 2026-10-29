"""Exercise durable same-store transfers through the management and S3 APIs.

Uses the explicitly opted-in source-quarantine fixture and fresh test buckets.
"""
import os
import secrets
import uuid
import boto3
import requests

from tests.storage.source_quarantine import Fixture, wait


def run():
    fixture = Fixture()
    if os.environ.get('MOKYU_TEST_REPLICATION_MODE') == 'raw':
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    target = fixture.cli('bucket', 'create', fixture.name + '-target', '--storage', fixture.store['id'])
    target_id, target_name = target['id'], target['name']
    source_id, source_name = fixture.bucket_id, fixture.name
    client = fixture.client
    credentials = fixture.cli('cred', 'create', target_name)
    target_client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
        aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
        config=client.meta.config)
    checks = []

    def request(path, body=None, expected=200):
        return fixture.api(path, value=body, expected=expected)

    def start(scope, **changes):
        body = dict(source_bucket=source_id, target_bucket=target_id, operation='copy',
                    scope=scope, target_prefix='', conflict='report', permissions='inherit')
        body.update(changes)
        task = request('/api/v1/replications', body, 202)['task_id']
        return task

    def finish(task, failed=False):
        value = wait(lambda: (row if (row := request('/api/v1/replications/' + task))['phase']
                             in ('complete', 'cancelled') or row['state'] == 'failed' else None),
                     'replication did not finish', seconds=180)
        assert value['state'] == ('failed' if failed else 'completed'), value
        return value

    def entries(task, limit=100):
        result, after = [], None
        while True:
            path = '/api/v1/replications/' + task + '/items?limit=' + str(limit)
            if after is not None:
                path += '&after=' + str(after)
            page = request(path)
            result.extend(page['items'])
            after = page['next_after']
            if after is None:
                return result

    def body(bucket, key):
        connection = client if bucket == source_name else target_client
        with connection.get_object(Bucket=bucket, Key=key)['Body'] as response:
            return response.read()

    def public(bucket, key):
        connection = client if bucket == source_name else target_client
        grants = connection.get_object_acl(Bucket=bucket, Key=key)['Grants']
        return any(grant['Grantee'].get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers'
                   and grant['Permission'] == 'READ' for grant in grants)

    data = b'replication fixture\x00' * 4096
    client.put_object(Bucket=source_name, Key='in/shared', Body=data, ACL='public-read',
                      ContentType='application/octet-stream', Metadata={'fixture': 'replication'})
    for key in ['in/../keep', 'in/a//b', 'in/%2f', 'in/A.png', 'in/unicode-\u3042', 'in/empty']:
        client.put_object(Bucket=source_name, Key=key, Body=b'' if key.endswith('empty') else key.encode())
    for index in range(131):
        client.copy_object(Bucket=source_name, Key=f'in/repeated/{index:03}',
                           CopySource={'Bucket': source_name, 'Key': 'in/shared'}, ACL='public-read')
    client.put_object(Bucket=source_name, Key='outside', Body=b'not selected')
    target_client.put_object(Bucket=target_name, Key='outside', Body=b'keep target')
    task = start(dict(selection='prefix', prefix='in/'), target_prefix='out/')
    row = finish(task)
    assert row['progress']['completed'] == 138, row
    items = entries(task, 11)
    assert len(items) == 138 and len({item['source_key'] for item in items}) == 138, items
    assert row['progress']['sources'] < 10, row
    assert body(target_name, 'out/shared') == data
    assert public(target_name, 'out/shared')
    repeated = start(dict(selection='objects', keys=['in/shared']), target_key='out/shared')
    assert finish(repeated)['progress']['skipped'] == 1
    assert body(target_name, 'outside') == b'keep target'
    assert body(source_name, 'outside') == b'not selected'
    for suffix in ['../keep', 'a//b', '%2f', 'A.png', 'unicode-\u3042', 'empty']:
        assert body(target_name, 'out/' + suffix) == body(source_name, 'in/' + suffix)
    checks.append('bounded prefix catalog, special keys, empty bodies, physical deduplication and item pagination')

    target_client.put_object_acl(Bucket=target_name, Key='out/shared', ACL='private')
    task = start(dict(selection='objects', keys=['in/shared']), target_key='out/shared')
    assert finish(task)['progress']['completed'] == 1
    assert public(target_name, 'out/shared')
    task = start(dict(selection='objects', keys=['in/shared']), target_key='out/shared', permissions='private')
    finish(task)
    assert not public(target_name, 'out/shared')
    task = start(dict(selection='objects', keys=['in/shared']), target_key='ignored-new', permissions='ignore')
    finish(task)
    assert not public(target_name, 'ignored-new')
    target_client.put_object(Bucket=target_name, Key='ignored-existing', Body=b'old', ACL='public-read')
    task = start(dict(selection='objects', keys=['in/empty']), target_key='ignored-existing', permissions='ignore', conflict='overwrite')
    finish(task)
    assert body(target_name, 'ignored-existing') == b'' and public(target_name, 'ignored-existing')
    task = start(dict(selection='objects', keys=['in/empty']), target_key='forced-public', permissions='public_read')
    finish(task)
    assert public(target_name, 'forced-public')
    checks.append('inherit, ignore, public and private permissions are independent of body overwrite')

    target_client.put_object(Bucket=target_name, Key='conflict', Body=b'target body', ACL='private')
    task = start(dict(selection='objects', keys=['in/shared']), target_key='conflict')
    assert finish(task, failed=True)['progress']['conflicts'] == 1
    assert body(target_name, 'conflict') == b'target body' and not public(target_name, 'conflict')
    task = start(dict(selection='objects', keys=['in/shared']), target_key='conflict', operation='move', conflict='skip')
    assert finish(task)['progress']['skipped'] == 1
    assert body(source_name, 'in/shared') == data
    task = start(dict(selection='objects', keys=['in/empty']), target_key='moved', operation='move')
    assert finish(task)['progress']['completed'] == 1
    assert not client.list_objects_v2(Bucket=source_name, Prefix='in/empty').get('Contents')
    assert body(target_name, 'moved') == b''
    task = start(dict(selection='objects', keys=['missing']))
    assert finish(task, failed=True)['progress']['conflicts'] == 1
    checks.append('conflict reports preserve bodies and ACLs; skipped moves retain their source')

    for scope, changes in [
        (dict(selection='objects', keys=['in/shared', 'in/shared']), {}),
        (dict(selection='prefix', prefix='in/'), dict(target_bucket=source_id, target_prefix='in/copied/')),
        (dict(selection='objects', keys=['in/shared']), dict(target_bucket=source_id, target_key='in/shared')),
    ]:
        value = dict(source_bucket=source_id, target_bucket=target_id, operation='copy', scope=scope)
        value.update(changes)
        request('/api/v1/replications', value, 400)
    checks.append('duplicate selections, overlapping ranges and source clobbering are rejected')

    source = fixture.api('/api/v1/buckets')
    project = next(bucket['project_id'] for bucket in source if bucket['id'] == source_id)
    password = secrets.token_urlsafe(32)
    username = 'replication-' + uuid.uuid4().hex[:12]
    user = request('/api/v1/users', dict(username=username, password=password, role='member', must_change_password=False), 201)
    membership_path = fixture.web + '/api/v1/projects/' + project + '/members/' + user['id']
    membership = dict(role='writer', scope='selected', grants=[
        dict(bucket_id=source_id, actions=['object.read']),
        dict(bucket_id=target_id, actions=['object.write']),
    ])
    response = fixture.session.put(membership_path, headers=fixture.headers, json=membership, timeout=30)
    assert response.status_code == 204, response.text
    member = requests.Session()
    login = member.post(fixture.web + '/api/v1/auth/login', headers={'Origin': fixture.web},
                        json=dict(username=username, password=password), timeout=30)
    assert login.status_code == 200, login.text
    member.headers.update({'Origin': fixture.web, 'X-CSRF-Token': login.json()['csrf_token']})
    value = dict(source_bucket=source_id, target_bucket=target_id, operation='copy',
                 scope=dict(selection='objects', keys=['in/shared']), target_key='member-copy', permissions='inherit')
    response = member.post(fixture.web + '/api/v1/replications', json=value, timeout=30)
    assert response.status_code == 403, response.text
    value['permissions'] = 'ignore'
    identifier = str(uuid.uuid4())
    for _ in range(2):
        response = member.post(fixture.web + '/api/v1/replications', headers={'Idempotency-Key': identifier}, json=value, timeout=30)
        assert response.status_code == 202 and response.json()['task_id'] == identifier, response.text
    finish(identifier)
    assert member.get(fixture.web + '/api/v1/replications/' + identifier, timeout=30).status_code == 200
    assert not public(target_name, 'member-copy')
    response = member.post(fixture.web + '/api/v1/replications', headers={'Idempotency-Key': identifier},
                           json={**value, 'target_key': 'different'}, timeout=30)
    assert response.status_code == 409, response.text
    response = member.post(fixture.web + '/api/v1/replications', json={**value, 'target_key': None, 'scope': dict(selection='all')}, timeout=30)
    assert response.status_code == 403, response.text
    checks.append('member read/write grants, separate ACL/list permissions and idempotent creation')

    client.put_object(Bucket=source_name, Key='equal-size', Body=b'aaaa')
    target_client.put_object(Bucket=target_name, Key='equal-size', Body=b'bbbb', ACL='private')
    task = start(dict(selection='objects', keys=['equal-size']), conflict='report', permissions='public_read')
    assert finish(task, failed=True)['progress']['conflicts'] == 1
    assert body(target_name, 'equal-size') == b'bbbb' and not public(target_name, 'equal-size')
    checks.append('equal lengths do not establish content identity or permit ACL changes')
    task = start(dict(selection='objects', keys=[f'missing/{index:04}/' + 'x' * 900 for index in range(1000)]))
    row = finish(task, failed=True)
    assert row['progress']['conflicts'] == 1000 and row['progress']['sources'] == 0
    assert len(entries(task)) == 1000
    checks.append('a thousand long explicit paths fit the bounded creation contract and paginate completely')
    return checks


if __name__ == '__main__':
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    for check in run():
        print('PASS:', check, flush=True)
