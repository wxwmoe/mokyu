"""Media publication permissions and public cache invalidation on an isolated backend."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
import requests
from botocore.config import Config


def main():
    assert os.environ['MOKYU_TEST_ALLOW_STATE_CHANGES'] == 'isolated-only'
    panel = os.environ['MOKYU_TEST_WEB']
    endpoint = os.environ['MOKYU_TEST_ENDPOINT']
    public = os.environ['MOKYU_TEST_PUBLIC_WEB']
    name = 'publish-' + uuid.uuid4().hex[:12]
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']

    def cli(*args, value=None):
        return json.loads(subprocess.check_output(command + list(args), text=True,
                          input=json.dumps(value) if value is not None else None, timeout=30))

    def login(user, password):
        session = requests.Session()
        session.headers['Origin'] = panel
        response = session.post(panel + '/api/v1/auth/login', json={'username': user, 'password': password})
        response.raise_for_status()
        session.headers['X-CSRF-Token'] = response.json()['csrf_token']
        return session

    password = os.environ['MOKYU_TEST_PASSWORD']
    admin = login('tester', password)
    admin.post(panel + '/api/v1/account/reauth', json={'password': password}).raise_for_status()
    project = cli('project', 'create', name)['id']
    buckets = []
    clients = []
    for suffix in ('-a', '-b'):
        label = name + suffix
        storage = cli('storage', 'create', label, 'local', '--init', '--path',
                      str(Path(os.environ['MOKYU_TEST_DATA']) / label))
        buckets.append(cli('bucket', 'create', label, '--project', project, '--storage', storage['id']))
        cred = cli('cred', 'create', label)
        clients.append(boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                       aws_access_key_id=cred['access_key'], aws_secret_access_key=cred['secret_key'],
                       config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})))
    source, target = buckets
    key = 'visible'
    domain = name + '.test'
    cli('domain', 'set', domain, source['name'])
    profile = cli('storage', 'profile', 'show', source['name'])
    profile.update(public_cache_validation='background', public_cache_refresh_interval='45s', public_cache_max_age='60s')
    cli('storage', 'profile', 'set', source['name'], '-', value=profile)

    def put(index, key, body, acl='private'):
        clients[index].put_object(Bucket=buckets[index]['name'], Key=key, Body=body, ACL=acl, ContentType='text/plain')
        response = admin.get(panel + '/api/v1/buckets/' + buckets[index]['id'] + '/objects', params={'q': key, 'mode': 'exact'})
        response.raise_for_status()
        return response.json()['items'][0]['id']

    def action(kind, item, *, actor=admin, **options):
        response = actor.post(panel + '/api/v1/media/actions', json={'bucket': source['id'], 'action': kind,
                              'objects': [{'client_id': str(uuid.uuid4()), **item}], **options}, timeout=30)
        response.raise_for_status()
        return response.json()['results'][0]

    def reads():
        return [requests.get(url, headers=headers, timeout=5) for url, headers in (
            (endpoint + '/' + source['name'] + '/' + key, {}),
            (endpoint + '/' + key, {'Host': source['name']}),
            (public + '/' + key, {'Host': domain}))]

    def warm():
        hits = cli('status')['public_read_cache']['hits']
        deadline = time.monotonic() + 10
        while True:
            responses = reads()
            assert all(response.status_code == 200 and response.content == b'public' for response in responses)
            current = cli('status')['public_read_cache']['hits']
            if current - hits >= len(responses):
                return
            assert time.monotonic() < deadline, 'public routes did not warm up'
            hits = current
            time.sleep(.05)

    version = put(0, key, b'public', 'public-read')
    warm()
    edit = action('metadata', {'key': key, 'version': version}, metadata={'content_type': 'image/png', 'user': {}})
    assert edit['status'] == 200, edit
    assert all(response.headers.get('Content-Type') == 'image/png' for response in reads())
    hidden = put(0, 'private', b'private')
    result = action('copy', {'key': 'private', 'version': hidden, 'target_key': key, 'target_version': edit['output_version']},
                    target_bucket=source['id'])
    assert result['status'] == 200, result
    assert all(response.status_code in (403, 404) for response in reads())
    version = put(0, key, b'public', 'public-read')
    warm()
    result = action('move', {'key': key, 'version': version, 'target_key': 'moved'}, target_bucket=source['id'])
    assert result['status'] == 200, result
    assert all(response.status_code in (403, 404) for response in reads())
    version = put(0, key, b'public', 'public-read')
    moving = put(0, 'private-move', b'private')
    warm()
    result = action('move', {'key': 'private-move', 'version': moving, 'target_key': key, 'target_version': version},
                    target_bucket=source['id'])
    assert result['status'] == 200, result
    assert all(response.status_code in (403, 404) for response in reads())
    print('PASS metadata edit, private overwrite and move invalidate three public routes', flush=True)

    member_password = uuid.uuid4().hex
    response = admin.post(panel + '/api/v1/users', json={'username': name, 'password': member_password, 'role': 'member', 'must_change_password': False})
    response.raise_for_status()
    member_url = panel + '/api/v1/projects/' + project + '/members/' + response.json()['id']
    grants = [{'bucket_id': source['id'], 'actions': ['object.read', 'object.delete']},
              {'bucket_id': target['id'], 'actions': ['object.write']}]

    def grant():
        admin.put(member_url, json={'role': 'writer', 'scope': 'selected', 'grants': grants}).raise_for_status()

    grant()
    member = login(name, member_password)
    old = put(1, 'existing', b'original')
    marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'migration-before-publish'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    try:
        queued = action('move', {'key': 'private', 'version': hidden, 'target_key': 'existing', 'target_version': old},
                        actor=member, target_bucket=target['id'], confirm_storage_migration=True)
        assert queued['status'] == 202, queued
        until = time.monotonic() + 45
        while not marker.with_suffix('.hit').exists():
            assert time.monotonic() < until, 'migration did not reach publication'
            time.sleep(.05)
        grants[1]['actions'] = ['bucket.list']
        grant()
    finally:
        marker.unlink(missing_ok=True)
    until = time.monotonic() + 45
    while (state := cli('task', 'show', queued['task_id'])['state']) in ('running', 'queued'):
        assert time.monotonic() < until, state
        time.sleep(.1)
    assert state == 'failed', state
    for index, path, expected in [(0, 'private', b'private'), (1, 'existing', b'original')]:
        assert clients[index].get_object(Bucket=buckets[index]['name'], Key=path)['Body'].read() == expected
    print('PASS revoked media migration preserves source and existing destination', flush=True)


if __name__ == '__main__':
    main()
