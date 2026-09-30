"""Project/action isolation and revocation during admitted writes; isolated Docker only."""
import json
import os
import http.client
import time
import uuid
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from urllib.parse import urlsplit
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url, endpoint = os.environ['MOKYU_TEST_WEB'], os.environ['MOKYU_TEST_ENDPOINT']
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
prefix = 'access-' + uuid.uuid4().hex[:12]
password = 'Project-test-' + uuid.uuid4().hex
all_actions = ['bucket.list', 'object.read', 'object.write', 'object.delete', 'object.acl', 'bucket.settings', 'storage.inspect']
users, keys = [], []


def cli(*args, secret=None):
    result = subprocess.run(command + list(args), input=secret, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def login(name, secret):
    client = requests.Session()
    reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': name, 'password': secret}, timeout=10)
    assert reply.status_code == 200, reply.status_code
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    return client


def wait(check):
    deadline = time.monotonic() + 15
    while not check():
        assert time.monotonic() < deadline, 'test boundary not reached'
        time.sleep(.02)


def error(call, **kwargs):
    try:
        call(**kwargs)
    except ClientError as exc:
        assert exc.response['Error']['Code'] in ('AccessDenied', '403'), exc.response['Error']['Code']
    else:
        raise AssertionError('unauthorized S3 request succeeded')


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
assert admin.post(url + '/api/me/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204
try:
    # Empty projects may be removed; the built-in one and non-empty projects may not.
    temp = admin.post(url + '/api/projects', json={'name': prefix + '-empty'})
    assert temp.status_code == 201, temp.text
    assert admin.delete(url + '/api/projects/' + temp.json()['id']).status_code == 204
    assert admin.delete(url + '/api/projects/00000000-0000-0000-0000-000000000001').status_code == 409
    projects, buckets = [], []
    for index in range(2):
        project = admin.post(url + '/api/projects', json={'name': prefix + '-' + str(index)}).json()
        projects.append(project['id'])
        bucket = cli('bucket', 'create', prefix + '-' + str(index))
        db.execute('UPDATE buckets SET project_id=%s WHERE id=%s', (project['id'], bucket['id']))
        buckets.append(bucket)
    assert admin.put(url + '/api/settings/projects', json={'enabled': False}).status_code == 409
    assert admin.delete(url + '/api/projects/' + projects[0]).status_code == 409
    admin_id = admin.get(url + '/api/me').json()['id']
    db.execute("UPDATE sessions SET reauthenticated_at=now()-interval '10 minutes' WHERE user_id=%s", (admin_id,))
    reply = admin.post(url + '/api/projects', json={'name': prefix + '-expired'})
    assert reply.status_code == 403 and reply.json()['code'] == 'ReauthenticationRequired'
    assert admin.post(url + '/api/me/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204

    bucket = buckets[0]
    credential = cli('credential', 'create', bucket['name'])
    keys.append(credential['access_key'])
    s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                      aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                      config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                                    request_checksum_calculation='when_required', response_checksum_validation='when_required'))
    s3.put_object(Bucket=bucket['name'], Key='public', Body=b'project content', ACL='public-read')
    s3.put_object(Bucket=bucket['name'], Key='private', Body=b'private content')
    clients = {}
    for role in ['reader', 'writer', 'maintainer']:
        name = prefix + '-' + role
        user = cli('user', 'create', name, '--password-stdin', secret=password + '\n')['id']
        users.append(name)
        db.execute("UPDATE web_users SET role='member' WHERE id=%s", (user,))
        db.execute("INSERT INTO project_members(user_id,project_id,role,scope) VALUES(%s,%s,%s,'selected')", (user, projects[0], role))
        db.execute('INSERT INTO member_grants(user_id,project_id,bucket_id,actions) VALUES(%s,%s,%s,%s)', (user, projects[0], bucket['id'], all_actions))
        client = login(name, password)
        clients[role] = (client, user)
        visible = client.get(url + '/api/buckets').json()
        assert len(visible) == 1 and visible[0]['id'] == bucket['id']
        assert len(visible[0]['actions']) == {'reader': 3, 'writer': 6, 'maintainer': 7}[role]
        assert len(client.get(url + '/api/projects').json()) == 1
        for route in ['/api/status', '/api/tasks', '/api/packs', '/api/settings/projects',
                      '/api/tasks/' + str(uuid.uuid4()) + '/report', '/api/packs/1']:
            assert client.get(url + route).status_code == 403, route
        assert client.post(url + '/api/projects', json={'name': 'denied'}).status_code == 403
        for route in ['/api/objects', '/api/object', '/api/download', '/api/object/chunks']:
            response = client.get(url + route, params={'bucket': buckets[1]['id'], 'key': 'private', 'version': str(uuid.uuid4())})
            assert response.status_code == 403, (route, response.status_code)
        assert client.get(url + '/api/download', params={'bucket': bucket['id'], 'key': 'private'}).content == b'private content'
        detail = client.get(url + '/api/object', params={'bucket': bucket['id'], 'key': 'private'}).json()
        assert detail['bucket_grants'] == []
        response = client.get(url + '/api/object/chunks', params={'bucket': bucket['id'], 'key': 'private', 'version': detail['object']['id']})
        assert response.status_code == 200, response.text
        assert all(not any(key in chunk for key in ['reads', 'range_reads', 'key_id']) for chunk in response.json()['chunks'])
        for method in ['get', 'put']:
            response = getattr(client, method)(url + '/api/buckets/' + bucket['id'] + '/cors', **({'json': []} if method == 'put' else {}))
            assert response.status_code == (200 if role == 'maintainer' else 403), (role, response.text)
        response = client.post(url + '/api/objects/actions', json={'bucket': bucket['id'], 'action': 'private', 'objects': [{'key': 'private', 'version': detail['object']['id']}]})
        assert response.status_code == 200, response.text
        assert response.json()['results'][0]['status'] == (403 if role == 'reader' else 200)
    reader, user = clients['reader']
    db.execute('DELETE FROM member_grants WHERE user_id=%s', (user,))
    assert reader.get(url + '/api/session').status_code == 200
    assert reader.get(url + '/api/buckets').json() == []
    assert reader.get(url + '/api/download', params={'bucket': bucket['id'], 'key': 'private'}).status_code == 403
    db.execute("UPDATE project_members SET scope='all' WHERE user_id=%s", (user,))
    assert len(reader.get(url + '/api/buckets').json()) == 1
    assert requests.get(endpoint + '/' + bucket['name'] + '/public').content == b'project content'
    print('PASS project ownership, role ceilings, scoped routes, private objects and live revocation', flush=True)

    def grant(actions):
        db.execute('UPDATE grants SET actions=%s WHERE access_key=%s', (actions, credential['access_key']))

    grant(['object.read'])
    assert s3.get_object(Bucket=bucket['name'], Key='private')['Body'].read() == b'private content'
    error(s3.list_objects_v2, Bucket=bucket['name'])
    error(s3.put_object, Bucket=bucket['name'], Key='denied', Body=b'x')
    grant(['object.write'])
    s3.put_object(Bucket=bucket['name'], Key='write-only', Body=b'x')
    error(s3.put_object, Bucket=bucket['name'], Key='denied-acl', Body=b'x', ACL='public-read')
    error(s3.delete_object, Bucket=bucket['name'], Key='write-only')
    error(s3.put_object_acl, Bucket=bucket['name'], Key='write-only', ACL='public-read')
    error(s3.copy_object, Bucket=bucket['name'], Key='copy', CopySource=bucket['name'] + '/private')
    grant(all_actions)
    db.execute('INSERT INTO grants(access_key,bucket_id,actions) VALUES(%s,%s,%s)', (credential['access_key'], buckets[1]['id'], all_actions))
    error(s3.put_object, Bucket=buckets[1]['name'], Key='cross-project', Body=b'x')
    assert all(item['Name'] != buckets[1]['name'] for item in s3.list_buckets()['Buckets'])
    db.execute('DELETE FROM grants WHERE access_key=%s AND bucket_id=%s', (credential['access_key'], buckets[1]['id']))
    print('PASS separate S3 read/write/delete/ACL/list actions and service project boundary', flush=True)

    # Pause the HTTP body after admission. Revocation must prevent publication.
    for part in [False, True]:
        grant(all_actions)
        key = 'part-revoked' if part else 'put-revoked'
        upload = s3.create_multipart_upload(Bucket=bucket['name'], Key=key)['UploadId'] if part else None
        arguments = {'Bucket': bucket['name'], 'Key': key, 'ContentLength': 131072}
        if part:
            arguments.update(UploadId=upload, PartNumber=1)
        target = urlsplit(s3.generate_presigned_url('upload_part' if part else 'put_object', Params=arguments, HttpMethod='PUT', ExpiresIn=60))
        connection = (http.client.HTTPSConnection if target.scheme == 'https' else http.client.HTTPConnection)(target.hostname, target.port, timeout=20)
        try:
            connection.putrequest('PUT', target.path + '?' + target.query)
            connection.putheader('Content-Length', '131072')
            connection.endheaders()
            connection.send(b'x' * 65536)
            wait(lambda: db.execute("SELECT EXISTS(SELECT 1 FROM streams WHERE bucket_id=%s AND object_key=%s AND state='writing')", (bucket['id'], key)).fetchone()[0])
            grant(['object.read', 'bucket.list'])
            connection.send(b'y' * 65536)
            response = connection.getresponse()
            assert response.status == 403, response.read().decode()
        finally:
            connection.close()
        assert not db.execute('SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=%s AND key=%s AND stream_id IS NOT NULL)', (bucket['id'], key)).fetchone()[0]
        if part:
            assert s3.list_parts(Bucket=bucket['name'], Key=key, UploadId=upload).get('Parts', []) == []
            grant(all_actions)
            s3.abort_multipart_upload(Bucket=bucket['name'], Key=key, UploadId=upload)
    print('PASS revocation during PUT and UploadPart prevents object/part publication', flush=True)

    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    grant(all_actions)
    upload = s3.create_multipart_upload(Bucket=bucket['name'], Key='complete-revoked')['UploadId']
    etag = s3.upload_part(Bucket=bucket['name'], Key='complete-revoked', UploadId=upload, PartNumber=1, Body=b'complete')['ETag']
    marker = faults / 'multipart-completing'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(error, s3.complete_multipart_upload, Bucket=bucket['name'], Key='complete-revoked', UploadId=upload,
                              MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag}]})
        try:
            wait(lambda: marker.with_suffix('.hit').exists())
            grant(['object.read'])
        finally:
            marker.unlink(missing_ok=True)
        pending.result(timeout=15)
    assert not db.execute("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id=%s AND key='complete-revoked' AND stream_id IS NOT NULL)", (bucket['id'],)).fetchone()[0]
    grant(all_actions)
    s3.abort_multipart_upload(Bucket=bucket['name'], Key='complete-revoked', UploadId=upload)
    print('PASS revocation after CompleteMultipartUpload admission prevents async publication', flush=True)
finally:
    for name in users:
        cli('user', 'delete', name)
    for key in keys:
        cli('credential', 'disable', key)
    db.close()
