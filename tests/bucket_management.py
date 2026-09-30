"""Scoped bucket creation, revisioned settings, drained transfer and destructive previews."""
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = 'buckets-' + uuid.uuid4().hex[:10]
created, users = [], []


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))


def login(username, password):
    client = requests.Session()
    result = client.post(url + '/api/login', headers={'Origin': url}, json={'username': username, 'password': password})
    assert result.status_code == 200, result.text
    client.headers.update({'Origin': url, 'X-CSRF-Token': result.json()['csrf_token']})
    assert client.post(url + '/api/me/reauth', json={'password': password}).status_code == 204
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
project = admin.post(url + '/api/projects', json={'name': name, 'allow_bucket_create': True}).json()['id']
other = admin.post(url + '/api/projects', json={'name': name + '-other'}).json()['id']


def create(suffix, client=admin, project_id=project, status=201):
    result = client.post(url + '/api/buckets', json={'name': name + suffix, 'project_id': project_id})
    assert result.status_code == status, (result.status_code, result.text)
    if status != 201:
        return result.json()
    bucket = result.json()
    created.append(bucket)
    return bucket


def get(bucket, client=admin):
    result = client.get(url + f"/api/buckets/{bucket['id']}/settings")
    assert result.status_code == 200, result.text
    return result.json()


def settings(value):
    return {**{k: value['bucket'][k] for k in ['revision', 'cors', 'website_enabled', 'index_document', 'error_document', 'public_base_url', 'uploads_paused']}, 'domains': value['domains']}


def save(bucket, value, client=admin, status=200):
    result = client.put(url + f"/api/buckets/{bucket['id']}/settings", json=value)
    assert result.status_code == status, (result.status_code, result.text)
    return result.json()


def member(suffix, role, scope='selected', grants=None):
    password = 'Bucket-' + uuid.uuid4().hex
    result = admin.post(url + '/api/users', json={'username': name + suffix, 'password': password, 'role': 'member', 'must_change_password': False})
    assert result.status_code == 201, result.text
    id = result.json()['id']; users.append(id)
    assert admin.put(url + f'/api/projects/{project}/members/{id}', json={'role': role, 'scope': scope, 'grants': grants or []}).status_code == 204
    return id, login(name + suffix, password)


def s3_client(bucket):
    key = cli('credential', 'create', bucket['name'])
    result = admin.put(url + '/api/credentials/' + key['access_key'] + '/grants', json=[{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'object.write', 'object.delete', 'object.acl', 'bucket.settings', 'storage.inspect']}])
    assert result.status_code == 200, result.text
    return boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1', aws_access_key_id=key['access_key'], aws_secret_access_key=key['secret_key'], config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}, request_checksum_calculation='when_required'))


def deny(call, code='OperationAborted', **kwargs):
    try:
        call(**kwargs)
    except ClientError as failure:
        assert failure.response['Error']['Code'] == code, failure.response['Error']
    else:
        raise AssertionError('operation unexpectedly succeeded')


try:
    assert admin.post(url + '/api/buckets', json={'name': 'Bad/Name'}).status_code == 400
    owner, maintainer = member('-maintainer', 'maintainer')
    _, reader = member('-reader', 'reader', 'all')
    assert reader.get(url + '/api/bucket-projects').json() == []
    create('-denied', reader, status=403)
    bucket = create('-a', maintainer)
    assert bucket['actions'] == ['bucket.list', 'object.read', 'object.write', 'object.delete', 'object.acl', 'bucket.settings', 'storage.inspect']
    assert any(b['id'] == bucket['id'] for b in maintainer.get(url + '/api/buckets').json())
    create('-a', status=409)
    second = create('-b')
    print('PASS project policy, maintainer selected grant, reader denial and duplicate bucket', flush=True)

    config = settings(get(bucket))
    old = dict(config)
    config.update(website_enabled=True, public_base_url='https://media.example.test', domains=[name + '.example.test'], cors=[{'origins': ['https://client.test'], 'methods': ['GET', 'HEAD'], 'headers': ['*'], 'expose': ['ETag'], 'max_age': 300}])
    saved = save(bucket, config)
    assert saved['bucket']['public_base_url'] == 'https://media.example.test/' and saved['bucket']['revision'] != config['revision']
    save(bucket, old, status=412)
    value = settings(saved)
    with ThreadPoolExecutor() as pool:
        results = list(pool.map(lambda _: admin.put(url + f"/api/buckets/{bucket['id']}/settings", json=value), range(2)))
    assert sorted(result.status_code for result in results) == [200, 412], [(r.status_code, r.text) for r in results]
    duplicate = settings(get(second)); duplicate['domains'] = config['domains']
    save(second, duplicate, status=409)
    assert not get(second)['domains']
    for invalid in ['https://host/path', 'evil@host', 'bad host', '-bad.test', 'bad..test', 'host:999999']:
        invalid_settings = settings(get(second)); invalid_settings['domains'] = [invalid]
        save(second, invalid_settings, status=400)
    limited = settings(get(bucket, maintainer)); limited.pop('domains')
    limited['index_document'] = 'home.html'
    save(bucket, limited, maintainer)
    limited = settings(get(bucket, maintainer)); limited.pop('domains'); limited['uploads_paused'] = True
    save(bucket, limited, maintainer, 403)
    assert reader.get(url + f"/api/buckets/{bucket['id']}/settings").status_code == 403
    before = settings(get(bucket))
    cli('domain', 'set', name + '-extra.test', bucket['name'])
    save(bucket, before, status=412)
    cli('domain', 'delete', name + '-extra.test')
    print('PASS revision conflict, concurrent edits, domain ownership/validation and scoped settings', flush=True)

    s3 = s3_client(bucket)
    content = b'<h1>Little room</h1>'
    s3.put_object(Bucket=bucket['name'], Key='home.html', Body=content, ContentType='text/html', ACL='public-read')
    public = requests.get(os.environ['MOKYU_TEST_PUBLIC'] + '/', headers={'Host': name + '.example.test', 'Origin': 'https://client.test'})
    assert public.status_code == 200 and public.content == content and public.headers['access-control-allow-origin'] == 'https://client.test'
    before = settings(get(bucket))
    rules = Path('/config/' + name + '-cors.json')
    rules.write_text(json.dumps([{'origins': ['*'], 'methods': ['GET'], 'headers': [], 'expose': [], 'max_age': 300}]))
    cli('bucket', 'cors', bucket['name'], str(rules))
    rules.unlink()
    save(bucket, before, status=412)
    upload = s3.create_multipart_upload(Bucket=bucket['name'], Key='continuing')['UploadId']
    paused = settings(get(bucket)); paused['uploads_paused'] = True
    save(bucket, paused)
    deny(s3.put_object, Bucket=bucket['name'], Key='new', Body=b'new')
    deny(s3.create_multipart_upload, Bucket=bucket['name'], Key='new-multipart')
    path = url + f"/api/buckets/{bucket['id']}/transfer"
    preview = admin.post(path + '/preview', json={'target_project': other}).json()
    confirmation = {'target_project': other, 'confirm_name': bucket['name'], 'confirmation': preview['confirmation']}
    result = admin.post(path, json=confirmation)
    assert result.status_code == 409 and result.json()['code'] == 'BucketNotDrained', result.text
    part = s3.upload_part(Bucket=bucket['name'], Key='continuing', UploadId=upload, PartNumber=1, Body=b'old multipart')['ETag']
    s3.complete_multipart_upload(Bucket=bucket['name'], Key='continuing', UploadId=upload, MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part}]})
    assert s3.get_object(Bucket=bucket['name'], Key='continuing')['Body'].read() == b'old multipart'
    print('PASS website and CORS reuse; paused admission still permits existing multipart and reads', flush=True)

    token = maintainer.post(url + '/api/tokens', json={'label': name, 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read']}]}).json()
    assert 'secret' in token, token
    assert admin.put(url + '/api/quotas/project/' + other, json={'byte_limit': '0', 'inflight_limit': None, 'bucket_limit': None}).status_code == 200
    preview = admin.post(path + '/preview', json={'target_project': other}).json()
    confirmation['confirmation'] = preview['confirmation']
    result = admin.post(path, json=confirmation)
    assert result.status_code == 403 and result.json()['code'] == 'QuotaExceeded', result.text
    assert get(bucket)['bucket']['project_id'] == project
    assert admin.put(url + '/api/quotas/project/' + other, json={'byte_limit': None, 'inflight_limit': None, 'bucket_limit': None}).status_code == 200
    result = admin.post(path, json=confirmation)
    assert result.status_code == 200, result.text
    assert result.json()['project_id'] == other and not result.json()['uploads_paused']
    deny(s3.get_object, 'AccessDenied', Bucket=bucket['name'], Key='continuing')
    assert maintainer.get(url + f"/api/buckets/{bucket['id']}/objects").status_code == 403
    assert requests.get(url + f"/api/buckets/{bucket['id']}/objects", headers={'Authorization': 'Bearer ' + token['secret']}).status_code == 403
    assert get(bucket)['domains'] == [name + '.example.test']
    for scope in [project, other]:
        measured = db.execute('SELECT coalesce(sum(s.size),0),count(*) FROM objects o JOIN streams s ON s.id=o.stream_id JOIN buckets b ON b.id=o.bucket_id WHERE b.project_id=%s', (scope,)).fetchone()
        ledger = db.execute("SELECT used_bytes,object_count FROM quota_accounts WHERE kind='project' AND id=%s", (scope,)).fetchone()
        assert measured == ledger, (measured, ledger)
    print('PASS drained nonempty transfer, target quota rollback, old member/key/PAT revocation and exact ledgers', flush=True)

    preview = admin.post(url + f"/api/buckets/{bucket['id']}/purge/preview").json()
    result = admin.post(url + f"/api/buckets/{bucket['id']}/purge", json={'confirm_name': 'wrong', 'confirmation': preview['confirmation']})
    assert result.status_code == 412, result.text
    assert admin.delete(url + f"/api/buckets/{bucket['id']}", json={'confirm_name': bucket['name'], 'confirmation': preview['confirmation']}).status_code == 409
    purge = admin.post(url + f"/api/buckets/{bucket['id']}/purge", json={'confirm_name': bucket['name'], 'confirmation': preview['confirmation']})
    assert purge.status_code == 200 and purge.json()['task_id'], purge.text
    end = time.monotonic() + 35
    while db.execute('SELECT EXISTS(SELECT 1 FROM buckets WHERE id=%s)', (bucket['id'],)).fetchone()[0]:
        cli('gc', 'run')
        assert time.monotonic() < end, 'purge did not finish'
        time.sleep(.1)
    preview = admin.post(url + f"/api/buckets/{second['id']}/purge/preview").json()
    assert admin.delete(url + f"/api/buckets/{second['id']}", json={'confirm_name': second['name'], 'confirmation': preview['confirmation']}).status_code == 204
    print('PASS destructive name/preview guard, nonempty rejection, asynchronous purge and empty deletion', flush=True)
finally:
    for id in users:
        admin.delete(url + '/api/users/' + id)
    db.close()
