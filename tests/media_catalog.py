"""Current catalog, literal search, deterministic cursors, live authorization and quota neutrality."""
import json
import os
import subprocess
import uuid
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
name = 'catalog-' + uuid.uuid4().hex[:10]


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))


def login(username, password):
    client = requests.Session()
    reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': username, 'password': password})
    assert reply.status_code == 200, reply.text
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
bucket = cli('bucket', 'create', name)
credential = cli('credential', 'create', name)
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}, request_checksum_calculation='when_required'))
path = url + '/api/buckets/' + bucket['id'] + '/objects'
objects = {'folder/cute%_heart.png': (11, 'image/png'), 'folder/cuteZZheart.png': (12, 'image/png'),
           'folder/HELLO.txt': (22, 'text/plain'), 'folder/sub/hello.webm': (22, 'video/webm'),
           'a.txt': (1, 'text/plain'), 'b.zip': (30, 'application/zip'),
           'folder/' + '\u53ef\u7231\u732b' + '.png': (41, 'image/png'),
           'odd/../a +%_#.txt': (43, 'text/plain'), 'zero': (0, 'application/octet-stream')}
user = None


def page(client=admin, **query):
    reply = client.get(path, params=query, timeout=10)
    assert reply.status_code == 200, (reply.status_code, reply.text)
    return reply.json()


def keys(result):
    return [row['object_key'] for row in result['objects']]


try:
    for key, (size, mime) in objects.items():
        s3.put_object(Bucket=name, Key=key, Body=bytes(size), ContentType=mime)
    assert admin.get(url + '/api/buckets/' + bucket['id'] + '/catalog').json()['phase'] == 'ready'
    root = page()
    assert root['layout'] == 'folders' and root['prefixes'] == ['folder/', 'odd/']
    assert set(keys(root)) == {'a.txt', 'b.zip', 'zero'}
    assert keys(page(q='cute%_heart')) == ['folder/cute%_heart.png']
    assert set(keys(page(q='hello'))) == {'folder/HELLO.txt', 'folder/sub/hello.webm'}
    assert keys(page(q='folder')) == []
    assert len(keys(page(q='folder', search_in='path'))) == 5
    assert keys(page(prefix='folder/', q='HE', mode='contains')) == ['folder/HELLO.txt']
    assert page(prefix='folder/', q='HE')['search_mode'] == 'prefix'
    assert keys(page(q='\u53ef\u7231\u732b')) == ['folder/' + '\u53ef\u7231\u732b' + '.png']
    assert keys(page(q='odd/../a +%_#.txt', mode='exact')) == ['odd/../a +%_#.txt']
    assert keys(page(prefix='odd/../', recursive='true')) == ['odd/../a +%_#.txt']
    assert len(keys(page(kind='image'))) == 3
    assert set(keys(page(min_size='22', max_size='22'))) == {'folder/HELLO.txt', 'folder/sub/hello.webm'}
    assert keys(page(max_size='0')) == ['zero']
    print('PASS literal wildcards, Unicode trigrams, short-prefix fallback, raw paths and metadata filters', flush=True)

    for sort in ['name', 'size', 'modified']:
        for order in ['asc', 'desc']:
            found, cursor = [], None
            while True:
                result = page(recursive='true', sort=sort, order=order, limit=2, **({'after': cursor} if cursor else {}))
                found += result['objects']
                cursor = result['next']
                if not cursor:
                    break
            assert len(found) == len(objects) and len(set(row['id'] for row in found)) == len(objects)
            values = [(int(row['size']), row['object_key']) if sort == 'size' else (row['modified_at'], row['object_key']) if sort == 'modified' else row['object_key'] for row in found]
            assert values == sorted(values, reverse=order == 'desc'), values
    first = page(recursive='true', limit=2)
    assert admin.get(path, params={'recursive': 'true', 'after': first['next'], 'limit': 2, 'kind': 'image'}).status_code == 400
    for query in [{'sort': 'injected'}, {'q': '\0'}, {'min_size': '-1'}, {'limit': 0}, {'max_size': '1', 'min_size': '2'}, {'unused': 'x'}]:
        assert admin.get(path, params=query).status_code in (400, 422), query
    before = admin.get(url + '/api/quotas/bucket/' + bucket['id']).json()
    s3.put_object_acl(Bucket=name, Key='a.txt', ACL='public-read')
    assert keys(page(public='true')) == ['a.txt']
    s3.put_object(Bucket=name, Key='a.txt', Body=b'updated', ContentType='video/mp4')
    assert keys(page(public='true')) == []
    assert 'a.txt' in keys(page(kind='video'))
    s3.delete_object(Bucket=name, Key='b.zip')
    assert keys(page(q='b.zip', mode='exact')) == []
    after = admin.get(url + '/api/quotas/bucket/' + bucket['id']).json()
    assert int(after['used_bytes']) == int(before['used_bytes']) + 6 - 30
    assert int(after['object_count']) == len(objects) - 1
    print('PASS all six sort/cursor combinations, current overwrite/ACL/deletion and quota neutrality', flush=True)

    secret = 'Catalog-test-' + uuid.uuid4().hex
    reply = admin.post(url + '/api/users', json={'username': name, 'password': secret, 'role': 'member', 'must_change_password': False})
    assert reply.status_code == 201
    user = reply.json()['id']
    project = next(b['project_id'] for b in admin.get(url + '/api/buckets').json() if b['id'] == bucket['id'])
    membership = url + f'/api/projects/{project}/members/{user}'
    assert admin.put(membership, json={'role': 'reader', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list']}]}).status_code == 204
    member = login(name, secret)
    assert keys(page(client=member, q='hello'))
    assert member.get(url + '/api/buckets/' + bucket['id'] + '/catalog').json()['scanned'] is None
    foreign = next(b['id'] for b in admin.get(url + '/api/buckets').json() if b['id'] != bucket['id'])
    assert member.get(url + '/api/buckets/' + foreign + '/objects', params={'q': 'hello'}).status_code in (403, 404)
    assert admin.delete(membership).status_code == 204
    assert member.get(path, params={'recursive': 'true', 'after': first['next'], 'limit': 2}).status_code in (403, 404)
    print('PASS scoped list-only search, hidden instance progress and revocation with an old cursor', flush=True)
finally:
    if user:
        admin.delete(url + '/api/users/' + user)
    cli('credential', 'disable', credential['access_key'])
    db.close()
