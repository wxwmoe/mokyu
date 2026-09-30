"""Versioned metadata-only actions, idempotent receipts, quotas and publication races."""
import json
import hashlib
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

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = 'actions-' + uuid.uuid4().hex[:10]
fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'media-before-publish'
after = fault.with_name('media-after-operation')
user = None


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))


def login(username, password):
    client = requests.Session()
    reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': username, 'password': password})
    assert reply.status_code == 200, reply.status_code
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    return client


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
assert admin.post(url + '/api/me/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204
project = admin.post(url + '/api/projects', json={'name': name}).json()['id']
buckets = [cli('bucket', 'create', name + suffix) for suffix in ['-a', '-b']]
for bucket in buckets:
    db.execute('UPDATE buckets SET project_id=%s WHERE id=%s', (project, bucket['id']))
a, b = [bucket['id'] for bucket in buckets]
credential = cli('credential', 'create', buckets[0]['name'])
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}, request_checksum_calculation='when_required'))


def put(key, body=b'some little chunks', **kwargs):
    s3.put_object(Bucket=buckets[0]['name'], Key=key, Body=body, ContentType='text/plain', **kwargs)
    item = admin.get(url + f'/api/buckets/{a}/objects', params={'q': key, 'mode': 'exact'}).json()['objects'][0]
    return {'client_id': str(uuid.uuid4()), 'key': key, 'version': item['id']}


def batch(action, items, bucket=a, client=admin, **kwargs):
    reply = client.post(url + '/api/media/actions', json={'bucket': bucket, 'action': action, 'objects': items, **kwargs}, timeout=70)
    assert reply.status_code == 200, (reply.status_code, reply.text)
    return reply.json()['results']


def one(action, item, status=200, **kwargs):
    result = batch(action, [item], **kwargs)[0]
    assert result['status'] == status, result
    return result


def limits(size):
    reply = admin.put(url + '/api/quotas/project/' + project, json={'byte_limit': None if size is None else str(size), 'inflight_limit': None, 'bucket_limit': None})
    assert reply.status_code == 200, reply.text


def used():
    return db.execute("SELECT used_bytes FROM quota_accounts WHERE kind='project' AND id=%s", (project,)).fetchone()[0]


def content(bucket, key, version=None):
    if version is None:
        version = admin.get(url + f'/api/buckets/{bucket}/objects', params={'q': key, 'mode': 'exact'}).json()['objects'][0]['id']
    reply = admin.get(url + f'/api/buckets/{bucket}/object/content', params={'key': key, 'version': version})
    assert reply.status_code == 200, reply.text
    return reply.content


def arm(path):
    path.with_suffix('.hit').unlink(missing_ok=True)
    path.touch()
    path.chmod(0o666)


def hit(path):
    end = time.monotonic() + 15
    while not path.with_suffix('.hit').exists():
        assert time.monotonic() < end, 'action did not reach publication boundary'
        time.sleep(.02)


try:
    raw = os.urandom(6 * 1024 * 1024 + 23)
    source = put('source.bin', raw, Metadata={'flower': 'peach'}, CacheControl='max-age=60')
    chunks = db.execute('SELECT count(*) FROM chunks').fetchone()[0]
    original = used()
    copied = {**source, 'target_key': 'copy.bin'}
    result = one('copy', copied, target_bucket=b)
    assert result['output_version'] != source['version'] and not result['replayed']
    assert content(b, 'copy.bin') == raw and used() == original + len(raw)
    assert db.execute('SELECT count(*) FROM chunks').fetchone()[0] == chunks
    assert one('copy', copied, target_bucket=b)['replayed']
    assert one('copy', {**copied, 'target_key': 'changed.bin'}, 409, target_bucket=b)['code'] == 'IdempotencyConflict'
    assert one('copy', {**copied, 'client_id': str(uuid.uuid4())}, 412, target_bucket=b)['code'] == 'PreconditionFailed'
    # Build a valid sliced mapping to verify that shared copy preserves extent offsets.
    sliced = str(uuid.uuid4())
    fragment = raw[7:30]
    with db.transaction():
        db.execute("INSERT INTO streams SELECT (jsonb_populate_record(NULL::streams,to_jsonb(s)||jsonb_build_object('id',%s::text,'object_key','slice.bin','size',23,'etag',%s::text,'checksums','{}'::jsonb))).* FROM streams s WHERE id=%s", (sliced, hashlib.md5(fragment).hexdigest(), source['version']))
        db.execute('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) SELECT %s,0,23,chunk_id,source_offset+7 FROM extents WHERE stream_id=%s AND offset_bytes=0', (sliced, source['version']))
        db.execute("INSERT INTO objects(bucket_id,key,stream_id,write_epoch) VALUES(%s,'slice.bin',%s,%s)", (a, sliced, sliced))
    one('copy', {'client_id': str(uuid.uuid4()), 'key': 'slice.bin', 'version': sliced, 'target_key': 'slice-copy'}, target_bucket=b)
    assert content(b, 'slice-copy') == fragment
    s3.copy_object(Bucket=buckets[0]['name'], Key='slice-s3', CopySource={'Bucket': buckets[0]['name'], 'Key': 'slice.bin'})
    assert content(a, 'slice-s3') == fragment
    for bucket_id, key in [(a, 'slice.bin'), (a, 'slice-s3'), (b, 'slice-copy')]:
        item = admin.get(url + f'/api/buckets/{bucket_id}/objects', params={'q': key, 'mode': 'exact'}).json()['objects'][0]
        one('delete', {'client_id': str(uuid.uuid4()), 'key': key, 'version': item['id']}, bucket=bucket_id)
    print('PASS zero-data copy, exact content, logical quota, durable replay and overwrite guards', flush=True)

    limits(used())
    move = {**source, 'client_id': str(uuid.uuid4()), 'target_key': 'moved.bin'}
    moved = one('move', move, target_bucket=b)
    assert moved['output_version'] == source['version'] and content(b, 'moved.bin') == raw
    assert admin.get(url + f'/api/buckets/{a}/object', params={'key': source['key'], 'version': source['version']}).status_code == 404
    assert one('move', move, target_bucket=b)['replayed']
    assert one('copy', {**source, 'client_id': str(uuid.uuid4()), 'key': 'moved.bin', 'target_key': 'over.bin'}, 403, bucket=b, target_bucket=b)['code'] == 'QuotaExceeded'
    assert content(b, 'moved.bin') == raw
    meta = {'content_type': 'application/octet-stream', 'cache_control': 'max-age=120', 'user': {'flower': 'rose'}}
    edited = one('metadata', {**source, 'client_id': str(uuid.uuid4()), 'key': 'moved.bin'}, bucket=b, metadata=meta)
    assert edited['output_version'] != source['version'] and content(b, 'moved.bin') == raw
    detail = admin.get(url + f'/api/buckets/{b}/object', params={'key': 'moved.bin', 'version': edited['output_version']}).json()
    assert detail['metadata']['user'] == {'flower': 'rose'}
    assert detail['metadata']['cache_control'] == 'max-age=120'
    assert used() == original + len(raw)
    invalid = admin.post(url + '/api/media/actions', json={'bucket': b, 'action': 'metadata', 'metadata': {'content_type': 'text/plain\r\nEvil: 1'}, 'objects': [{**source, 'client_id': str(uuid.uuid4()), 'key': 'moved.bin'}]})
    assert invalid.status_code == 400
    limits(None)
    other = admin.post(url + '/api/projects', json={'name': name + '-other'}).json()['id']
    destination = cli('bucket', 'create', name + '-other')
    buckets.append(destination)
    db.execute('UPDATE buckets SET project_id=%s WHERE id=%s', (other, destination['id']))
    assert admin.put(url + '/api/quotas/project/' + other, json={'byte_limit': '0', 'inflight_limit': None, 'bucket_limit': None}).status_code == 200
    denied = one('move', {'client_id': str(uuid.uuid4()), 'key': 'moved.bin', 'version': edited['output_version'], 'target_key': 'cross-project'}, 403, bucket=b, target_bucket=destination['id'])
    assert denied['code'] == 'QuotaExceeded' and content(b, 'moved.bin') == raw
    print('PASS full-quota net move and metadata edit, new version and header validation', flush=True)

    race = put('race', b'first')
    arm(fault)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(one, 'copy', {**race, 'target_key': 'race-result'}, 412, target_bucket=b)
        hit(fault)
        put('race', b'second')
        fault.unlink()
        assert future.result()['code'] == 'PreconditionFailed'
    assert admin.get(url + f'/api/buckets/{b}/object', params={'key': 'race-result', 'version': race['version']}).status_code == 404
    print('PASS source replacement during staged copy cannot publish stale content', flush=True)

    good = put('good')
    stale = put('stale')
    put('stale', b'new')
    results = batch('delete', [good, stale])
    assert [r['status'] for r in results] == [200, 412], results
    assert one('delete', good)['replayed']
    # Receipt persists before the HTTP response: retry after the commit boundary.
    lost = put('lost-response')
    arm(after)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(one, 'delete', lost)
        hit(after)
        stored = db.execute('SELECT result FROM media_operations WHERE client_id=%s', (lost['client_id'],)).fetchone()[0]
        assert stored['status'] == 200
        after.unlink()
        future.result()
    assert one('delete', lost)['replayed']
    print('PASS per-item partial results and receipt committed before response', flush=True)

    password = 'Media-actions-' + uuid.uuid4().hex
    reply = admin.post(url + '/api/users', json={'username': name, 'password': password, 'role': 'member', 'must_change_password': False})
    assert reply.status_code == 201, reply.text
    user = reply.json()['id']
    member_url = url + f'/api/projects/{project}/members/{user}'
    grant = {'role': 'writer', 'scope': 'selected', 'grants': [{'bucket_id': a, 'actions': ['bucket.list', 'object.read']}, {'bucket_id': b, 'actions': ['bucket.list', 'object.write']}]}
    assert admin.put(member_url, json=grant).status_code == 204
    member = login(name, password)
    scoped = put('scoped', ACL='public-read')
    private = one('copy', {**scoped, 'target_key': 'private-copy'}, client=member, target_bucket=b)
    detail = admin.get(url + f'/api/buckets/{b}/object', params={'key': 'private-copy', 'version': private['output_version']}).json()
    assert not detail['item']['public_read']
    one('copy', {**scoped, 'client_id': str(uuid.uuid4()), 'target_key': 'public-copy'}, 403, client=member, target_bucket=b, public_read=True)
    pending = {**scoped, 'client_id': str(uuid.uuid4()), 'target_key': 'revoked'}
    arm(fault)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(one, 'copy', pending, 403, client=member, target_bucket=b)
        hit(fault)
        grant['grants'][1]['actions'] = ['bucket.list']
        assert admin.put(member_url, json=grant).status_code == 204
        fault.unlink()
        future.result()
    one('copy', {**scoped, 'target_key': 'private-copy'}, 403, client=member, target_bucket=b)
    print('PASS private-by-default copies, explicit public permission, publication and replay revocation', flush=True)

    # Independently reconcile trigger ledgers after every successful and failed mutation.
    for bucket in buckets:
        expected = db.execute('SELECT coalesce(sum(s.size),0),count(*) FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=%s', (bucket['id'],)).fetchone()
        actual = db.execute("SELECT used_bytes,object_count FROM quota_accounts WHERE kind='bucket' AND id=%s", (bucket['id'],)).fetchone()
        assert expected == actual, (expected, actual)
    print('PASS independent final quota reconciliation', flush=True)
finally:
    for path in [fault, after]:
        path.unlink(missing_ok=True)
        path.with_suffix('.hit').unlink(missing_ok=True)
    limits(None)
    if user:
        admin.delete(url + '/api/users/' + user)
    for bucket in buckets:
        rows = db.execute('SELECT key,stream_id FROM objects WHERE bucket_id=%s AND stream_id IS NOT NULL', (bucket['id'],)).fetchall()
        if rows:
            batch('delete', [{'client_id': str(uuid.uuid4()), 'key': key, 'version': str(version)} for key, version in rows], bucket=bucket['id'])
    db.close()
