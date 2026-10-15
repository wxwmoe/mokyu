"""Logical quota reservations across S3 writes, multipart replacement, cancellation and access scopes."""
import http.client
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import boto3
import psycopg
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url, endpoint = os.environ['MOKYU_TEST_WEB'], os.environ['MOKYU_TEST_ENDPOINT']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
prefix = 'quota-' + uuid.uuid4().hex[:10]


def cli(*args):
    return json.loads(subprocess.run(command + list(args), capture_output=True, text=True, check=True, timeout=20).stdout)


admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
assert reply.status_code == 200
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
assert admin.post(url + '/api/v1/account/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204
project = admin.post(url + '/api/v1/projects', json={'name': prefix}).json()['id']
storage = cli('storage', 'show', os.environ['MOKYU_TEST_STORAGE'])['id']
bucket = cli('bucket', 'create', prefix, '--storage', storage)
db.execute('UPDATE buckets SET project_id=%s WHERE id=%s', (project, bucket['id']))
credential = cli('cred', 'create', prefix)
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
uploads, connections = [], []
user = None


def limits(size=None, inflight=None, buckets=None, scope='project'):
    target = project if scope == 'project' else bucket['id']
    reply = admin.put(url + '/api/v1/quotas/' + scope + '/' + target, json={
        'logical_bytes_limit': None if size is None else str(size),
        'inflight_bytes_limit': None if inflight is None else str(inflight),
        'bucket_count_limit': None if buckets is None else str(buckets)})
    assert reply.status_code == 200, reply.text
    return reply.json()


def snapshot():
    return db.execute("SELECT used_bytes,reserved_bytes,inflight_bytes FROM quota_scopes WHERE scope='project' AND scope_id=%s", (project,)).fetchone()


def wait_counts(expected):
    deadline = time.monotonic() + 20
    while snapshot() != expected:
        assert time.monotonic() < deadline, (snapshot(), expected)
        time.sleep(.05)
    # Both scopes are updated by the same transaction.
    assert db.execute("SELECT used_bytes,reserved_bytes,inflight_bytes FROM quota_scopes WHERE scope='bucket' AND scope_id=%s", (bucket['id'],)).fetchone() == expected


def writer(key, exclude=None):
    streams = [row[0] for row in db.execute("SELECT id FROM streams WHERE bucket_id=%s AND object_key=%s AND state='writing'", (bucket['id'], key)) if row[0] != exclude]
    assert len(streams) == 1, streams
    return streams[0]


def expire_writer(stream):
    # Advance only the canceled writer's durable lease; active writers must remain protected.
    assert db.execute("UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE stream_id=%s AND lease_type='write'", (stream,)).rowcount == 1
    cli('gc', 'run')


def put(key, data):
    return s3.put_object(Bucket=prefix, Key=key, Body=data)


def denied(call, **kwargs):
    try:
        call(**kwargs)
    except ClientError as error:
        assert error.response['Error']['Code'] == 'QuotaExceeded', error.response['Error']['Code']
        assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403
    else:
        raise AssertionError('over-quota request succeeded')


def mpu(key):
    value = s3.create_multipart_upload(Bucket=prefix, Key=key)['UploadId']
    uploads.append((key, value))
    return value


def part(key, upload, number, data):
    return s3.upload_part(Bucket=prefix, Key=key, UploadId=upload, PartNumber=number, Body=data)['ETag']


def abort(key, upload):
    s3.abort_multipart_upload(Bucket=prefix, Key=key, UploadId=upload)


def hold(key, size, upload=None):
    params = {'Bucket': prefix, 'Key': key, 'ContentLength': size}
    if upload:
        params.update(UploadId=upload, PartNumber=1)
    target = urlsplit(s3.generate_presigned_url('upload_part' if upload else 'put_object', Params=params, HttpMethod='PUT', ExpiresIn=60))
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=10)
    connection.putrequest('PUT', target.path + '?' + target.query)
    connection.putheader('Content-Length', str(size))
    connection.endheaders()
    connection.send(b'x')
    connections.append(connection)
    return connection


try:
    limits(10)
    put('file', b'a' * 10); wait_counts((10, 0, 0))
    denied(s3.copy_object, Bucket=prefix, Key='copy', CopySource={'Bucket': prefix, 'Key': 'file'})
    put('file', b'b' * 10); wait_counts((10, 0, 0))
    limits(5)
    put('file', b'c' * 9); wait_counts((9, 0, 0))
    denied(s3.put_object, Bucket=prefix, Key='file', Body=b'c' * 10)
    s3.delete_object(Bucket=prefix, Key='file'); wait_counts((0, 0, 0))
    limits(0)
    denied(s3.put_object, Bucket=prefix, Key='zero', Body=b'x')
    put('empty', b''); wait_counts((0, 0, 0))
    limits(10); limits(4, scope='bucket')
    denied(s3.put_object, Bucket=prefix, Key='tight', Body=b'12345')
    limits(scope='bucket')
    print('PASS exact logical accounting, copy, overwrite credit, lower limits, zero and bucket cap', flush=True)

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(put, key, b'123456') for key in ['a', 'b']]
        results = []
        for future in futures:
            try:
                future.result(); results.append('ok')
            except ClientError as error:
                results.append(error.response['Error']['Code'])
    assert sorted(results) == ['QuotaExceeded', 'ok'], results
    wait_counts((6, 0, 0))
    for key in ['a', 'b']:
        s3.delete_object(Bucket=prefix, Key=key)
    wait_counts((0, 0, 0))
    target = s3.generate_presigned_url('put_object', Params={'Bucket': prefix, 'Key': 'unknown'}, HttpMethod='PUT', ExpiresIn=60)
    reply = requests.put(target, data=iter([b'123', b'456']), timeout=15)
    assert reply.status_code == 200, reply.text
    wait_counts((6, 0, 0))
    target = s3.generate_presigned_url('put_object', Params={'Bucket': prefix, 'Key': 'unknown-over'}, HttpMethod='PUT', ExpiresIn=60)
    reply = requests.put(target, data=iter([b'12', b'345']), timeout=15)
    assert reply.status_code == 403 and 'QuotaExceeded' in reply.text, reply.text
    expire_writer(writer('unknown-over'))
    s3.delete_object(Bucket=prefix, Key='unknown'); wait_counts((0, 0, 0))
    print('PASS parallel admission and bounded unknown-length reservations', flush=True)

    # Model cancellation cleanup arriving before the receiver has inserted its reservation.
    pending = hold('cancel-ledger', 10); wait_counts((0, 10, 10))
    stream = db.execute("SELECT id FROM streams WHERE bucket_id=%s AND object_key='cancel-ledger' AND state='writing'", (bucket['id'],)).fetchone()[0]
    db.execute('SELECT quota_drop(%s)', (stream,))
    lock = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text())
    lock.execute("SELECT 1 FROM quota_scopes WHERE scope='project' AND scope_id=%s FOR UPDATE", (project,))
    pids = {}

    def worker(name, statement):
        with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True, options='-c statement_timeout=15000') as client:
            pids[name] = client.execute('SELECT pg_backend_pid()').fetchone()[0]
            client.execute(statement, (stream,))

    with ThreadPoolExecutor(max_workers=2) as workers:
        reserve = workers.submit(worker, 'reserve', 'SELECT quota_reserve(%s,10,false,NULL,NULL)')
        try:
            deadline = time.monotonic() + 10
            while 'reserve' not in pids or db.execute('SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s', (pids['reserve'],)).fetchone()[0] != 'Lock':
                assert time.monotonic() < deadline
                time.sleep(.01)
            reap = workers.submit(worker, 'reap', "UPDATE streams SET state='abandoned' WHERE id=%s")
            while 'reap' not in pids or pids['reserve'] not in db.execute('SELECT pg_blocking_pids(%s)', (pids['reap'],)).fetchone()[0]:
                assert not reap.done() and time.monotonic() < deadline, 'cleanup raced ahead of durable admission'
                time.sleep(.01)
        finally:
            lock.rollback(); lock.close()
        reserve.result(); reap.result()
    pending.close(); wait_counts((0, 0, 0))
    print('PASS cancellation and durable admission serialize on the stream before quota locking', flush=True)

    limits(10, 20)
    put('held', b'0123456789')
    first = hold('held', 10); wait_counts((10, 0, 10))
    first_stream = writer('held')
    second = hold('held', 10); wait_counts((10, 0, 20))
    second_stream = writer('held', first_stream)
    first.close(); expire_writer(first_stream); wait_counts((10, 0, 10))
    assert db.execute("SELECT expires_at>now() FROM stream_leases WHERE stream_id=%s AND lease_type='write'", (second_stream,)).fetchone()[0]
    limits(10, 15)
    denied(s3.put_object, Bucket=prefix, Key='held', Body=b'0123456789')
    second.close(); expire_writer(second_stream); wait_counts((10, 0, 0))
    assert s3.get_object(Bucket=prefix, Key='held')['Body'].read() == b'0123456789'
    s3.delete_object(Bucket=prefix, Key='held'); wait_counts((0, 0, 0))
    print('PASS superseded writers retain in-flight budget until cancellation cleanup', flush=True)

    limits(10, 30)
    upload = mpu('parts')
    part('parts', upload, 1, b'aaaa'); wait_counts((0, 4, 4))
    etag = part('parts', upload, 1, b'bbbbbbb'); wait_counts((0, 7, 7))
    part('parts', upload, 2, b'ccc'); wait_counts((0, 10, 10))
    manifest = {'Parts': [{'PartNumber': 1, 'ETag': etag}]}
    for _ in range(2):
        s3.complete_multipart_upload(Bucket=prefix, Key='parts', UploadId=upload, MultipartUpload=manifest)
        wait_counts((7, 0, 0))
    limits(7, 30)
    a = mpu('parts'); etag = part('parts', a, 1, b'ddddddd'); wait_counts((7, 0, 7))
    b = mpu('parts')
    denied(s3.upload_part, Bucket=prefix, Key='parts', UploadId=b, PartNumber=1, Body=b'eeeeeee')
    abort('parts', b)
    s3.delete_object(Bucket=prefix, Key='parts'); wait_counts((0, 7, 7))
    s3.complete_multipart_upload(Bucket=prefix, Key='parts', UploadId=a, MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag}]})
    wait_counts((7, 0, 0))
    s3.delete_object(Bucket=prefix, Key='parts'); wait_counts((0, 0, 0))
    a = mpu('cancel')
    pending = hold('cancel', 7, a); wait_counts((0, 7, 7))
    pending_stream = writer('cancel')
    abort('cancel', a); wait_counts((0, 0, 7))
    pending.close(); expire_writer(pending_stream); wait_counts((0, 0, 0))
    print('PASS part replacement, partial manifest, completion retry, exclusive credit and abort', flush=True)

    limits(None, None, 1)
    extra = cli('bucket', 'create', prefix + '-extra', '--storage', storage)
    try:
        try:
            db.execute('UPDATE buckets SET project_id=%s WHERE id=%s', (project, extra['id']))
        except psycopg.DatabaseError as error:
            assert error.sqlstate == 'MKQ01'
        else:
            raise AssertionError('bucket count cap bypassed')
    finally:
        cli('bucket', 'delete', extra['name'])
    assert cli('quota', 'get', 'project', project)['bucket_count_limit'] == '1'
    secret = 'Quota-member-' + uuid.uuid4().hex
    user_reply = admin.post(url + '/api/v1/users', json={'username': prefix, 'password': secret, 'role': 'member', 'must_change_password': False})
    assert user_reply.status_code == 201, user_reply.text
    user = user_reply.json()['id']
    reply = admin.put(url + f'/api/v1/projects/{project}/members/{user}', json={'role': 'reader', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list', 'object.read', 'storage.inspect']}]})
    assert reply.status_code == 204, (reply.status_code, reply.text)
    member = requests.Session()
    reply = member.post(url + '/api/v1/auth/login', headers={'Origin': url}, json={'username': prefix, 'password': secret})
    assert reply.status_code == 200, reply.text
    member.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    visible = member.get(url + '/api/v1/quotas/project/' + project).json()
    assert visible['used_bytes'] is None and visible['bucket_count'] is None and visible['bucket_count_limit'] == '1', visible
    assert member.get(url + '/api/v1/quotas/bucket/' + bucket['id']).json()['used_bytes'] == '0'
    assert member.put(url + '/api/v1/quotas/project/' + project, json={'logical_bytes_limit': None}).status_code == 403
    print('PASS bucket count limit, CLI view and scoped quota privacy', flush=True)
    put('purge', b'abcde')
    expired = mpu('expired')
    part('expired', expired, 1, b'xyz')
    wait_counts((5, 3, 3))
    db.execute("UPDATE uploads SET touched_at=now()-interval '100 years' WHERE id=%s", (expired,))
    cli('gc', 'run')
    wait_counts((5, 0, 0))
    assert db.execute('SELECT state FROM uploads WHERE id=%s', (expired,)).fetchone()[0] == 'aborted'
    # Every writer above has ended; advance remaining rejected writes before testing full purge.
    db.execute("UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE lease_type='write' AND stream_id IN (SELECT id FROM streams WHERE bucket_id=%s)", (bucket['id'],))
    deadline = time.monotonic() + 20
    while db.execute("SELECT count(*) FROM streams WHERE bucket_id=%s AND state='writing'", (bucket['id'],)).fetchone()[0]:
        assert time.monotonic() < deadline, 'ended writers were not reclaimed'
        cli('gc', 'run')
        time.sleep(.1)
    preview = admin.post(url + f"/api/v1/buckets/{bucket['id']}/purge/preview")
    assert preview.status_code == 200, preview.text
    purge = admin.post(url + f"/api/v1/buckets/{bucket['id']}/purge", json={'confirm_name': prefix, 'confirmation': preview.json()['confirmation']})
    assert purge.status_code == 200, purge.text
    task = purge.json()['task_id']
    deadline = time.monotonic() + 45
    while db.execute('SELECT state FROM tasks WHERE id=%s', (task,)).fetchone()[0] != 'completed':
        assert time.monotonic() < deadline, 'purge did not complete'
        cli('gc', 'run')
        time.sleep(.2)
    assert snapshot() == (0, 0, 0)
    assert db.execute("SELECT bucket_count FROM quota_scopes WHERE scope='project' AND scope_id=%s", (project,)).fetchone()[0] == 0
    assert not db.execute('SELECT 1 FROM quota_reservations WHERE bucket_id=%s', (bucket['id'],)).fetchone()
    assert db.execute("SELECT count(*) FROM quota_scopes q WHERE scope='bucket' AND (used_bytes<>(SELECT coalesce(sum(s.size),0) FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=q.scope_id) OR reserved_bytes<>(SELECT coalesce(sum(greatest(r.logical_bytes-r.credit_bytes,0)),0) FROM quota_reservations r WHERE r.bucket_id=q.scope_id) OR inflight_bytes<>(SELECT coalesce(sum(r.inflight_bytes),0) FROM quota_reservations r WHERE r.bucket_id=q.scope_id))").fetchone()[0] == 0
    print('PASS upload expiry, full bucket purge and independent ledger reconciliation', flush=True)
finally:
    for connection in connections:
        connection.close()
    limits()
    for key, upload in uploads:
        try:
            abort(key, upload)
        except ClientError:
            pass
    if user:
        admin.delete(url + '/api/v1/users/' + user)
    cli('cred', 'disable', credential['access_key'])
    db.close()
