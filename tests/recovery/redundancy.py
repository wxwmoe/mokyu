"""Exercise bucket redundancy through the real control and S3 servers in an isolated fixture."""

import concurrent.futures
import secrets
import shutil
import uuid
import requests

from botocore.exceptions import ClientError
from tests.storage.source_quarantine import Fixture, wait
from tests.recovery.source_repair import Database


def run(restart):
    source, target, secondary = Fixture(), Fixture(), Fixture()
    db = Database()
    base = '/api/v1/buckets/' + source.bucket_id + '/redundancy'
    checks = []

    def url(bucket):
        return '/api/v1/buckets/' + bucket + '/redundancy'

    def status(link, want, timeout=150):
        def observe():
            row = source.api(base + '/' + link)
            if row['status'] == 'failed' and want != 'failed':
                raise AssertionError((row, source.api('/api/v1/replications/' + link)))
            return row if row['status'] == want else None

        return wait(observe, 'link did not reach ' + want, seconds=timeout)

    def command(link, action, **extra):
        row = source.api(base + '/' + link)
        return source.api(
            base + '/' + link + '/command', dict(revision=row['revision'], action=action, **extra), 202
        )

    def put(f, key, data, public=False):
        f.client.put_object(
            Bucket=f.name,
            Key=key,
            Body=data,
            ACL='public-read' if public else 'private',
            Metadata={'case': 'redundancy'},
            ContentType='text/plain',
        )

    def read(f, key):
        with f.client.get_object(Bucket=f.name, Key=key)['Body'] as response:
            return response.read()

    def public(f, key):
        grants = f.client.get_object_acl(Bucket=f.name, Key=key)['Grants']
        return any(
            g['Permission'] == 'READ' and g['Grantee'].get('URI', '').endswith('/AllUsers') for g in grants
        )

    def absent(f, key):
        try:
            f.client.head_object(Bucket=f.name, Key=key)
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 404, error
            return True
        return False

    def denied(call):
        try:
            call()
        except ClientError as error:
            assert error.response['Error']['Code'] == 'AccessDenied', error.response
            # S3 can carry a completion error in a 200 body; botocore reports it as 500.
            expected = (403, 500) if error.operation_name == 'CompleteMultipartUpload' else (403,)
            assert error.response['ResponseMetadata']['HTTPStatusCode'] in expected, error.response
            return
        raise AssertionError('managed prefix accepted a direct mutation')

    for key in ['original', 'keep', 'gone', 'resumed', 'drift']:
        put(source, key, ('initial:' + key).encode(), key == 'original')
    put(target, 'mirror/unrelated', b'preexisting target-only object')
    put(target, 'mirror/original', b'older target')
    put(target, 'outside', b'unmanaged')
    upload = target.client.create_multipart_upload(Bucket=target.name, Key='mirror/inflight')['UploadId']
    part = target.client.upload_part(
        Bucket=target.name, Key='mirror/inflight', UploadId=upload, PartNumber=1, Body=b'before configuration'
    )
    settings = dict(target_bucket=target.bucket_id, target_prefix='mirror/', propagate_deletes=True)
    preview = source.api(base + '/preview', settings)
    assert preview['source_storage'] == source.store['id'] and preview['target_storage'] == target.store['id']
    if db.type == 'mysql':
        import pymysql

        cfg = db.settings
        with pymysql.connect(
            host=cfg['host'], port=cfg.get('port', 3306), user=cfg['user'],
            password=cfg['password'], database=cfg['name'], autocommit=False,
        ) as reader:
            with reader.cursor() as cursor:
                cursor.execute(
                    'SELECT id FROM buckets WHERE id=%s LOCK IN SHARE MODE', (uuid.UUID(source.bucket_id).bytes,)
                )
                assert cursor.fetchone()
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                previews = [
                    pool.submit(
                        requests.post, source.web + base + '/preview', json=settings,
                        headers=source.headers, cookies=source.session.cookies.get_dict(), timeout=8,
                    )
                    for _ in range(2)
                ]
                try:
                    for request in previews:
                        response = request.result(timeout=12)
                        assert response.status_code == 200 and response.json() == preview
                finally:
                    reader.rollback()
        checks.append('concurrent previews complete while another transaction holds a shared bucket lock')
    source.api(base + '/preview', dict(target_bucket=source.bucket_id), 400)
    same = source.cli('bucket', 'create', source.name + '-alias', '--storage', source.store['id'])
    source.api(base + '/preview', dict(target_bucket=same['id']), 400)
    ids = [str(uuid.uuid4()), str(uuid.uuid4())]

    def contend(key):
        reply = source.session.post(
            source.web + base, json=settings, headers={**source.headers, 'Idempotency-Key': key}, timeout=90
        )
        return reply.status_code, reply.json(), key

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        results = list(pool.map(contend, ids))
    assert sorted(v[0] for v in results) in ([202, 400], [202, 409]), results
    _, reply, key = next(v for v in results if v[0] == 202)
    link = reply['task_id']
    rejected = next(v for v in results if v[0] != 202)
    assert contend(rejected[2])[0] == 400
    assert [row['id'] for row in source.api(base)['links']] == [link]
    assert link == key
    assert source.api(base, settings, 202, headers={**source.headers, 'Idempotency-Key': key}) == reply
    source.api(url(target.bucket_id) + '/preview', dict(target_bucket=secondary.bucket_id), 400)
    source.api(
        url(secondary.bucket_id) + '/preview',
        dict(target_bucket=target.bucket_id, target_prefix='mirror/nested/'),
        400,
    )
    source.api(base, dict(target_bucket=target.bucket_id, target_prefix='another/'), 400)
    source.api('/api/v1/replications/' + link + '/command', 'pause', 400)
    assert link not in [v['id'] for v in source.api('/api/v1/replications')['replications']]
    checks.append(
        'independent storage and preview, competing creation, idempotency, single-layer and scope guards'
    )
    second = source.api(base, dict(target_bucket=secondary.bucket_id, target_prefix='copy/'), 202)['task_id']
    one = status(link, 'healthy')
    two = status(second, 'healthy')
    assert one['pending'] == two['pending'] == 0 and one['confirmed'] == two['confirmed'] == 5, (one, two)
    assert read(target, 'mirror/original') == b'initial:original' and public(target, 'mirror/original')
    assert read(secondary, 'copy/original') == b'initial:original' and public(secondary, 'copy/original')
    assert read(target, 'mirror/unrelated') == b'preexisting target-only object'
    assert source.api(url(target.bucket_id))['links'][0]['id'] == link
    denied(lambda: put(target, 'mirror/new', b'forbidden'))
    denied(lambda: target.client.delete_object(Bucket=target.name, Key='mirror/original'))
    denied(lambda: target.client.put_object_acl(Bucket=target.name, Key='mirror/original', ACL='private'))
    denied(
        lambda: target.client.copy_object(
            Bucket=target.name, Key='mirror/copy', CopySource={'Bucket': target.name, 'Key': 'outside'}
        )
    )
    denied(lambda: target.client.create_multipart_upload(Bucket=target.name, Key='mirror/new-multipart'))
    denied(
        lambda: target.client.complete_multipart_upload(
            Bucket=target.name,
            Key='mirror/inflight',
            UploadId=upload,
            MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]},
        )
    )
    assert absent(target, 'mirror/inflight')
    target.client.abort_multipart_upload(Bucket=target.name, Key='mirror/inflight', UploadId=upload)
    put(target, 'mirror-other', b'outside range')
    checks.append(
        'two independent receipts and ACL/metadata, target-only keys retained, guarded PUT/Copy/ACL/Delete/new and previously started multipart'
    )
    def update(lane):
        for i in range(lane, 18, 3):
            if lane == 0:
                source.client.put_object_acl(
                    Bucket=source.name, Key='original', ACL='public-read' if i % 2 else 'private'
                )
            else:
                put(source, 'updates/' + str(lane), str(i).encode())

    with concurrent.futures.ThreadPoolExecutor(3) as pool:
        list(pool.map(update, range(3)))

    key = 'updates/race'
    sql_key = key if db.type == 'postgres' else key.encode()
    marker = source.arm('source-before-publish')
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        first = pool.submit(put, source, key, b'superseded')
        try:
            wait(lambda: marker.with_suffix('.hit').exists(), 'first PUT did not reach publication')
            winning_write = pool.submit(put, source, key, b'winner')
            wait(
                lambda: len(db.query(
                    "SELECT id FROM streams WHERE bucket_id=%s AND object_key=%s AND state='writing'",
                    uuid.UUID(source.bucket_id), sql_key,
                )) == 2,
                'second PUT did not acquire a new write generation',
            )
        finally:
            marker.unlink(missing_ok=True)
        winning_write.result(timeout=30)
        try:
            first.result(timeout=30)
        except ClientError as error:
            assert error.response['Error']['Code'] == 'OperationAborted', error.response
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 409, error.response
        else:
            raise AssertionError('superseded PUT acknowledged')
    put(source, 'original', b'final replacement')
    source.client.delete_object(Bucket=source.name, Key='gone')
    for dest, prefix in [(target, 'mirror/'), (secondary, 'copy/')]:
        wait(lambda: read(dest, prefix + 'original') == b'final replacement', 'latest bytes not visible')
        assert not public(dest, prefix + 'original')
    status(link, 'healthy')
    status(second, 'healthy')
    for bucket, prefix in [(source, ''), (target, 'mirror/'), (secondary, 'copy/')]:
        for suffix, value in [('1', b'16'), ('2', b'17'), ('race', b'winner')]:
            assert read(bucket, prefix + 'updates/' + suffix) == value
    assert absent(target, 'mirror/gone') and read(secondary, 'copy/gone') == b'initial:gone'
    checks.append(
        'racing ACL and object changes converge; opted-in deletion and retained-copy policies differ as configured'
    )
    old = source.api(base + '/' + link)['revision']
    command(link, 'pause')
    row = status(link, 'paused')
    assert row['confirmed'] is None
    source.api(base + '/' + link + '/command', dict(revision=old, action='resume'), 409)
    denied(lambda: put(target, 'mirror/new', b'while paused'))
    source.client.delete_object(Bucket=source.name, Key='resumed')
    put(source, 'while-paused', b'backfill')
    assert read(target, 'mirror/resumed') == b'initial:resumed'
    restart(lambda: None)
    assert status(link, 'paused')['revision'] == row['revision']
    command(link, 'resume')
    status(link, 'healthy')
    assert absent(target, 'mirror/resumed') and read(target, 'mirror/while-paused') == b'backfill'
    command(link, 'reconcile', prefix='updates/')
    status(link, 'healthy')
    checks.append('revision fence, restart while paused, missing-key reconciliation and scoped rescan')
    for key in [link, second]:
        assert not db.query('SELECT task_id FROM transfer_changes WHERE task_id=%s', uuid.UUID(key))
        assert not db.query(
            'SELECT id FROM transfer_items WHERE task_id=%s AND source_pin IS NOT NULL', uuid.UUID(key)
        )
    cmd = source.api(url(target.bucket_id) + '/' + link)
    source.api(
        url(target.bucket_id) + '/' + link + '/command', dict(action='remove', revision=cmd['revision']), 202
    )
    wait(lambda: not any(row['id'] == link for row in source.api(base)['links']), 'unlink did not drain')
    put(target, 'mirror/original', b'now writable')
    assert read(target, 'mirror/while-paused') == b'backfill'
    command(second, 'remove')
    wait(lambda: not source.api(base)['links'], 'second unlink did not drain')
    assert read(secondary, 'copy/original') == b'final replacement'
    checks.append(
        'no idle queue or source pins; incoming/outgoing unlink preserves replicas and releases only its managed range'
    )
    settings = dict(
        target_bucket=target.bucket_id, target_prefix='bounded/', propagate_deletes=True, max_pending_keys=2
    )
    source.api(base + '/preview', {**settings, 'max_pending_keys': 0}, 400)
    bounded = source.api(base, settings, 202)['task_id']
    assert status(bounded, 'healthy')['queued_keys'] == 0
    marker = source.arm('replication-batch')
    try:
        put(source, 'queue/one', b'first')
        wait(lambda: marker.with_suffix('.hit').exists(), 'replication did not stop between transactions')
        put(source, 'queue/two', b'second')
        assert source.api(base + '/' + bounded)['queued_keys'] == 2
        try:
            put(source, 'queue/three', b'not accepted')
        except ClientError as error:
            assert error.response['Error']['Code'] == 'SlowDown', error.response
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 503, error.response
        else:
            raise AssertionError('full queue admitted a new key')
        assert absent(source, 'queue/three')
        put(source, 'queue/one', b'latest')
        source.client.delete_object(Bucket=source.name, Key='queue/two')
        assert (
            db.query('SELECT queued_keys FROM redundancy_backlogs WHERE id=%s', uuid.UUID(bounded))[0][
                'queued_keys'
            ]
            == 2
        )
        restart(lambda: marker.unlink(missing_ok=True))
    finally:
        marker.unlink(missing_ok=True)
    assert status(bounded, 'healthy')['queued_keys'] == 0
    assert read(target, 'bounded/queue/one') == b'latest' and absent(target, 'bounded/queue/two')
    put(source, 'queue/three', b'retried after recovery')
    wait(lambda: not absent(target, 'bounded/queue/three'), 'recovered target missing new key')
    assert read(target, 'bounded/queue/three') == b'retried after recovery'
    status(bounded, 'healthy')
    command(bounded, 'remove')
    wait(lambda: not source.api(base)['links'], 'bounded relation did not drain')
    assert not db.query('SELECT id FROM redundancy_backlogs WHERE id=%s', uuid.UUID(bounded))
    checks.append(
        'bounded backfill and durable two-key cap; full queue rejects new source publication, coalesces updates and deletion, drains and accepts retry after restart'
    )
    project = next(x['project_id'] for x in source.api('/api/v1/buckets') if x['id'] == source.bucket_id)
    password = secrets.token_urlsafe(28)
    username = 'replica-' + uuid.uuid4().hex[:12]
    user = source.api(
        '/api/v1/users',
        dict(username=username, password=password, role='member', must_change_password=False),
        201,
    )
    membership = source.web + '/api/v1/projects/' + project + '/members/' + user['id']
    source_rights = dict(
        bucket_id=source.bucket_id, actions=['bucket.settings', 'bucket.list', 'object.read']
    )
    target_rights = dict(
        bucket_id=target.bucket_id,
        actions=['bucket.settings', 'bucket.list', 'object.read', 'object.write', 'object.acl'],
    )

    def grant(*grants):
        response = source.session.put(
            membership,
            headers=source.headers,
            json=dict(role='maintainer', scope='selected', grants=list(grants)),
            timeout=30,
        )
        assert response.status_code == 204, response.text

    grant(source_rights, target_rights)
    member = requests.Session()
    login = member.post(
        source.web + '/api/v1/auth/login',
        headers={'Origin': source.web},
        json=dict(username=username, password=password),
        timeout=30,
    )
    assert login.status_code == 200, login.text
    member.headers.update({'Origin': source.web, 'X-CSRF-Token': login.json()['csrf_token']})

    def member_command(bucket, link, action):
        revision = source.api(base + '/' + link)['revision']
        return member.post(
            source.web + url(bucket) + '/' + link + '/command',
            json=dict(revision=revision, action=action),
            timeout=30,
        )

    created = member.post(
        source.web + base,
        json=dict(target_bucket=target.bucket_id, target_prefix='owner/', permissions='ignore'),
        timeout=60,
    )
    assert created.status_code == 202, created.text
    owned = created.json()['task_id']
    status(owned, 'healthy')
    grant(source_rights)
    put(source, 'revoked', b'not authorized to publish')
    status(owned, 'failed')
    assert absent(target, 'owner/revoked')
    assert member_command(source.bucket_id, owned, 'retry').status_code == 403
    assert member_command(source.bucket_id, owned, 'pause').status_code == 202
    status(owned, 'paused')
    grant(source_rights, target_rights)
    profile = target.cli('storage', 'profile', 'show', target.name)
    target.cli(
        'storage',
        'profile',
        'set',
        target.name,
        '-',
        value={**profile, 'unreferenced_grace': '2s' if profile['unreferenced_grace'] == '1s' else '1s'},
    )
    if not target.backend:
        relocated = target.root.parent / (target.name + '-alternate')
        relocated.mkdir()
        shutil.copyfile(target.root / 'mokyu.json', relocated / 'mokyu.json')
        response = target.session.put(
            target.web + '/api/v1/storages/' + target.name,
            headers=target.headers,
            json=dict(path=str(relocated)),
            timeout=30,
        )
        assert response.status_code == 409 and response.json()['code'] == 'OperationAborted', (
            response.status_code,
            response.text,
        )
        assert target.cli('storage', 'show', target.name)['path'] == str(target.root)
    assert member_command(source.bucket_id, owned, 'resume').status_code == 202
    status(owned, 'healthy')
    assert read(target, 'owner/revoked') == b'not authorized to publish'
    grant(target_rights)
    assert member.get(source.web + base, timeout=30).status_code == 403
    assert member_command(target.bucket_id, owned, 'remove').status_code == 202
    wait(
        lambda: not source.api(base)['links'], 'target manager could not detach a revoked source', seconds=180
    )
    assert read(target, 'owner/revoked') == b'not authorized to publish'
    member.close()
    checks.append(
        'least-privilege creator loses target grants before publication, can pause, cannot retry; namespace relocation blocked while linked, same-storage profile refresh and incoming-owner detach preserve replicas'
    )
    for i in range(70):
        put(source, f'initial/{i:02}', b'before scan')
    marker = source.arm('replication-batch')
    try:
        backfill = source.api(
            base, dict(target_bucket=target.bucket_id, target_prefix='backfill/', propagate_deletes=True), 202
        )['task_id']
        wait(lambda: marker.with_suffix('.hit').exists(), 'first backfill batch did not finish')
        assert source.api(base + '/' + backfill)['status'] == 'backfilling'
        checkpoint = db.query('SELECT cursor_key FROM transfer_runs WHERE task_id=%s', uuid.UUID(backfill))
        cursor = checkpoint[0]['cursor_key']
        assert cursor and 'initial/01' <= cursor < 'initial/69', cursor
        put(source, 'initial/00', b'new version', True)
        put(source, '!inserted-behind-cursor', b'new key behind the cursor')
        source.client.delete_object(Bucket=source.name, Key='initial/01')
    finally:
        marker.unlink(missing_ok=True)
    status(backfill, 'healthy', timeout=240)
    assert read(target, 'backfill/initial/00') == b'new version' and public(target, 'backfill/initial/00')
    assert read(target, 'backfill/!inserted-behind-cursor') == b'new key behind the cursor'
    assert absent(target, 'backfill/initial/01') and read(target, 'backfill/initial/69') == b'before scan'
    command(backfill, 'remove')
    wait(lambda: not source.api(base)['links'], 'backfill relation did not drain')
    checks.append('insert, replace and delete behind the initial scan cursor converge to the current source')
    for f in (source, target, secondary):
        f.client.close()
        f.session.close()
        if f.backend:
            f.backend.close()
    return checks
