"""Actual S3 output, external drift and credential recovery for an isolated persistent link."""

import os, uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def run(restart, backend):
    src, peer, native = Fixture(), Fixture(), Fixture()
    db = Database()
    project = src.api('/api/v1/projects')[0]['id']
    base = '/api/v1/buckets/' + src.bucket_id + '/redundancy'
    catalog = '/api/v1/external-buckets'
    checks = []

    def add(bucket, endpoint, creds, prefix, region='us-east-1'):
        return src.api(
            catalog,
            dict(
                project_id=project,
                name='redundancy-' + uuid.uuid4().hex[:12],
                connection=dict(
                    endpoint_url=endpoint, bucket=bucket, region=region, prefix=prefix, allow_http=True
                ),
                **{k: creds[k] for k in ('access_key', 'secret_key')},
            ),
            201,
        )

    def patch(resource, **change):
        reply = src.session.patch(
            src.web + catalog + '/' + resource['id'],
            headers=src.headers,
            json=dict(revision=resource['revision'], **change),
            timeout=30,
        )
        assert reply.status_code == 200, (reply.status_code, reply.text)
        return reply.json()

    def state(link):
        return src.api(base + '/' + link)

    def status(link, want):
        def poll():
            row = state(link)
            if row['status'] == 'failed' and want != 'failed':
                raise AssertionError(row)
            return row if row['status'] == want else None

        return wait(poll, 'external relation did not reach ' + want, seconds=180)

    def command(link, action, code=202, **kw):
        return src.api(
            base + '/' + link + '/command', dict(action=action, revision=state(link)['revision'], **kw), code
        )

    def create(settings):
        return src.api(base, settings, 202)['task_id']

    def remove(link):
        command(link, 'remove')
        wait(lambda: link not in [x['id'] for x in src.api(base)['links']], 'unlink stalled', seconds=180)

    def put(f, k, data, public=False, metadata=None):
        f.client.put_object(
            Bucket=f.name,
            Key=k,
            Body=data,
            ACL='public-read' if public else 'private',
            Metadata=metadata or {'fixture': 'outgoing'},
            ContentType='text/plain',
        )

    def body(client, bucket, key):
        with client.get_object(Bucket=bucket, Key=key)['Body'] as stream:
            return stream.read()

    def public(f, k):
        return any(
            grant['Permission'] == 'READ' and grant['Grantee'].get('URI', '').endswith('/AllUsers')
            for grant in f.client.get_object_acl(Bucket=f.name, Key=k)['Grants']
        )

    def absent(f, key):
        try:
            f.client.head_object(Bucket=f.name, Key=key)
        except ClientError as err:
            assert err.response['ResponseMetadata']['HTTPStatusCode'] == 404, err
            return True
        return False

    def retained(resource):
        reply = src.session.delete(
            src.web + catalog + '/' + resource['id'],
            headers=src.headers,
            json=dict(revision=resource['revision']),
            timeout=30,
        )
        assert reply.status_code == 409, (reply.status_code, reply.text)

    remote = add(peer.name, os.environ['MOKYU_TEST_ENDPOINT'], peer.credentials, 'managed')
    direct = create(dict(target_bucket=native.bucket_id, target_prefix='native/'))
    put(src, 'one', b'one generation', True)
    put(src, 'two', b'private two')
    put(peer, 'managed/out/untouched', b'retain target-only key')
    settings = dict(
        target_bucket=remote['id'], target_external=True, target_prefix='out/', permissions='inherit'
    )
    src.api(base + '/preview', {**settings, 'propagate_deletes': True}, 400)
    link = create(settings)
    assert status(link, 'healthy')['confirmed'] == 2
    assert status(direct, 'healthy')['confirmed'] == 2
    assert public(peer, 'managed/out/one') and not public(peer, 'managed/out/two')
    output = peer.client.head_object(Bucket=peer.name, Key='managed/out/one')['Metadata']
    assert output['fixture'] == 'outgoing' and output['mokyu-replica-origin'] == link, output
    assert (
        body(native.client, native.name, 'native/one')
        == body(peer.client, peer.name, 'managed/out/one')
        == b'one generation'
    )
    put(peer, 'managed/out/one', b'independent drift')
    command(link, 'reconcile', prefix='one')
    status(link, 'healthy')
    assert body(peer.client, peer.name, 'managed/out/one') == b'one generation' and public(
        peer, 'managed/out/one'
    )
    assert body(peer.client, peer.name, 'managed/out/untouched') == b'retain target-only key'
    checks.append(
        'mixed native and ordinary Mokyu S3 targets, inherited public/private ACLs, origin and user metadata, reconciliation repairs external drift'
    )
    remote = patch(remote, enabled=False)
    put(src, 'offline', b'latest while disabled')
    row = status(link, 'failed')
    assert row['pending'] > 0 and row['confirmed'] == 2 and absent(peer, 'managed/out/offline'), row
    restart(lambda: None)
    assert state(link)['pending'] > 0
    remote = patch(remote, enabled=True)
    command(link, 'retry')
    status(link, 'healthy')
    assert body(peer.client, peer.name, 'managed/out/offline') == b'latest while disabled'
    command(link, 'pause')
    status(link, 'paused')
    retained(remote)
    binding = remote['connection']
    remote = patch(remote, connection={**binding, 'prefix': 'redirected'})
    command(link, 'resume', 409)
    assert state(link)['status'] == 'paused' and absent(peer, 'redirected/out/one')
    remote = patch(remote, connection=binding)
    command(link, 'resume')
    status(link, 'healthy')
    checks.append(
        'disabled external credential leaves a recoverable lag; restart and authorized retry catch up; paused reference cannot be removed or redirected on resume'
    )
    provider = add(
        backend['bucket'], backend['endpoint'], backend, 'non-acl-' + uuid.uuid4().hex, backend['region']
    )
    ignore = create(dict(target_bucket=provider['id'], target_external=True))
    plain = boto3.client(
        's3',
        endpoint_url=backend['endpoint'],
        region_name=backend['region'],
        aws_access_key_id=backend['access_key'],
        aws_secret_access_key=backend['secret_key'],
        config=Config(
            s3={'addressing_style': 'path'},
            retries={'max_attempts': 0},
            request_checksum_calculation='when_required',
        ),
    )
    assert status(ignore, 'healthy')['confirmed'] == 3
    assert body(plain, backend['bucket'], provider['connection']['prefix'] + '/one') == b'one generation'
    plain.close()
    remove(ignore)
    checks.append(
        'separate external S3 backend copies public sources without requiring object ACL operations under default ignore policy'
    )

    alias = add(src.name, os.environ['MOKYU_TEST_ENDPOINT'], src.credentials, 'loop')
    loop = create(dict(target_bucket=alias['id'], target_external=True))
    status(loop, 'failed')
    assert not src.client.list_objects_v2(Bucket=src.name, Prefix='loop/').get('Contents')
    assert body(src.client, src.name, 'one') == b'one generation'
    remove(loop)
    try:
        put(src, 'bad-input', b'forged', metadata={'mokyu-replica-origin': 'invented'})
    except ClientError as err:
        assert err.response['Error']['Code'] == 'InvalidBucketState', err.response
    else:
        raise AssertionError('marked source write was accepted')
    assert absent(src, 'bad-input')
    for k in (link, direct):
        remove(k)
    put(src, 'marked', b'already present', metadata={'mokyu-replica-origin': 'invented'})
    again = create(settings)
    row = status(again, 'lagging')
    assert row['progress']['conflicts'] == 1 and absent(peer, 'managed/out/marked'), row
    items = src.api('/api/v1/replications/' + again + '/items?limit=30')['items']
    assert any(i['error'] == 'replica_origin' for i in items), items
    put(src, 'marked', b'corrected user metadata')
    status(again, 'healthy')
    assert body(peer.client, peer.name, 'managed/out/marked') == b'corrected user metadata'
    remove(again)
    checks.append(
        'opaque same-deployment alias cannot overwrite source or amplify keys; reserved user marker cannot silently exempt a write; old marked source produces visible recoverable conflict'
    )
    for f in (src, peer, native):
        f.client.close()
        f.session.close()
        if f.backend:
            f.backend.close()
    return checks
