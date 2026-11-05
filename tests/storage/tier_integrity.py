"""Check cold sources through a separate data node and integrity worker."""
import os
import random

from botocore.exceptions import BotoCoreError, ClientError
from urllib3.exceptions import ProtocolError

from tests.storage.replication_tiering import configure, wait_for
from tests.storage.source_quarantine import Fixture


def audit(source, mode):
    task = source.finished(source.cli('integrity', 'check', '--mode', mode, '--bucket', source.name), seconds=300)
    if task['detail'].get('role') == 'parent':
        child, = [row for row in source.cli('task', 'list') if row.get('parent_task_id') == task['id']]
        task = source.finished({'task_id': child['id']}, seconds=300)
    return source.api('/api/v1/tasks/' + task['id'] + '/issues')['items']


def pack_object(source, route):
    task = source.cli('pack', 'run')

    def packed():
        nonlocal task
        status = source.api('/api/v1/tasks/' + task['task_id'])['status']
        if source.api(route + '/usage?unit=pack') and not source.api(route + '/usage?unit=chunk'):
            return True
        assert status not in ('failed', 'paused'), (task, status)
        if status == 'completed':
            # A run already in progress can have a cutoff before this upload.
            assert task.get('existing'), 'pack task completed without packing the fixture'
            task = source.cli('pack', 'run')
        return False

    wait_for(packed, 'fixture chunks did not reach a pack', seconds=600)


def check(unit):
    source = Fixture()
    cold = Fixture(backend_file=os.environ['MOKYU_TEST_TIER_COLD_BACKEND'])
    configure(source, raw=unit == 'raw', pack=unit == 'pack')
    if unit == 'pack':
        for store in (source, cold):
            key = store.cli('key', 'create', store.name, 'block', '-a', 'aes-256-gcm')
            store.cli('storage', 'key', 'set', store.name, key['name'])
    data = random.Random(unit).randbytes(6 << 20 if unit == 'pack' else 32 << 10)
    source.put('cold', data)
    route = '/api/v1/buckets/' + source.bucket_id + '/tiering'
    if unit == 'pack':
        pack_object(source, route)
    policy = dict(cold_storage=cold.store['id'], cool_after_seconds=1, observation_seconds=1,
                  promote_reads=1000, residency_seconds=1, parallelism=2, bytes_per_second=64 << 20)
    result = source.session.put(source.web + route, headers=source.headers,
                                json=dict(revision=0, policy=policy), timeout=30)
    result.raise_for_status()

    def placed():
        rows = source.api(route + '/usage?unit=' + unit)
        return rows and all(row['cold_bytes'] > 0 and
                            (row['hot_bytes'] == 0 or row['phase'] == 'retiring') for row in rows)

    wait_for(placed, 'source did not reach cold storage')
    source.api(route + '/state', dict(revision=source.api(route)['revision'], action='pause'))
    source.put('hot', b'hot object in the same audit')
    for mode in ('head', 'full'):
        issues = audit(source, mode)
        assert not issues, (unit, mode, issues)
    source.read('cold', data)

    # The physical backend holds a different key from the logical source.
    path = cold.keys()[0]
    original = cold.bytes(path)
    listing = '/api/v1/maintenance/quarantines?source=' + unit + '&storage_id='
    try:
        cold.backend.delete_object(Bucket=cold.remote_bucket, Key=path)
        try:
            with source.client.get_object(Bucket=source.name, Key='cold')['Body'] as stream:
                stream.read()
        except (BotoCoreError, ClientError, ProtocolError):
            pass
        else:
            raise AssertionError('missing cold source was returned')
        rows = wait_for(lambda: source.api(listing + source.store['id'])['items'],
                        'worker did not confirm the missing cold source')
        row, = rows
        assert row['code'] == 'remote_missing' and not source.api(listing + cold.store['id'])['items'], row
    finally:
        cold.bytes(path, original)
    source.api('/api/v1/account/reauth', dict(password=source.password), expected=204)
    source.recheck(dict(storage_id=source.store['id'], source=unit, id=row['id']))
    assert not source.api(listing + source.store['id'])['items']
    source.read('cold', data)
    source.read('hot', b'hot object in the same audit')
    print('PASS', unit, 'mixed-tier audits, cold fault report, source recheck and restored reads', flush=True)


if __name__ == '__main__':
    for unit in ('raw', 'chunk', 'pack'):
        check(unit)
