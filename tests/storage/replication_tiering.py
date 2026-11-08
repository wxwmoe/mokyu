"""Copy raw files, chunks and packs after their original S3 keys have been retired."""
import os
import random
import time

from tests.storage.source_quarantine import Fixture


def wait_for(check, message, seconds=240):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if value := check():
            return value
        time.sleep(.5)
    raise AssertionError(message)


def configure(fixture, raw, pack=False):
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=not raw, deduplication_enabled=not raw, pack_enabled=pack,
                   min_storage_duration='0s', pack_max_size='16MiB',
                   compression_enabled=pack, write_processing='inline', migration_concurrency=1)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)


def copy(source, target_bucket, target_name, reader, key, payload):
    request = dict(source_bucket=source.bucket_id, target_bucket=target_bucket, operation='copy',
                   scope=dict(selection='objects', keys=['original']), target_key=key)
    task = source.api('/api/v1/replications', request, expected=202)['task_id']

    def finished():
        row = source.api('/api/v1/replications/' + task)
        return row if row['state'] in ('failed', 'completed') else None

    result = wait_for(finished, f'copy did not finish: {task}')
    assert result['state'] == 'completed' and result['progress']['completed'] == 1, result
    with reader.get_object(Bucket=target_name, Key=key)['Body'] as body:
        assert body.read() == payload, (target_name, key)


def run():
    cold = Fixture(backend_file=os.environ['MOKYU_TEST_TIER_COLD_BACKEND'])
    third = Fixture(backend_file=os.environ['MOKYU_TEST_BACKEND'])
    configure(cold, raw=True)
    configure(third, raw=False)
    policy = dict(cold_storage=cold.store['id'], cool_after_seconds=2, observation_seconds=1,
                  promote_reads=1000, residency_seconds=1, parallelism=2, bytes_per_second=64 << 20)
    cases = {}
    for unit in ('raw', 'chunk', 'pack'):
        source = Fixture()
        configure(source, raw=unit == 'raw', pack=unit == 'pack')
        if unit == 'pack':
            block_key = source.cli('key', 'create', source.name, 'block', '-a', 'aes-256-gcm')
            source.cli('storage', 'key', 'set', source.name, block_key['name'])
        data = random.Random(unit).randbytes(6 << 20 if unit == 'pack' else 32 << 10)
        source.put('original', data)
        if unit == 'pack':
            source.finished(source.cli('pack', 'run'))
        route = '/api/v1/buckets/' + source.bucket_id + '/tiering'
        usage = route + '/usage?unit=' + unit
        units = source.api(usage)
        assert units and all(row['hot_bytes'] > 0 for row in units), (unit, units)
        ids = {row['id'].replace('-', '') for row in units}
        original = {key for key in source.keys() if key.rsplit('/', 1)[-1] in ids}
        assert len(original) == len(ids), (unit, original, ids)
        response = source.session.put(source.web + route, headers=source.headers,
                                      json=dict(revision=0, policy=policy), timeout=30)
        assert response.ok, response.text
        cases[unit] = source, data, route, usage, original

    for unit, (source, data, _, usage, original) in cases.items():
        def retired():
            rows = source.api(usage)
            return (rows and all(row['cold_bytes'] > 0 and row['hot_bytes'] == 0 for row in rows)
                    and original.isdisjoint(source.keys()))
        wait_for(retired, f'{unit} did not retire its original S3 keys')
        for target in (cold, third):
            copy(source, target.bucket_id, target.name, target.client, 'cold-' + unit, data)

    # Exercise encoded writes to the same physical backend with just one migration permit.
    configure(cold, raw=False)
    source, data, *_ = cases['chunk']
    copy(source, cold.bucket_id, cold.name, cold.client, 'cold-encoded', data)
    source, data, *_ = cases['raw']
    name = source.name + '-alias'
    sibling = source.cli('bucket', 'create', name, '--storage', source.store['id'])
    sibling_route = '/api/v1/buckets/' + sibling['id'] + '/tiering'
    response = source.session.put(source.web + sibling_route, headers=source.headers,
                                  json=dict(revision=0, policy=policy), timeout=30)
    assert response.ok, response.text
    source.cli('cred', 'grant', source.credentials['access_key'], name)
    copy(source, sibling['id'], name, source.client, 'same-home', data)

    source.api(sibling_route + '/state', dict(revision=source.api(sibling_route)['revision'], action='drain'))
    for source, _, route, _, _ in cases.values():
        source.api(route + '/state', dict(revision=source.api(route)['revision'], action='drain'))
    # Fresh raw outputs force the encoded source reader to open the new hot address.
    configure(third, raw=True)
    for unit, (source, data, _, usage, original) in cases.items():
        def promoted():
            rows = source.api(usage)
            return rows and all(row['hot_bytes'] > 0 and row['phase'] in (None, 'retiring') for row in rows)
        wait_for(promoted, f'{unit} did not return to hot storage')
        assert original.isdisjoint(source.keys())
        copy(source, third.bucket_id, third.name, third.client, 'promoted-' + unit, data)
    print('PASS tiered raw, chunk and pack copies, same-backend permits and retired S3 keys', flush=True)


if __name__ == '__main__':
    run()
