"""Exercise public cross-store copies and differential publication in isolated stores."""
import os
import random

from tests.storage.source_quarantine import Fixture, wait


def run():
    source, target = Fixture(), Fixture()
    source_raw = os.environ.get('MOKYU_TEST_REPLICATION_MODE') == 'raw'
    target_raw = os.environ.get('MOKYU_TEST_REPLICATION_TARGET', 'raw' if source_raw else 'chunks') == 'raw'
    for fixture, raw in [(source, source_raw), (target, target_raw)]:
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=not raw, deduplication_enabled=not raw,
                       pack_enabled=False, pack_range_enabled=False, compression_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    data = random.Random(75).randbytes(6 * 1024 * 1024) + b'public transfer'
    source.put('original', data)
    source.client.copy_object(Bucket=source.name, Key='alias', CopySource={'Bucket': source.name, 'Key': 'original'})
    source.client.put_object_acl(Bucket=source.name, Key='original', ACL='public-read')
    source.put('empty', b'')
    target.put('existing', b'untouched target')

    def task(keys, **changes):
        value = dict(source_bucket=source.bucket_id, target_bucket=target.bucket_id,
                     operation='copy', scope=dict(selection='objects', keys=keys))
        value.update(changes)
        identifier = source.api('/api/v1/replications', value, expected=202)['task_id']
        result = wait(lambda: (row if (row := source.api('/api/v1/replications/' + identifier))['phase'] == 'complete'
                              or row['state'] == 'failed' else None), 'cross-store task did not finish', seconds=240)
        return result

    def public(key):
        reply = target.client.get_object_acl(Bucket=target.name, Key=key)
        return any(grant['Grantee'].get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers'
                   and grant['Permission'] == 'READ' for grant in reply['Grants'])

    keys = ['original', 'alias', 'empty']
    copied = task(keys)
    assert copied['state'] == 'completed' and copied['progress']['completed'] == 3, copied
    assert copied['progress']['sources'] > 0, copied
    for key in ('original', 'alias'):
        target.read(key, data)
        with target.client.get_object(Bucket=target.name, Key=key, Range='bytes=1048571-1048585')['Body'] as body:
            assert body.read() == data[1048571:1048586]
    target.read('empty', b'')
    assert public('original') and not public('alias')
    repeated = task(keys)
    assert repeated['state'] == 'completed' and repeated['progress']['skipped'] == 3, repeated
    assert repeated['progress']['sources'] == 0, repeated
    source.client.put_object_acl(Bucket=source.name, Key='original', ACL='private')
    changed = task(keys)
    assert changed['state'] == 'completed' and changed['progress']['completed'] == 1, changed
    assert changed['progress']['skipped'] == 2 and changed['progress']['sources'] == 0, changed
    assert not public('original')
    target.read('original', data)
    private = task(['original'], permissions='public_read')
    assert private['state'] == 'completed' and private['progress']['sources'] == 0 and public('original'), private
    ignored = task(['original'], permissions='ignore')
    assert ignored['state'] == 'completed' and ignored['progress']['skipped'] == 1 and public('original'), ignored
    moved = task(['original'], operation='move', permissions='private')
    assert moved['state'] == 'completed' and moved['progress']['sources'] == 0, moved
    assert not source.client.list_objects_v2(Bucket=source.name, Prefix='original').get('Contents')
    target.read('original', data)
    assert not public('original')
    source.read('alias', data)
    target.read('existing', b'untouched target')
    source.put('different', b'aaaa')
    target.put('different', b'bbbb')
    conflict = task(['different'], permissions='public_read')
    assert conflict['state'] == 'failed' and conflict['progress']['conflicts'] == 1, conflict
    assert conflict['progress']['sources'] == 0 and not public('different'), conflict
    target.read('different', b'bbbb')
    checks = ['public cross-store transfer preserves complete/range bytes, aliases, empty objects and ACLs',
              'prior version receipts skip repeated bodies before backend work across payload representations',
              'ACL-only copies and moves reuse confirmed target bodies, preserve unrelated targets and fence conflicts']
    if not source_raw and not target_raw:
        source.put('independent', data)
        target.put('independent', data)
        same = task(['independent'], permissions='public_read')
        assert same['state'] == 'completed' and same['progress']['sources'] == 0 and public('independent'), same
        target.read('independent', data)
        checks.append('independently uploaded chunk layouts compare content hashes and reuse the target body')
    return checks


if __name__ == '__main__':
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    for check in run():
        print('PASS:', check, flush=True)
