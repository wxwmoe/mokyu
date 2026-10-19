"""Logical raw/chunk migration obeys the target profile and preserves source content."""
import os
import uuid
from urllib.parse import urlencode

from tests.storage.source_quarantine import Fixture


def main():
    remote = os.environ.pop("MOKYU_TEST_BACKEND", None)
    source = Fixture()
    if remote:
        os.environ["MOKYU_TEST_BACKEND"] = remote
    target = Fixture()
    assert source.backend is None
    profile = source.cli('storage', 'profile', 'show', source.name)
    profile['cdc_enabled'] = False
    source.cli('storage', 'profile', 'set', source.name, '-', value=profile)
    key = source.name + '-key'
    source.cli('key', 'create', key, 'block', '-a', 'aes-256-gcm')
    target.cli('storage', 'key', 'set', target.name, key)
    data = b'raw logical migration' * 400001
    source.put('original', data)

    def item(fixture, name):
        items = fixture.api('/api/v1/buckets/' + fixture.bucket_id + '/objects?' + urlencode({'prefix': name}))['items']
        return next(row for row in items if row['object_key'] == name)

    def migrate(origin, destination, name, output, action):
        current = item(origin, name)
        reply = origin.api('/api/v1/media/actions', dict(bucket=origin.bucket_id, action=action,
            target_bucket=destination.bucket_id, confirm_storage_migration=True,
            objects=[dict(client_id=str(uuid.uuid4()), key=name, version=current['id'], target_key=output)]))
        result = reply['results'][0]
        assert result['status'] == 202, reply
        origin.finished(result)
        destination.read(output, data)
        row = item(destination, output)
        detail = destination.api('/api/v1/buckets/' + destination.bucket_id + '/object/chunks?' +
                                 urlencode({'key': output, 'version': row['id']}))
        return detail

    chunks = migrate(source, target, 'original', 'encrypted', 'copy')
    assert chunks['payload_type'] == 'chunks' and chunks['items']
    assert all(chunk['algorithm'] == 'aes-256-gcm' for chunk in chunks['items']), chunks
    source.read('original', data)
    raw = migrate(target, source, 'encrypted', 'returned', 'move')
    assert raw['payload_type'] == 'raw' and not raw['items']
    assert not target.api('/api/v1/buckets/' + target.bucket_id + '/objects')['items']
    source.read('original', data)
    print('PASS raw to encrypted chunks copy and encrypted chunks to raw move preserve logical bytes', flush=True)
    target.cli('storage', 'key', 'clear', target.name)
    profile = target.cli('storage', 'profile', 'show', target.name)
    profile.update(cdc_enabled=False, deduplication_enabled=True, pack_range_enabled=False)
    target.cli('storage', 'profile', 'set', target.name, '-', value=profile)
    raw = migrate(source, target, 'original', 'raw-copy', 'copy')
    assert raw['payload_type'] == 'raw' and not raw['items']
    source.read('original', data)
    raw = migrate(target, source, 'raw-copy', 'raw-returned', 'move')
    assert raw['payload_type'] == 'raw' and not raw['items']
    assert not target.api('/api/v1/buckets/' + target.bucket_id + '/objects')['items']
    print('PASS raw copy and move between independent local or S3 storages preserve logical bytes', flush=True)


if __name__ == '__main__':
    main()
