"""Exact recheck cannot be deleted by another worker before its final CAS."""
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture = Fixture()
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(upload_retention='1s', unreferenced_grace='1s', min_storage_duration='0s', deleted_chunk_retention='1d', integrity_batch_timeout='120s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    raw = b'physical protection during exact recheck\n' * 1024
    fixture.put('original', raw)
    physical = wait(fixture.keys, 'source was not listed')[0]
    original = fixture.bytes(physical)
    fixture.bytes(physical, b'!')
    scope = fixture.scope()
    fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
    fixture.bytes(physical, original)
    marker = fixture.arm('source-before-commit')
    try:
        recheck = fixture.execute('recheck', fixture.preview('recheck', scope))
        wait(lambda: marker.with_suffix('.hit').exists(), 'recheck did not reach final CAS')
        fixture.client.delete_object(Bucket=fixture.name, Key='original')
        def retired():
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            listed = fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=chunk')['items']
            return listed and listed[0]['state'] != 'ready'
        wait(retired, 'GC did not retire the unreferenced source', seconds=30)
        # The live distributed recheck protects this exact location even after its logical owner retires.
        if fixture.backend:
            assert fixture.bytes(physical) == original
    finally:
        marker.unlink(missing_ok=True)
    fixture.finished(recheck, 'failed')
    fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
    listed = fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=chunk')['items']
    assert listed and listed[0]['state'] in ('retired','deleting','deleted')
    print('PASS recheck versus deletion and independent GC preserves protection and rejects a retired-source CAS', flush=True)


if __name__ == '__main__':
    main()