"""An unrelated raw upload must leave warm public routes and CORS bindings usable."""
import os
import uuid

import requests
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    hot, writer = Fixture(), Fixture()
    assert writer.backend
    db = Database()
    profile = hot.cli('storage', 'profile', 'show', hot.name)
    hot.cli('storage', 'profile', 'set', hot.name, '-', value={
        **profile, 'cdc_enabled': False, 'pack_range_enabled': False, 'public_cache_validation': 'background',
        'public_cache_refresh_interval': '30s', 'public_cache_max_age': '60s', 'cors_cache_enabled': True})
    hot.cli('bucket', 'cors', hot.name, '-', value=[dict(origins=['https://reader.test'], methods=['GET', 'HEAD'])])
    profile = writer.cli('storage', 'profile', 'show', writer.name)
    writer.cli('storage', 'profile', 'set', writer.name, '-', value={
        **profile, 'cdc_enabled': False, 'pack_range_enabled': False, 'deduplication_enabled': False,
        'write_processing': 'inline', 'upload_cache': True})
    keys = [f'hot-{i}' for i in range(8)]
    for key in keys:
        hot.client.put_object(Bucket=hot.name, Key=key, Body=key.encode(), ACL='public-read')
    marker = writer.arm('raw-upload-before-commit')
    try:
        writer.put('pending', b'pending upload')
        wait(lambda: marker.with_suffix('.hit').exists(), 'raw upload did not reach publication')
        def read_batch():
            for key in keys:
                response = requests.head(f"{os.environ['MOKYU_TEST_ENDPOINT']}/{hot.name}/{key}",
                                         headers={'Origin': 'https://reader.test'}, timeout=5)
                assert response.status_code == 200 and response.headers['Access-Control-Allow-Origin'] == 'https://reader.test'
        def hits():
            return hot.cli('status')['public_read_cache']['hits']
        def warm():
            before = hits()
            read_batch()
            return hits() - before >= len(keys)
        wait(warm, 'public routes did not warm')
        before = hot.cli('status')
    finally:
        marker.unlink(missing_ok=True)
    storage = uuid.UUID(writer.store['id'])
    wait(lambda: db.query('SELECT COUNT(*) AS n FROM raw_uploads u JOIN raw_sources r ON r.id=u.source_id WHERE r.storage_id=%s', storage)[0]['n'] == 0,
         'raw upload did not drain')
    read_batch()
    after = hot.cli('status')
    hit_count = after['public_read_cache']['hits'] - before['public_read_cache']['hits']
    invalidated = after['cors_cache']['invalidated'] - before['cors_cache']['invalidated']
    print(f'first warm HEADs after raw upload: {hit_count} hits across {len(keys)} routes, CORS invalidations: {invalidated}', flush=True)
    assert hit_count >= len(keys) and invalidated == 0
    writer.read('pending', b'pending upload')


if __name__ == '__main__':
    main()
