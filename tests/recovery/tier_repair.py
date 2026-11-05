"""Recover a quarantined cold chunk by uploading fresh bytes to its home storage."""
import os

from tests.recovery.source_repair import Database, location, recovery_audit
from tests.storage.replication_tiering import configure, wait_for
from tests.storage.source_quarantine import Fixture, rejected


def run():
    db = Database()
    hot = Fixture()
    cold = Fixture(backend_file=os.environ['MOKYU_TEST_TIER_COLD_BACKEND'])
    configure(hot, raw=False)
    payload = os.urandom(32 << 10)
    etag = hot.put('original', payload)['ETag']
    original, = db.chunks(hot)
    old_keys = set(hot.keys())
    assert len(old_keys) == 1
    route = '/api/v1/buckets/' + hot.bucket_id + '/tiering'
    policy = dict(cold_storage=cold.store['id'], cool_after_seconds=1, observation_seconds=1,
                  promote_reads=1000, residency_seconds=1, parallelism=1, bytes_per_second=64 << 20)
    hot.session.put(hot.web + route, headers=hot.headers, json=dict(revision=0, policy=policy),
                    timeout=30).raise_for_status()
    wait_for(lambda: cold.keys() and old_keys.isdisjoint(hot.keys()), 'original hot key did not retire')
    hot.api(route + '/state', dict(revision=hot.api(route)['revision'], action='pause'))
    cold_key, = cold.keys()
    damaged = bytearray(cold.bytes(cold_key))
    damaged[0] ^= 1
    cold.bytes(cold_key, bytes(damaged))
    scope = dict(storage_id=hot.store['id'], source='chunk', id=str(original['current_encoding_id']))
    hot.finished(hot.execute('quarantine', hot.preview('quarantine', scope)))

    home_marker = hot.prefix + 'meta.json'
    cold_marker = cold.prefix + 'meta.json'
    home_identity = hot.bytes(home_marker)
    cold_identity = cold.bytes(cold_marker)
    hot.bytes(home_marker, cold_identity)
    try:
        rejected(lambda: hot.put('repair', payload))
        current, = db.chunks(hot)
        assert current['current_encoding_id'] == original['current_encoding_id'] and not hot.keys()
    finally:
        hot.bytes(home_marker, home_identity)

    cold.backend.delete_object(Bucket=cold.remote_bucket, Key=cold_marker)
    try:
        assert hot.put('repair', payload)['ETag'] == etag
        restored, = db.chunks(hot)
        assert restored['id'] == original['id'] and restored['current_encoding_id'] != original['current_encoding_id']
        encoding, = db.query("SELECT location_id FROM chunk_encodings WHERE encoding_id=%s AND state='ready'",
                             restored['current_encoding_id'])
        assert location(hot, encoding['location_id']) in hot.keys()
        hot.read('original', payload)
        hot.read('repair', payload)
        assert hot.client.head_object(Bucket=hot.name, Key='original')['ETag'] == etag
        assert cold.bytes(cold_key) == damaged
        recovery_audit(db, original, 's3')
    finally:
        cold.bytes(cold_marker, cold_identity)
    print('PASS cold chunk reupload, home identity check, original references and preserved evidence', flush=True)


if __name__ == '__main__':
    run()
