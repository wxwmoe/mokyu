"""Exercise a data node's S3 upload against an isolated control with fault injection."""
import os
from pathlib import Path

from tests.recovery.source_repair import (
    Database, blocked, isolate, location, original_unchanged, recovery_audit,
)
from tests.storage.source_quarantine import Fixture, rejected


def run():
    db = Database()
    home = Fixture()
    key = home.cli('key', 'create', home.name, 'block', '-a', 'aes-256-gcm')
    home.cli('storage', 'key', 'set', home.name, key['id'])
    payload = os.urandom(32 << 10)
    original, old_key, encoded = isolate(home, db, payload)
    etag = home.client.head_object(Bucket=home.name, Key='original')['ETag']

    marker = home.prefix + 'meta.json'
    identity = home.bytes(marker)
    before = set(home.keys())
    control_faults = home.faults
    home.faults = Path(os.environ['MOKYU_TEST_DATA_FAULT_DIR'])
    try:
        failed = blocked(home, 'node-write-seal', lambda: home.put('retry', payload),
                         lambda: home.bytes(marker, b'{}'))
        rejected(failed.result)
        assert set(home.keys()) == before
        original_unchanged(home, db, original)
    finally:
        home.faults = control_faults
        home.bytes(marker, identity)

    def tamper():
        candidate = db.candidates(home)[-1]
        path = location(home, candidate['location_id'])
        stored = home.bytes(path)
        home.bytes(path, bytes([stored[0] ^ 1]) + stored[1:])

    failed = blocked(home, 'repair-stored', lambda: home.put('retry', payload), tamper)
    rejected(failed.result)
    original_unchanged(home, db, original)

    def revoke():
        home.cli('cred', 'disable', home.credentials['access_key'])
    try:
        failed = blocked(home, 'repair-before-commit', lambda: home.put('retry', payload), revoke)
        rejected(failed.result)
        original_unchanged(home, db, original)
    finally:
        home.cli('cred', 'enable', home.credentials['access_key'])

    assert home.put('retry', payload)['ETag'] == etag
    home.read('original', payload)
    home.read('retry', payload)
    assert home.bytes(old_key) != encoded
    recovery_audit(db, original, 's3')
    print('PASS data node namespace check, damaged candidate, revoked writer and verified retry', flush=True)


if __name__ == '__main__':
    run()
