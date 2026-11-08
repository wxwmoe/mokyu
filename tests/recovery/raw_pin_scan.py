"""A slow pending inventory lookup must not hold the foreground stream lock."""
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, db = Fixture(), Database()
    assert fixture.backend
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value={
        **profile, 'cdc_enabled': False, 'deduplication_enabled': False,
        'write_processing': 'inline', 'upload_cache': False})
    fixture.put('ready', b'already durable')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value={
        **fixture.cli('storage', 'profile', 'show', fixture.name), 'upload_cache': True})
    worker = fixture.arm('raw-upload-before-process')
    scan = None
    source = None
    try:
        fixture.put('first', b'first queued upload')
        wait(lambda: worker.with_suffix('.hit').exists(), 'upload worker did not pause')
        fixture.put('later', b'still pending')
        source = db.query('SELECT e.source_id FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id '
                          'WHERE o.bucket_id=%s AND o.object_key=%s', uuid.UUID(fixture.bucket_id),
                          'later' if db.type == 'postgres' else b'later')[0]['source_id']
        defer = {'sqlite': '4102444800000000', 'postgres': "now()+interval '1 day'", 'mysql': 'UTC_TIMESTAMP(6)+INTERVAL 1 DAY'}
        db.query(f'UPDATE raw_uploads SET next_attempt_at={defer[db.type]} WHERE source_id=%s', source)
        scan = fixture.arm('raw-pin-scan')
        worker.unlink(missing_ok=True)
        wait(lambda: scan.with_suffix('.hit').exists(), 'pending inventory lookup did not start')
        with ThreadPoolExecutor(1) as pool:
            started = time.monotonic()
            read = pool.submit(fixture.read, 'ready', b'already durable')
            try:
                read.result(timeout=2)
                print(f'foreground GET during paused pin inventory: {time.monotonic() - started:.3f}s', flush=True)
            finally:
                scan.unlink(missing_ok=True)
        fixture.read('later', b'still pending')
        assert db.query('SELECT pending_upload FROM raw_sources WHERE id=%s', source)[0]['pending_upload']
    finally:
        worker.unlink(missing_ok=True)
        if scan:
            scan.unlink(missing_ok=True)
        if source is not None:
            now = {'sqlite': '0', 'postgres': 'now()', 'mysql': 'UTC_TIMESTAMP(6)'}
            db.query(f'UPDATE raw_uploads SET next_attempt_at={now[db.type]} WHERE source_id=%s', source)
    wait(lambda: not db.query('SELECT source_id FROM raw_uploads WHERE source_id=%s', source), 'retained upload did not drain')
    fixture.read('later', b'still pending')


if __name__ == '__main__':
    main()
