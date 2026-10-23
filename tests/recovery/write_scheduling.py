"""Per-storage write delay, admission backpressure and durable flush/restart."""
import datetime
import os
import subprocess
import time
import uuid
from pathlib import Path

from botocore.exceptions import ClientError
from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    assert os.environ.get('MOKYU_TEST_BACKEND'), 'requires an isolated S3 backend'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'write-scheduling.log').open('a')
    process = None
    markers = []

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)
        def ready():
            assert process.poll() is None, 'service exited; inspect write-scheduling.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'scheduling service did not start', seconds=60)

    def stop(crash=True):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def configure(fixture, **changes):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(changes)
        return fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def arm(fixture, name):
        marker = fixture.arm(name)
        markers.append(marker)
        return marker

    def raw(fixture, key):
        return database.query("""SELECT r.pending_upload,r.durability FROM objects o
            JOIN raw_ranges e ON e.stream_id=o.stream_id JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s""", uuid.UUID(fixture.bucket_id),
            key if database.type == 'postgres' else key.encode())[0]

    def pending_chunks(fixture):
        return database.query('SELECT COUNT(*) AS count FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE c.storage_id=%s', uuid.UUID(fixture.store['id']))[0]['count']

    def rejected(call):
        try:
            call()
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 503, error.response
            assert error.response['Error']['Code'] == 'SlowDown', error.response
        else:
            raise AssertionError('overdue backlog accepted a rejected write')

    try:
        start()
        remote, database = Fixture(), Database()
        for changes in [dict(write_flush_interval='61s'), dict(write_flush_interval='bad'),
                        dict(write_backlog_max_age='0s'), dict(write_backlog_max_age='25h'),
                        dict(background_scheduling='unknown')]:
            profile = remote.cli('storage', 'profile', 'show', remote.name)
            profile.update(changes)
            remote.cli('storage', 'profile', 'set', remote.name, '-', value=profile, succeeds=False)
            invalid = remote.session.put(remote.web + '/api/v1/storages/' + remote.name + '/profile', headers=remote.headers, json=profile, timeout=15)
            assert invalid.status_code == 400 and invalid.headers.get('x-request-id'), invalid.text
        configure(remote, cdc_enabled=False, pack_range_enabled=False, upload_cache=True, cache_enabled=True,
                  write_flush_interval='60s', write_backlog_max_age='5m', background_scheduling='foreground')
        profile = remote.cli('storage', 'profile', 'show', remote.name)
        assert profile['write_flush_interval'] == '60s' and profile['write_backlog_max_age'] == '300s'
        print('PASS scheduling validation and duration roundtrip', flush=True)

        data = os.urandom(128 * 1024)
        remote.put('delayed', data)
        time.sleep(1)
        assert raw(remote, 'delayed')['pending_upload']
        remote.read('delayed', data)
        remote.cli('cache', 'flush', '--wait', '--timeout', '20s')
        assert not raw(remote, 'delayed')['pending_upload']
        print('PASS raw coalescing retains immediate reads and manual flush bypasses delay', flush=True)

        configure(remote, write_flush_interval='0s', write_backlog_max_age='1s', upload_cache_overflow='reject')
        uploader = arm(remote, 'raw-upload-before-process')
        remote.put('overdue', data)
        wait(uploader.with_suffix('.hit').exists, 'raw uploader did not pause')
        wait(lambda: remote.cli('cache', 'status')['write_backlog']['oldest'], 'backlog was not sampled')
        time.sleep(2)
        rejected(lambda: remote.put('blocked', data))
        other = Fixture()
        configure(other, cdc_enabled=False, pack_range_enabled=False, upload_cache=True, cache_enabled=True)
        other.put('independent', data)
        other.read('independent', data)
        configure(remote, upload_cache_overflow='fallback')
        remote.put('synchronous', data)
        assert not raw(remote, 'synchronous')['pending_upload']
        remote.read('synchronous', data)
        uploader.unlink(missing_ok=True)
        remote.cli('cache', 'flush', '--wait', '--timeout', '20s')
        print('PASS overdue reject/fallback and independent storage admission', flush=True)

        pressure = Fixture()
        configure(pressure, cdc_enabled=False, pack_range_enabled=False, upload_cache=True, cache_enabled=True, write_flush_interval='60s', write_backlog_max_age='5m', upload_cache_overflow='reject')
        pressure.put('pressure', data)
        assert raw(pressure, 'pressure')['pending_upload']
        configure(pressure, upload_cache_size='1B')
        rejected(lambda: pressure.put('quota', data))
        wait(lambda: not raw(pressure, 'pressure')['pending_upload'], 'pressure did not bypass coalescing', seconds=15)
        configure(pressure, upload_cache_size=None)
        print('PASS quota pressure expedites existing writes without discarding them', flush=True)

        chunked = Fixture()
        configure(chunked, upload_cache=True, cache_enabled=True, write_flush_interval='60s',
                  write_backlog_max_age='5m', background_scheduling='foreground')
        chunked.put('delayed-chunks', data)
        time.sleep(1)
        assert pending_chunks(chunked) > 0
        chunked.read('delayed-chunks', data)
        chunked.cli('cache', 'flush', '--wait', '--timeout', '20s')
        assert pending_chunks(chunked) == 0
        print('PASS chunk coalescing and explicit durable drain', flush=True)

        backend = os.environ.pop('MOKYU_TEST_BACKEND')
        try:
            local = Fixture()
        finally:
            os.environ['MOKYU_TEST_BACKEND'] = backend
        configure(local, cdc_enabled=False, pack_range_enabled=False, write_durability='buffered', write_flush_interval='4s')
        local.put('buffered', data)
        assert raw(local, 'buffered')['durability'] == 'buffered'
        local.read('buffered', data)
        wait(lambda: raw(local, 'buffered')['durability'] == 'durable', 'local sync did not run after delay', seconds=15)
        skewed = Fixture()
        configure(skewed, cdc_enabled=False, pack_range_enabled=False, upload_cache=True, cache_enabled=True)
        backend = os.environ.pop('MOKYU_TEST_BACKEND')
        try:
            local_chunks = Fixture()
        finally:
            os.environ['MOKYU_TEST_BACKEND'] = backend
        configure(local_chunks, write_durability='buffered', write_flush_interval='60s',
                  pack_enabled=False, compression_enabled=False)
        for fixture in (skewed, chunked, local):
            configure(fixture, write_flush_interval='60s', write_backlog_max_age='5m')
        for fixture in (skewed, chunked, local, local_chunks):
            fixture.put('clock-skew', os.urandom(128 * 1024))
        assert raw(skewed, 'clock-skew')['pending_upload'] and pending_chunks(chunked)
        assert raw(local, 'clock-skew')['durability'] == 'buffered'
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        future = int(future.timestamp() * 1000000) if database.type == 'sqlite' else future
        for fixture in (skewed, local):
            database.query('UPDATE raw_sources SET created_at=%s WHERE storage_id=%s',
                           future, uuid.UUID(fixture.store['id']))
        database.query('UPDATE pending_uploads SET created_at=%s WHERE chunk_id IN '
                       '(SELECT id FROM chunks WHERE storage_id=%s)', future, uuid.UUID(chunked.store['id']))
        database.query('UPDATE chunk_encodings SET created_at=%s WHERE storage_id=%s',
                       future, uuid.UUID(local_chunks.store['id']))
        assert database.query('SELECT COUNT(*) AS count FROM chunk_encodings WHERE storage_id=%s AND sync_pending',
                              uuid.UUID(local_chunks.store['id']))[0]['count'] > 0
        for fixture in (skewed, chunked, local, local_chunks):
            configure(fixture, write_flush_interval='0s')
        wait(lambda: not raw(skewed, 'clock-skew')['pending_upload'], 'zero delay waited for a future raw timestamp', seconds=20)
        wait(lambda: pending_chunks(chunked) == 0, 'zero delay waited for a future chunk timestamp', seconds=20)
        wait(lambda: raw(local, 'clock-skew')['durability'] == 'durable', 'zero delay postponed local raw sync', seconds=20)
        wait(lambda: database.query('SELECT COUNT(*) AS count FROM chunk_encodings WHERE storage_id=%s AND sync_pending',
                                    uuid.UUID(local_chunks.store['id']))[0]['count'] == 0,
             'zero delay postponed local chunk sync', seconds=20)
        print('PASS zero delay drains raw/chunks and local sync after creation clock skew', flush=True)
        configure(local, write_flush_interval='60s')
        local.put('restart', data)
        assert raw(local, 'restart')['durability'] == 'buffered'
        configure(other, write_flush_interval='60s')
        other.put('restart', data)
        assert raw(other, 'restart')['pending_upload']
        stop()
        start()
        assert raw(local, 'restart')['durability'] == 'durable'
        local.read('restart', data)
        other.read('restart', data)
        stop(crash=False)
        assert not raw(other, 'restart')['pending_upload']
        print('PASS local sync delay, startup recovery and graceful flush bypass', flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
