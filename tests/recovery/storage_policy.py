"""Verify backend policy migration, independent expiry and target disk floors.

Run against a stopped disposable service initialized by the preceding schema.
The fixture owns process startup and injects a crash before listener startup.
"""
import json
import os
import subprocess
import uuid
from pathlib import Path

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    database = Database()
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    marker = faults / 'storage-profiles-initialized'
    process = None
    log = (faults / 'storage-policy.log').open('a')

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start(ready=True):
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        if ready:
            def available():
                assert process.poll() is None, 'service exited; inspect storage-policy.log'
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
            wait(available, 'service did not start', seconds=60)

    try:
        rows = database.query("SELECT value FROM runtime_settings WHERE section='uploads'")
        value = rows[0]['value'] if isinstance(rows[0]['value'], dict) else json.loads(rows[0]['value'])
        value['idle_timeout'] = '7d'
        database.query("UPDATE runtime_settings SET value=%s WHERE section='uploads'", json.dumps(value))
        config_path = Path(os.environ['MOKYU_TEST_CONFIG'])
        text = config_path.read_text()
        text = text.replace('database_connections = 8', 'database_connections = 1')
        text = text.replace('stream_memory_budget = "512MiB"', 'stream_memory_budget = "384MiB"')
        config_path.write_text(text)
        marker.touch()
        start(False)
        wait(lambda: marker.with_suffix('.hit').exists(), 'startup did not materialize profiles')
        stop(True)
        before = database.query('SELECT revision,min_free_space,multipart_idle_timeout FROM storage_profiles ORDER BY storage_id')
        assert before and all(row['min_free_space'] == database.config['instance']['min_free_space'] and row['multipart_idle_timeout'] == '7d' for row in before)
        marker.unlink()
        start()
        assert database.query('SELECT revision,min_free_space,multipart_idle_timeout FROM storage_profiles ORDER BY storage_id') == before
        print('PASS legacy policy materialization and crash/restart idempotence', flush=True)

        first, second = Fixture(), Fixture()
        for fixture, age in [(first, '24h'), (second, '7d')]:
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            assert profile['multipart_idle_timeout'] == '24h' and profile['min_free_space'] == '1GiB'
            profile.update(cdc_enabled=False, pack_range_enabled=False, multipart_idle_timeout=age, min_free_space='64MiB')
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        settings = first.cli('settings', 'get', 'uploads')
        assert 'idle_timeout' not in settings.get('value', settings)
        cleanup = first.arm('local-cleanup')
        uploads = []
        for fixture in (first, second):
            upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='expiry')['UploadId']
            ident = uuid.UUID(upload)
            touched = database.query('SELECT touched_at FROM uploads WHERE id=%s', ident)[0]['touched_at']
            fixture.client.list_multipart_uploads(Bucket=fixture.name)
            fixture.client.list_parts(Bucket=fixture.name, Key='expiry', UploadId=upload)
            assert database.query('SELECT touched_at FROM uploads WHERE id=%s', ident)[0]['touched_at'] == touched
            fixture.client.upload_part(Bucket=fixture.name, Key='expiry', UploadId=upload, PartNumber=1, Body=b'x' * 65536)
            assert database.query('SELECT touched_at FROM uploads WHERE id=%s', ident)[0]['touched_at'] > touched
            expression = "CAST((julianday('now')-2440587.5)*86400000000 AS INTEGER)-172800000000" if database.type == 'sqlite' else "UTC_TIMESTAMP(6)-INTERVAL 2 DAY" if database.type == 'mysql' else "now()-interval '2 days'"
            database.query('UPDATE uploads SET touched_at=' + expression + ' WHERE id=%s', ident)
            uploads.append(ident)
        cleanup.unlink(missing_ok=True)
        wait(lambda: database.query('SELECT state FROM uploads WHERE id=%s', uploads[0])[0]['state'] == 'aborted', 'shorter backend did not expire')
        assert database.query('SELECT state FROM uploads WHERE id=%s', uploads[1])[0]['state'] == 'active'
        print('PASS independent expiry, listing does not renew, committed part renews', flush=True)

        first.put('kept', b'original')
        profile = first.cli('storage', 'profile', 'show', first.name)
        profile['min_free_space'] = '999TiB'
        first.cli('storage', 'profile', 'set', first.name, '-', value=profile)
        rejected(lambda: first.put('kept', b'rejected'))
        first.read('kept', b'original')
        second.put('unaffected', b'other backend')
        second.read('unaffected', b'other backend')
        print('PASS local target floor rejects writes without losing data or affecting another backend', flush=True)

        capacity = Fixture()
        profile_url = '/api/v1/storages/' + capacity.name + '/profile'
        profile = capacity.api(profile_url)
        invalid = dict(profile, compression_enabled=True, compression_level=22)
        response = capacity.session.put(capacity.web + profile_url, headers=capacity.headers, json=invalid, timeout=30)
        assert response.status_code == 400, ('oversized workspace was not rejected as invalid', response.status_code, response.text)
        assert capacity.api(profile_url)['revision'] == profile['revision']
        errors = []
        for level in range(7, 19):
            candidate = dict(profile, compression_enabled=True, compression_level=level)
            response = capacity.session.put(capacity.web + profile_url, headers=capacity.headers, json=candidate, timeout=30)
            if response.status_code != 200:
                errors.append((level, response.status_code, response.text))
                continue
            profile = response.json()
            if capacity.api('/api/v1/system/status')['restart_required']:
                break
        else:
            raise AssertionError(('no larger workspace could be saved within the node budget', errors))
        rejected(lambda: second.read('unaffected', b'other backend'))
        stop()
        start()
        assert not capacity.api('/api/v1/system/status')['restart_required']
        assert capacity.api('/api/v1/system/config')
        second.read('unaffected', b'other backend')
        capacity.put('resized', b'resized workspace' * 4096)
        capacity.read('resized', b'resized workspace' * 4096)
        print('PASS memory growth requires restart, oversized settings preserve the previous revision', flush=True)
    finally:
        marker.unlink(missing_ok=True)
        (faults / 'local-cleanup').unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
