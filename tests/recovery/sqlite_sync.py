"""Saved SQLite synchronization, observed runtime state and process recovery."""
import os
import subprocess
import tomllib
from pathlib import Path

import requests
from botocore.exceptions import ClientError

from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    os.environ.pop('MOKYU_TEST_BACKEND', None)
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'sqlite-sync.log').open('a')
    process = None

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)

        def ready():
            assert process.poll() is None, 'SQLite service exited; inspect sqlite-sync.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'SQLite service did not start', seconds=60)

    try:
        start()
        fixture = Fixture()
        maintenance = fixture.api('/api/v1/runtime-settings/maintenance')
        response = fixture.session.put(fixture.web + '/api/v1/runtime-settings/maintenance', headers=fixture.headers,
            json={'value': maintenance['value'], 'revision': maintenance['revision'] + 99}, timeout=30)
        assert response.status_code == 409, response.text
        database = tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())['database']
        if database.get('type', 'postgres') != 'sqlite':
            assert fixture.api('/api/v1/runtime-settings/io')['value'] == {'shutdown_flush_timeout': '30s'}
            response = fixture.session.put(fixture.web + '/api/v1/runtime-settings/io', headers=fixture.headers,
                json={'value': {'sqlite_synchronous': 'off'}, 'revision': 1}, timeout=30)
            assert response.status_code == 400, response.text
            assert not any(row['key'] in ('database.synchronous', 'database.journal_mode')
                           for row in fixture.api('/api/v1/system/config'))
            print('PASS non-SQLite deployments omit and reject SQLite synchronization', flush=True)
            return
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        url = '/api/v1/runtime-settings/io'

        def observed():
            fields = {row['key']: row for row in fixture.api('/api/v1/system/config')}
            assert fields['database.journal_mode']['value'] == 'wal'
            assert fields['database.synchronous']['origin'] == 'environment'
            return fields['database.synchronous']['value']

        assert observed() == 'full'
        assert fixture.cli('settings', 'get', 'io') == {'sqlite_synchronous': 'full', 'shutdown_flush_timeout': '30s'}
        current = fixture.api(url)
        for value in ({'sqlite_synchronous': 'FULL'}, {'sqlite_synchronous': 'extra'}, {'journal_mode': 'off'}):
            response = fixture.session.put(fixture.web + url, headers=fixture.headers,
                                           json={'value': value, 'revision': current['revision']}, timeout=30)
            assert response.status_code == 400, response.text
        response = requests.put(fixture.web + url, headers={'Origin': fixture.web},
                                json={'value': {'sqlite_synchronous': 'off'}, 'revision': current['revision']}, timeout=30)
        assert response.status_code in (401, 403)
        response = fixture.session.put(fixture.web + url, headers=fixture.headers,
            json={'value': {'sqlite_synchronous': 'normal'}, 'revision': current['revision'] + 99}, timeout=30)
        assert response.status_code == 409, response.text
        assert fixture.api(url)['revision'] == current['revision']
        print('PASS default FULL, validation, administrator protection and revision conflicts', flush=True)

        data = os.urandom(65536)
        fixture.put('preserved', data)
        previous = 'full'
        for mode in ['normal', 'off', 'full']:
            fixture.cli('settings', 'set', 'io', '-', value={'sqlite_synchronous': mode})
            assert fixture.api(url)['restart_required']
            assert observed() == previous
            fixture.read('preserved', data)
            stop()
            start()
            current = fixture.api(url)
            assert not current['restart_required'] and current['revision'] == current['applied_revision']
            assert observed() == mode
            fixture.put('saved-' + mode, data + mode.encode())
            stop(crash=True)
            start()
            assert observed() == mode
            fixture.read('saved-' + mode, data + mode.encode())
            fixture.read('preserved', data)
            previous = mode
        assert fixture.cli('storage', 'profile', 'show', fixture.name)['revision'] == profile['revision'] + 1
        assert fixture.cli('settings', 'reset', 'io') == {'sqlite_synchronous': 'full', 'shutdown_flush_timeout': '30s'}
        print('PASS restart applies each level to runtime state and SIGKILL preserves the committed test data', flush=True)
        print('PASS database settings do not mutate storage profiles; process tests do not simulate power loss', flush=True)

        key = fixture.cli('key', 'create', fixture.name + '-key', 'block')
        fixture.cli('settings', 'set', 'io', '-', value={'sqlite_synchronous': 'off'})
        stop()
        start()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=True)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        fixture.cli('storage', 'key', 'set', fixture.name, key['name'])

        def rejected_encryption():
            try:
                fixture.put('off-encrypted', os.urandom(65536))
            except ClientError as error:
                assert error.response['Error']['Code'] == 'InvalidRequest', error.response
                return
            raise AssertionError('SQLite OFF accepted a new encryption nonce')

        rejected_encryption()
        fixture.cli('settings', 'set', 'io', '-', value={'sqlite_synchronous': 'normal'})
        rejected_encryption()
        stop()
        start()
        fixture.put('normal-encrypted', data)
        fixture.read('normal-encrypted', data)
        current = fixture.api(url)
        response = fixture.session.put(fixture.web + url, headers=fixture.headers,
            json={'value': {'sqlite_synchronous': 'off'}, 'revision': current['revision']}, timeout=30)
        assert response.status_code == 400 and response.json()['code'] == 'InvalidArgument', response.text
        assert fixture.api(url)['revision'] == current['revision']
        stop(crash=True)
        start()
        fixture.read('normal-encrypted', data)
        fixture.put('normal-next', os.urandom(65536))
        print('PASS OFF rejects encrypted writes until NORMAL restart and used nonce counters prevent OFF', flush=True)
    finally:
        stop()
        log.close()


if __name__ == '__main__':
    main()
