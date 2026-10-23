"""Server-owned I/O templates, read-only previews and explicit profile saves."""
import os
import subprocess
from pathlib import Path

import requests

from tests.storage.source_quarantine import Fixture, wait


def exercise():
    fixture = Fixture()
    other = Fixture()
    base = '/api/v1/storages/' + fixture.name + '/profile'
    original = fixture.cli('storage', 'profile', 'show', fixture.name)
    untouched = other.cli('storage', 'profile', 'show', other.name)
    settings = fixture.api('/api/v1/runtime-settings/io')
    catalog = fixture.api(base + '/presets')
    single = not os.environ.get('MOKYU_TEST_PEER_ENDPOINT')
    local = fixture.backend is None
    assert catalog == fixture.cli('storage', 'profile', 'presets', fixture.name)
    assert [item['name'] for item in catalog['presets']] == ['reliable', 'cached', 'throughput']
    assert catalog['constraints']['single'] == single
    if not single:
        assert catalog['constraints']['write_memory_bytes'] is None
        fixture.api('/api/v1/storage-profile-presets?type=local', expected=400)
    fixture.api('/api/v1/storage-profile-presets?type=invalid', expected=400)
    unauthorized = requests.get(fixture.web + base + '/presets', timeout=15)
    assert unauthorized.status_code in (401, 403), unauthorized.text
    assert fixture.session.post(fixture.web + base + '/preview', json=original, timeout=15).status_code == 403
    print('PASS authenticated templates share the CLI contract and deployment constraints', flush=True)

    for template in catalog['presets']:
        name, values = template['name'], template['values']
        assert ('write_durability' in values) == single
        assert values['upload_cache'] == (not local)
        assert not {'cdc_enabled', 'compression_enabled', 'cache_soft_limit', 'pack_enabled'} & values.keys()
        before = fixture.cli('storage', 'profile', 'show', fixture.name)
        proposed = {**before, **values}
        preview = fixture.api(base + '/preview', proposed)
        assert preview == fixture.cli('storage', 'profile', 'preset', fixture.name, name)
        assert preview == fixture.cli('storage', 'profile', 'preview', fixture.name, '-', value=proposed)
        assert proposed == fixture.cli('storage', 'profile', 'preset', fixture.name, name, '--draft')
        assert preview['selected'] == name
        assert {item['field'] for item in preview['changes']} == {key for key in values if before[key] != values[key]}
        assert fixture.cli('storage', 'profile', 'show', fixture.name) == before
        assert fixture.api('/api/v1/runtime-settings/io') == settings
        assert other.cli('storage', 'profile', 'show', other.name) == untouched
        saved = fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=proposed)
        assert saved['revision'] == before['revision'] + 1
        assert fixture.api(base + '/presets')['selected'] == name
        assert all(saved[key] == value for key, value in values.items())
        assert all(saved[key] == value for key, value in before.items() if key not in values and key != 'revision')
        body = b'Preset changes must reach ordinary reads and writes.\n' * 1000
        fixture.client.put_object(Bucket=fixture.name, Key=name, Body=body, ACL='public-read')
        fixture.read(name, body)
        endpoints = [os.environ['MOKYU_TEST_ENDPOINT']]
        if not single:
            endpoints.append(os.environ['MOKYU_TEST_PEER_ENDPOINT'])
        for endpoint in endpoints:
            def readable():
                response = requests.get(endpoint + '/' + fixture.name + '/' + name,
                    headers={'Accept-Encoding': 'identity'}, timeout=15)
                return response.status_code == 200 and response.content == body
            wait(readable, 'preset write was not readable on a data node', seconds=60)
        fixture.api(base + '/preview', proposed, expected=409)
        stale = fixture.session.put(fixture.web + base, headers=fixture.headers, json=proposed, timeout=15)
        assert stale.status_code == 409, stale.text
    print('PASS all three previews are read-only, saves are explicit and stale revisions conflict', flush=True)

    current = fixture.cli('storage', 'profile', 'show', fixture.name)
    invalid = {**current, 'read_integrity': 'invalid'}
    fixture.api(base + '/preview', invalid, expected=400)
    fixture.api(base + '/preview', {**current, 'storage_id': other.store['id']}, expected=400)
    custom = {**current, 'read_integrity': 'off'}
    assert fixture.api(base + '/preview', custom)['selected'] is None
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=custom)
    assert fixture.api(base + '/presets')['selected'] is None
    restored = fixture.cli('storage', 'profile', 'preset', fixture.name, 'reliable', '--draft')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=restored)
    current = fixture.cli('storage', 'profile', 'show', fixture.name)
    current['request_timeout'] = '31s'
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=current)
    assert fixture.api(base + '/presets')['selected'] == 'reliable'
    assert fixture.api('/api/v1/runtime-settings/io') == settings
    assert other.cli('storage', 'profile', 'show', other.name) == untouched
    print('PASS custom policies, validation, unrelated settings and restoration retain their boundaries', flush=True)


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    if os.environ.get('MOKYU_TEST_PEER_ENDPOINT'):
        exercise()
        return
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    with (faults / 'io-presets.log').open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log, env=dict(os.environ, NO_COLOR='1'))
        try:
            wait(lambda: subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0,
                'service did not start', seconds=60)
            exercise()
        finally:
            process.terminate()
            process.wait(timeout=60)


if __name__ == '__main__':
    main()
