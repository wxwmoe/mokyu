"""Atomic key replacement, lost responses, activation retries and encrypted reads.

Owns a stopped, initialized isolated service. Reuses source_quarantine's private
backend fixture and source_repair's database adapter; never runs on production.
"""
import json
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'key-replacement.log').open('a')
    database = Database()
    process = None

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect key-replacement.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not become ready', seconds=60)

    try:
        start()
        fixture = Fixture()
        source = fixture.cli('key', 'create', fixture.name + '-key', 'block')
        fixture.cli('storage', 'key', 'set', fixture.name, source['name'])
        local_root = Path(os.environ['MOKYU_TEST_FSYNC_ROOT']) / (fixture.name + '-local')
        local = fixture.cli('storage', 'create', fixture.name + '-local', 'local', '--init', '--path', str(local_root))
        fixture.cli('storage', 'key', 'set', local['name'], source['name'])
        # The disabled-store path is additionally covered by the shared four-database contract.
        body = os.urandom(8192)
        fixture.put('old', body)
        fixture.read('old', body)
        packed = None
        if fixture.backend:
            packed = os.urandom(12 * 1024 * 1024)
            fixture.put('packed', packed)
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(pack_enabled=True, pack_max_size='16MiB')
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
            fixture.finished(fixture.cli('pack', 'run'))
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile['pack_enabled'] = False
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
            fixture.read('packed', packed)
        source_id = uuid.UUID(source['id'])

        def preview(key=None, **values):
            return fixture.api('/api/v1/keys/' + (key or source['id']) + '/replace-preview', values)

        def apply(plan, expected=200):
            return fixture.api('/api/v1/key-replacements/' + plan['id'], {}, expected)

        def binding(storage):
            return fixture.cli('storage', 'show', storage['name'])['block_key_id']

        endpoint = fixture.web + '/api/v1/keys/' + source['id'] + '/replace-preview'
        assert requests.post(endpoint, json={}, timeout=10).status_code == 403
        assert fixture.session.post(endpoint, headers={'Origin': fixture.web}, json={}, timeout=10).status_code == 403
        exhausted = fixture.cli('key', 'create', fixture.name + '-exhausted', 'block')
        database.query('UPDATE nonce_counters SET reserved_id=%s WHERE block_key_id=%s AND type_tag=0', (1 << 63) - 64, uuid.UUID(exhausted['id']))
        fixture.api('/api/v1/keys/' + source['id'] + '/replace-preview', {'target': exhausted['id']}, expected=400)
        stale = preview()
        fixture.cli('storage', 'key', 'set', fixture.name, source['name'])
        apply(stale, expected=409)
        assert binding(fixture.store) == source['id']

        for boundary, committed in [('key-replacement-before-commit', False), ('key-replacement-after-commit', True)]:
            plan = preview()
            assert len(plan['storages']) == 2 and plan['create']
            marker = fixture.arm(boundary)
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(apply, plan)
                wait(lambda: marker.with_suffix('.hit').exists() or pending.done(), 'replacement did not reach ' + boundary)
                assert marker.with_suffix('.hit').exists(), pending.exception()
                rows = database.query('SELECT result FROM key_replacements WHERE id=%s', uuid.UUID(plan['id']))
                assert bool(rows[0]['result']) == committed
                stop(crash=True)
                marker.unlink(missing_ok=True)
                try:
                    pending.result(timeout=30)
                    raise AssertionError('lost response unexpectedly succeeded')
                except requests.RequestException:
                    pass
            start()
            status = fixture.api('/api/v1/key-replacements/' + plan['id'])
            assert bool(status['result']) == committed
            result = apply(plan)
            assert not result['pending']
            assert apply(plan)['result'] == result['result']
            assert binding(fixture.store) == binding(local) == plan['target']
            assert len(database.query('SELECT id FROM managed_keys WHERE id=%s', uuid.UUID(plan['target']))) == 1
            fixture.read('old', body)
            if packed:
                fixture.read('packed', packed)
            fixture.put('old-duplicate-' + boundary, body)
            assert not database.query('SELECT encoding_id FROM chunk_encodings WHERE block_key_id=%s', uuid.UUID(plan['target'])), 'deduplication must keep the original cipher'
            current = os.urandom(16384)
            fixture.put(boundary, current)
            fixture.read(boundary, current)
            assert database.query('SELECT encoding_id FROM chunk_encodings WHERE block_key_id=%s', uuid.UUID(plan['target']))
            source['id'] = plan['target']
            source['name'] = plan['target_name']

        plan = preview(key=source['id'])
        saved_root = local_root.with_name(local_root.name + '-saved')
        local_root.rename(saved_root)
        local_root.write_bytes(b'not a directory')
        try:
            result = apply(plan)
            assert local['id'] in result['pending'], result
            assert result['result']['preview']['target'] == plan['target']
        finally:
            local_root.unlink()
            saved_root.rename(local_root)
        assert not apply(plan)['pending']
        fixture.read('old', body)
        fixture.cli('key', 'revoke', str(source_id), succeeds=False)
        status = fixture.cli('key', 'replacement', plan['id'], succeeds=False)
        assert status is None, 'a CLI actor cannot reuse a browser-owned preview'
        cli_preview = fixture.cli('key', 'replace-preview', plan['target'])
        cli_result = fixture.cli('key', 'replace', cli_preview['id'])
        assert cli_result['result'] == fixture.cli('key', 'replace', cli_preview['id'])['result']
        assert fixture.cli('key', 'replacement', cli_preview['id'])['result'] == cli_result['result']
        fixture.read('old', body)
        stop(crash=True)
        start()
        fixture.read('old', body)
        print('key replacement: scope conflicts, exhausted targets, atomic crash recovery, lost responses, dedup, cipher reads, activation retry and CLI ownership passed', flush=True)
    finally:
        stop()
        log.close()


if __name__ == '__main__':
    main()
