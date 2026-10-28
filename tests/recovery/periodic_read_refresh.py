"""Batch resident periodic refreshes without blocking hits or restoring stale access."""
import os
import time
import uuid
from pathlib import Path

import requests

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    endpoint = os.environ['MOKYU_TEST_PEER_ENDPOINT']
    status_url = os.environ['MOKYU_TEST_PEER_STATUS']
    faults = Path(os.environ['MOKYU_TEST_PEER_FAULT_DIR'])
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, deduplication_enabled=False, cache_enabled=True,
                   pack_enabled=False, pack_range_enabled=False, compression_enabled=False,
                   public_cache_validation='background', public_cache_refresh_interval='5s',
                   public_cache_max_age='30s', read_source_validation='background',
                   source_validation_max_age='60s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    body = b'periodic metadata refresh keeps the cached body\n' * 200

    def status():
        response = requests.get(status_url, timeout=5)
        response.raise_for_status()
        return response.json()

    peer = uuid.UUID(status()['node_id'])

    def settled():
        rows = database.query('SELECT count(*) AS pending FROM notification_targets WHERE node_id=%s AND applied_at IS NULL AND retired_at IS NULL', peer)
        return rows[0]['pending'] == 0

    def read(key):
        return requests.get(endpoint + '/' + fixture.name + '/' + key,
                            headers={'Accept-Encoding': 'identity'}, timeout=15)

    def arm(name):
        marker = faults / name
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        return marker

    def release(marker):
        marker.unlink(missing_ok=True)
        marker.with_suffix('.hit').unlink(missing_ok=True)

    for count in (32, 65):
        keys = [f'periodic-{count}-{n}' for n in range(count)]
        for key in keys:
            fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body, ACL='public-read')
        wait(settled, 'creation notifications did not settle')
        for key in keys:
            for _ in range(2):
                assert read(key).content == body
        wait(lambda: status()['public_read_cache']['pending_checks'] == 0, 'fills did not settle')
        time.sleep(5.1)
        before = status()
        marker = arm('node-public-refresh')
        try:
            assert read(keys[0]).content == body
            wait(marker.with_suffix('.hit').exists, 'periodic refresh did not pause', seconds=2)
            for key in keys[1:]:
                assert read(key).content == body
            assert status()['public_read_cache']['pending_checks'] == count
            release(marker)
            wait(lambda: status()['public_read_cache']['pending_checks'] == 0, 'batch did not finish')
            after = status()
            assert after['public_read_cache']['refreshed'] - before['public_read_cache']['refreshed'] == count
            assert after['rpc']['unary']['started'] - before['rpc']['unary']['started'] == (count + 31) // 32
            assert after['io']['backend']['read']['started'] == before['io']['backend']['read']['started']
            assert all(read(key).content == body for key in keys)
        finally:
            release(marker)
        print(f'PASS {count} periodic refreshes use {(count + 31) // 32} RPCs without source reads', flush=True)

    key = keys[-1]
    time.sleep(5.1)
    marker = arm('node-public-refreshed')
    try:
        assert read(key).content == body
        wait(marker.with_suffix('.hit').exists, 'periodic authority reply did not pause', seconds=2)
        fixture.client.put_object_acl(Bucket=fixture.name, Key=key, ACL='private')
        wait(settled, 'ACL notification was not applied', seconds=2)
        release(marker)
        wait(lambda: status()['public_read_cache']['pending_checks'] == 0, 'stale refresh did not finish')
        assert read(key).status_code == 403
        assert read(keys[0]).content == body
    finally:
        release(marker)
    print('PASS late periodic replies cannot restore revoked public access', flush=True)


if __name__ == '__main__':
    main()
