"""CORS-only validation and deployment-wide limits on two isolated data nodes."""
import os
import time
from pathlib import Path
from urllib.parse import urlsplit

import requests

from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture = Fixture()
    endpoints = [os.environ['MOKYU_TEST_ENDPOINT'], os.environ['MOKYU_TEST_PEER_ENDPOINT']]
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, pack_range_enabled=False, public_cache_validation='background',
                   cors_cache_enabled=False, public_cache_refresh_interval='1s', public_cache_max_age='3s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    rules = [dict(origins=['https://old.test'], methods=['GET', 'HEAD'])]
    fixture.cli('bucket', 'cors', fixture.name, '-', value=rules)
    fixture.client.put_object(Bucket=fixture.name, Key='cors', Body=b'shared CORS', ACL='public-read')

    def read(node, origin):
        return requests.head(endpoints[node] + '/' + fixture.name + '/cors', headers={'Origin': origin}, timeout=10)

    def status(node):
        response = requests.get('http://' + urlsplit(endpoints[node]).hostname + ':9002/status', timeout=5)
        response.raise_for_status()
        return response.json()

    for node in range(2):
        wait(lambda: read(node, 'https://old.test').headers.get('Access-Control-Allow-Origin') == 'https://old.test'
             and status(node)['cors_cache']['buckets'] > 0, 'forced CORS cache did not load')
        before = status(node)['cors_cache']['hits']
        read(node, 'https://old.test')
        assert status(node)['cors_cache']['hits'] > before
    fixture.cli('bucket', 'cors', fixture.name, '-', value=[dict(rules[0], origins=['https://new.test'])])
    for node in range(2):
        wait(lambda: 'Access-Control-Allow-Origin' not in read(node, 'https://old.test').headers
             and read(node, 'https://new.test').headers.get('Access-Control-Allow-Origin') == 'https://new.test',
             'peer CORS revision did not converge', seconds=10)
    print('PASS background public reads force CORS caching and both peers confirm changed policies', flush=True)

    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(public_cache_validation='strict', cors_cache_enabled=True)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    time.sleep(3.2)
    for node in range(2):
        read(node, 'https://new.test')
        wait(lambda: status(node)['cors_cache']['pending'] == 0, 'independent CORS fill did not finish')
        before = status(node)
        read(node, 'https://new.test')
        after = status(node)
        assert after['cors_cache']['hits'] > before['cors_cache']['hits']
        assert after['public_read_cache']['hits'] == before['public_read_cache']['hits']
    print('PASS CORS-only cache stays enabled while public authority is checked on each request', flush=True)

    if signals := os.environ.get('MOKYU_TEST_CORS_RESTART_SIGNALS'):
        settings = fixture.cli('settings', 'get', 'requests')
        assert settings['cors_cache_size'] == '1MiB' and settings['cors_cache_entries'] == 256
        settings.update(cors_cache_size='2MiB', cors_cache_entries=512)
        fixture.cli('settings', 'set', 'requests', '-', value=settings)
        assert all(status(node)['cors_cache']['entry_limit'] == 256 for node in range(2))
        signal = Path(signals)
        signal.mkdir(parents=True, exist_ok=True)
        (signal / 'restart-peer').touch()

        def loaded():
            try:
                return status(1)['cors_cache']['entry_limit'] == 512
            except requests.RequestException:
                return False
        wait(loaded, 'peer did not load shared CORS limits after restart', seconds=90)
        assert status(1)['cors_cache']['limit_bytes'] == 2 * 1024 * 1024
        assert status(0)['cors_cache']['entry_limit'] == 256
        print('PASS shared limits load on restart and memory remains independent on each node', flush=True)


if __name__ == '__main__':
    main()
