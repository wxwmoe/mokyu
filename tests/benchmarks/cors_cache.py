"""Bounded CORS sharing benchmark; run only against a disposable started fixture."""
import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture = Fixture()
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    if 'cors_cache_enabled' in profile:
        profile.update(cors_cache_enabled=True, public_cache_refresh_interval='60s', public_cache_max_age='300s')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    rules = [dict(origins=[f'https://site{n}.test'], methods=['GET', 'HEAD'], headers=['range'], expose=['ETag'], max_age=60) for n in range(100)]
    names = []
    for n in range(100):
        name = fixture.name + '-' + str(n)
        fixture.cli('bucket', 'create', name, '--storage', fixture.store['id'])
        fixture.cli('bucket', 'cors', name, '-', value=rules)
        names.append(name)
    endpoint = os.environ['MOKYU_TEST_ENDPOINT']

    def read(n):
        response = requests.head(endpoint + '/' + names[n % len(names)] + '/absent', headers={'Origin': 'https://site99.test'}, timeout=15)
        assert response.status_code == 404 and response.headers['Access-Control-Allow-Origin'] == 'https://site99.test'
        return response.elapsed.total_seconds() * 1000

    for _ in range(3):
        for n in range(100):
            read(n)
    wait(lambda: fixture.cli('status')['cors_cache'].get('pending', 0) == 0, 'CORS fills did not drain')
    before = fixture.cli('status')['cors_cache']
    rounds = []
    for _ in range(5):
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=8) as pool:
            latencies = sorted(pool.map(read, range(1000)))
        rounds.append(dict(requests_per_second=1000 / (time.monotonic() - started),
                           median_ms=statistics.median(latencies), p95_ms=latencies[949]))
    after = fixture.cli('status')['cors_cache']
    print(json.dumps(dict(buckets=100, rules=100, concurrency=8, before=before, after=after, rounds=rounds), indent=2), flush=True)


if __name__ == '__main__':
    main()
