"""Cancellation, late publication and retained bytes with a 4 MiB response cache."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.s3.response_cache import (
    bucket, endpoint, gzip, os, prefix, put, read, requests, status, text, time, warm,
)

assert status()['response_cache']['limit_bytes'] == 4 * 1024 * 1024
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])

def wait_until(check):
    for _ in range(200):
        if check():
            return
        time.sleep(.02)
    raise AssertionError('boundary did not complete')

def arm(name):
    fault = faults / name
    fault.with_suffix('.hit').unlink(missing_ok=True)
    fault.touch()
    return fault

put('canceled', text)
warm('canceled', text)
read('canceled', 'gzip')
before = status()['response_cache']['entries']
fault = arm('http-compression-finish')
try:
    response = requests.get(endpoint + '/' + bucket + '/' + prefix + 'canceled', headers={'Accept-Encoding': 'gzip'}, stream=True, timeout=10)
    assert response.raw.read(32, decode_content=False)
    wait_until(lambda: fault.with_suffix('.hit').exists())
    assert status()['response_cache']['building'] == 1
    response.close()
    wait_until(lambda: status()['response_cache']['building'] == 0)
    assert status()['response_cache']['entries'] == before
    assert status()['response_cache']['candidate_bytes'] == 0
finally:
    fault.unlink(missing_ok=True)
warm('canceled', text, 'gzip')

put('replaced', text)
warm('replaced', text)
read('replaced', 'gzip')
fault = arm('http-compression-finish')
with ThreadPoolExecutor(max_workers=1) as pool:
    pending = pool.submit(read, 'replaced', 'gzip')
    try:
        wait_until(lambda: fault.with_suffix('.hit').exists())
        put('replaced', text[::-1])
    finally:
        fault.unlink(missing_ok=True)
    assert gzip.decompress(pending.result()[1]) == text
assert gzip.decompress(read('replaced', 'gzip')[1]) == text[::-1]

payload = os.urandom(200 * 1024)
put('retained', payload)
warm('retained', payload)
fault = arm('response-cache-body')
with ThreadPoolExecutor(max_workers=1) as pool:
    pending = pool.submit(read, 'retained')
    try:
        wait_until(lambda: fault.with_suffix('.hit').exists())
        assert status()['active_streams'] == 0
        # Recently used bodies can survive one eviction pass; wait for actual retirement.
        for number in range(40):
            data = bytes([number]) + payload[1:]
            name = 'pressure-' + str(number)
            put(name, data)
            # First request records reuse; the second builds without entering the paused hit path.
            assert read(name)[1] == data
            assert read(name)[1] == data
            cache = status()['response_cache']
            assert cache['live_bytes'] + cache['metadata_bytes'] <= cache['limit_bytes']
            assert cache['candidate_bytes'] == 0
            if cache['retired_bytes'] >= len(payload):
                break
        assert cache['retired_bytes'] >= len(payload), cache
        assert not pending.done()
    finally:
        fault.unlink(missing_ok=True)
    assert pending.result()[1] == payload
wait_until(lambda: status()['response_cache']['retired_bytes'] == 0)

put('oversized', payload * 2)
before = status()['response_cache']['entries']
warm('oversized', payload * 2)
assert status()['response_cache']['entries'] == before
assert status()['response_cache']['building'] == 0
print('PASS canceled and late builders, retained evicted bodies, bounded live memory, oversized fallback and reliable writes under pressure')
