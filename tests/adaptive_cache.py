"""Adaptive cache checks using the state_checks environment and default 20% threshold.

Requires an isolated deployment with enough cache space for three small chunks.
Run gateway and test as the same unprivileged UID so directory permissions apply.
Modifies its own cache files and temporarily removes shard directory write access.
"""
import json
import os
import stat
import subprocess
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
assert os.geteuid() != 0, 'run gateway and test as the same unprivileged UID'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
root = Path(os.environ['MOKYU_TEST_DATA']) / 'chunks'
prefix = 'adaptive-cache-' + os.urandom(4).hex() + '/'


def status():
    return json.loads(subprocess.check_output([os.environ['MOKYU_TEST_BINARY'], '--config',
        os.environ['MOKYU_TEST_CONFIG'], 'cli', 'status'], text=True))


def read(key, expected, **kwargs):
    with s3.get_object(Bucket=bucket, Key=key, **kwargs)['Body'] as body:
        assert body.read() == expected


for name, raw, extension in [
    ('random', os.urandom(200 * 1024), '.raw'),
    ('weak', os.urandom(180 * 1024) + bytes(20 * 1024), '.raw'),
    ('strong', b'compressible-cache\n' * 11000, '.zst'),
]:
    key = prefix + name
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    rows = db.execute('''SELECT c.storage_id,c.raw_size,c.stored_size,c.compressed,c.algorithm
        FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id
        JOIN chunks c ON c.id=e.chunk_id WHERE b.name=%s AND o.key=%s''', (bucket, key)).fetchall()
    assert len(rows) == 1
    storage_id, raw_size, stored_size, compressed, algorithm = rows[0]
    name_hex = storage_id.hex
    path = root / name_hex[:2] / (name_hex + extension)
    payload_size = stored_size - (0 if algorithm == 'none' else 16)
    assert raw_size == len(raw)
    if name == 'weak':
        assert compressed and 80 * raw_size < 100 * payload_size < 100 * raw_size
    original = path.read_bytes()
    if extension == '.zst':
        assert original[:4] == bytes.fromhex('28b52ffd') and len(original) == payload_size
    else:
        assert original == raw
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    before = status()
    read(key, raw)
    read(key, raw[13:73], Range='bytes=13-72')
    after = status()
    assert after['backend_gets'] == before['backend_gets']
    assert after['cache_hit_bytes'] - before['cache_hit_bytes'] == 2 * len(original)

    # Same-length corruption must trigger a verified backend fetch and repair.
    changed = bytearray(original)
    changed[len(changed) // 2] ^= 1
    path.write_bytes(changed)
    before = status()['backend_gets']
    read(key, raw)
    assert status()['backend_gets'] == before + 1
    assert path.read_bytes() == original

    # Failed cleanup must not block verified backend reads or release occupied space.
    path.write_bytes(changed)
    mode = stat.S_IMODE(path.parent.stat().st_mode)
    try:
        path.parent.chmod(0o500)
        try:
            path.unlink()
        except PermissionError:
            pass
        else:
            raise AssertionError('cache deletion must fail with the test permissions')
        before = status()
        read(key, raw)
        read(key, raw[13:73], Range='bytes=13-72')
        after = status()
        assert after['backend_gets'] == before['backend_gets'] + 2
        assert after['cache_hits'] == before['cache_hits']
        assert after['local_bytes'][1] == before['local_bytes'][1]
        assert path.read_bytes() == changed
    finally:
        path.parent.chmod(mode)
    before = status()['backend_gets']
    read(key, raw)
    assert status()['backend_gets'] == before + 1
    assert path.read_bytes() == original
    read(key, raw)
    assert status()['backend_gets'] == before + 1
    print('PASS read-only corrupt cache fallback, accounting and recovery:', name, extension, flush=True)

    # Bounds apply before decompression or serving a raw cache entry.
    for bad in [original[:-1], bytes(4 * 1024 * 1024 + 1)]:
        path.write_bytes(bad)
        before = status()['backend_gets']
        read(key, raw)
        assert status()['backend_gets'] == before + 1
        assert path.read_bytes() == original
    path.unlink()
    before = status()['backend_gets']
    read(key, raw)
    assert status()['backend_gets'] == before + 1
    assert path.read_bytes() == original
    print('PASS adaptive cache:', name, algorithm, extension, flush=True)

assert status()['local_bytes'][1] == sum(p.stat().st_size for p in root.glob('*/*') if p.is_file())
assert not list(root.glob('*/*.tmp'))
db.close()
print('PASS cache formats, upload warming, Range, corruption repair, bounds and accounting', flush=True)
