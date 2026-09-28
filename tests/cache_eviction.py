"""Cache eviction failures using the adaptive_cache.py environment.

Requires an isolated gateway with cache.max_entries=1 and at least 1 MiB of
cache space. Run gateway and test as the same unprivileged UID. Temporarily
removes write access from the shard directories of its own cached objects.
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
prefix = 'cache-eviction-' + os.urandom(4).hex() + '/'


def status():
    return json.loads(subprocess.check_output([os.environ['MOKYU_TEST_BINARY'], '--config',
        os.environ['MOKYU_TEST_CONFIG'], 'cli', 'status'], text=True))


def read(key, expected):
    with s3.get_object(Bucket=bucket, Key=key)['Body'] as body:
        assert body.read() == expected


def cache_path(key, extension):
    rows = db.execute('''SELECT c.storage_id FROM objects o
        JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id
        JOIN chunks c ON c.id=e.chunk_id WHERE b.name=%s AND o.key=%s''', (bucket, key)).fetchall()
    assert len(rows) == 1
    name = rows[0][0].hex
    return root / name[:2] / (name + extension)


assert status()['resources']['cache_entries'] == 1
for extension in ('.raw', '.zst'):
    for hot in (False, True):
        key = prefix + extension + ('-main' if hot else '-small')
        old = os.urandom(65536) if extension == '.raw' else os.urandom(16) * 4096
        new = os.urandom(32768)
        s3.put_object(Bucket=bucket, Key=key, Body=old)
        path = cache_path(key, extension)
        cached = path.read_bytes()
        assert status()['local_bytes'][1] == len(cached)
        if hot:
            # The first eviction attempt promotes this candidate into Main.
            read(key, old)
            read(key, old)
        mode = stat.S_IMODE(path.parent.stat().st_mode)
        try:
            path.parent.chmod(0o500)
            try:
                path.unlink()
            except PermissionError:
                pass
            else:
                raise AssertionError('cache deletion must fail with the test permissions')
            s3.put_object(Bucket=bucket, Key=key + '-new', Body=new)
            new_path = cache_path(key + '-new', '.raw')
            assert path.read_bytes() == cached and not new_path.exists()
            for _ in range(3):
                before = status()
                read(key + '-new', new)
                after = status()
                assert after['backend_gets'] == before['backend_gets'] + 1
                assert after['cache_hits'] == before['cache_hits']
                assert after['local_bytes'][1] == len(cached)
                assert path.read_bytes() == cached and not new_path.exists()
        finally:
            path.parent.chmod(mode)
        before = status()['backend_gets']
        read(key + '-new', new)
        assert status()['backend_gets'] == before + 1
        assert not path.exists() and new_path.read_bytes() == new
        assert status()['local_bytes'][1] == len(new)
        read(key + '-new', new)
        assert status()['backend_gets'] == before + 1
        assert len(list(root.glob('*/*'))) == 1
        print('PASS repeated eviction failures and recovery:', extension, 'Main' if hot else 'Small', flush=True)

# A candidate already missing on disk still releases its index entry and quota.
new_path.unlink()
last = os.urandom(16384)
s3.put_object(Bucket=bucket, Key=prefix + 'missing-candidate', Body=last)
assert cache_path(prefix + 'missing-candidate', '.raw').read_bytes() == last
assert status()['local_bytes'][1] == len(last)
before = status()['backend_gets']
read(prefix + 'missing-candidate', last)
assert status()['backend_gets'] == before
assert len(list(root.glob('*/*'))) == 1
db.close()
print('PASS missing-file eviction and final cache accounting', flush=True)
