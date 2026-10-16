"""Profile reloads retain in-flight storage concurrency without database permits."""
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from tests.integration import s3, bucket, read

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
assert db.execute('SELECT deployment_mode FROM mokyu_meta WHERE singleton').fetchone()[0] == 'single'
db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = os.environ['MOKYU_TEST_STORAGE']
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'storage-put-permitted'
document = Path(os.environ['MOKYU_TEST_DATA']) / 'permit-profile.json'
prefix = 'permit-' + uuid.uuid4().hex
keys = [prefix + '-first', prefix + '-second']


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True, timeout=20))


def wait(predicate):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if value := predicate():
            return value
        time.sleep(.02)
    raise AssertionError('storage gate condition timed out')


def calls():
    return db.execute("SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                      "WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                      "AND query LIKE '%%storage_permits%%' AND query NOT LIKE '%%pg_stat_statements%%'").fetchone()[0]


def active():
    return cli('status')['io']['backend']['put']['active']


original = cli('storage', 'profile', 'show', name)


def configure(limit):
    current = cli('storage', 'profile', 'show', name)
    document.write_text(json.dumps({**current, 'upload_concurrency': limit,
                                    'read_concurrency': 1, 'control_concurrency': 1,
                                    'upload_cache': False, 'pack_enabled': False}))
    cli('storage', 'profile', 'set', name, str(document))


before = calls()
payloads = [os.urandom(32768), os.urandom(32768)]
try:
    configure(1)
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(s3.put_object, Bucket=bucket, Key=keys[0], Body=payloads[0])
        try:
            wait(marker.with_suffix('.hit').exists)
            assert active() == 1
            configure(1)
            second = pool.submit(s3.put_object, Bucket=bucket, Key=keys[1], Body=payloads[1])
            wait(lambda: db.execute("SELECT count(*) FROM chunks c JOIN streams s ON s.id=c.owner_stream "
                                    "WHERE s.object_key=ANY(%s) AND c.state='uploading'", (keys,)).fetchone()[0] == 2)
            time.sleep(.1)
            assert active() == 1, 'profile reload multiplied the storage upload limit'
            configure(2)
            wait(lambda: active() == 2)
            configure(1)
            assert active() == 2, 'reducing the limit canceled active I/O'
        finally:
            marker.unlink(missing_ok=True)
        first.result(timeout=20)
        second.result(timeout=20)
    for key, payload in zip(keys, payloads):
        assert read(key)[0] == payload
        assert s3.head_object(Bucket=bucket, Key=key)['ContentLength'] == len(payload)
        s3.delete_object(Bucket=bucket, Key=key)
    assert active() == 0
    assert calls() == before, 'single runtime queried storage permits'
    assert db.execute('SELECT count(*) FROM storage_permits').fetchone()[0] == 0
    print('PASS profile reload retains old I/O counts; raising admits waiters and lowering drains; no single permit SQL', flush=True)
finally:
    marker.unlink(missing_ok=True)
    current = cli('storage', 'profile', 'show', name)
    document.write_text(json.dumps({**original, 'revision': current['revision']}))
    cli('storage', 'profile', 'set', name, str(document))
    document.unlink(missing_ok=True)
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
    db.close()
