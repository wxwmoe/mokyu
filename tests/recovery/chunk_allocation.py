"""Chunk claim lifetime and retry, with synchronous storage and a 10s upload idle timeout."""
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from botocore.exceptions import ClientError
from tests.integration import s3, bucket, read, error

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
dsn = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip()
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
prefix = 'chunk-claim-' + uuid.uuid4().hex + '-'
db = psycopg.connect(dsn, autocommit=True)
keys = []


def wait(predicate, message):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.05)
    raise AssertionError(message)


def claimed(key):
    row = db.execute('''SELECT c.id,c.storage_id,c.hash FROM chunks c
        JOIN streams s ON s.id=c.owner_stream JOIN buckets b ON b.id=s.bucket_id
        WHERE b.name=%s AND s.object_key=%s AND c.state IN ('preparing','uploading')''',
        (bucket, key)).fetchone()
    assert row, 'allocation checkpoint has no owned chunk'
    return row


def available(row):
    with psycopg.connect(dsn) as probe:
        return probe.execute('''SELECT pg_try_advisory_xact_lock(
            hashtextextended(%s::uuid::text || ':' || encode(%s::bytea,'hex'), 734922709851001))''',
            (row[1], row[2])).fetchone()[0]


try:
    settings = db.execute("SELECT value FROM runtime_settings WHERE section='requests'").fetchone()[0]
    assert settings['upload_idle_timeout'] == '10s', 'fixture requires a 10s upload idle timeout'
    for point in ('chunk-allocated', 'chunk-uploading', 'chunk-stored'):
        marker = faults / point
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        first, second = prefix + point, prefix + point + '-reuse'
        keys.extend((first, second))
        raw = os.urandom(32768)
        with ThreadPoolExecutor(max_workers=2) as pool:
            upload = pool.submit(s3.put_object, Bucket=bucket, Key=first, Body=raw)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or upload.done(), point + ' not reached')
                assert not upload.done(), point + ' did not block'
                row = claimed(first)
                assert not available(row), point + ' released its database claim early'
                duplicate = pool.submit(s3.put_object, Bucket=bucket, Key=second, Body=raw)
                time.sleep(.15)
                assert not duplicate.done(), 'concurrent reuse bypassed the in-flight writer'
            finally:
                marker.unlink(missing_ok=True)
            upload.result(timeout=20)
            duplicate.result(timeout=20)
        wait(lambda: available(row), point + ' leaked its claim')
        assert read(first)[0] == read(second)[0] == raw
        refs = db.execute('''SELECT e.chunk_id FROM objects o JOIN buckets b ON b.id=o.bucket_id
            JOIN extents e ON e.stream_id=o.stream_id WHERE b.name=%s AND o.object_key=ANY(%s)''',
            (bucket, [first, second])).fetchall()
        assert refs == [(row[0],), (row[0],)], refs
        assert db.execute("SELECT count(*) FROM chunks WHERE storage_id=%s AND hash=%s AND state='ready'",
                          (row[1], row[2])).fetchone() == (1,)
        print('PASS ' + point + ' retains its claim through storage and concurrent deduplication', flush=True)

    marker = faults / 'chunk-allocated'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    key, raw = prefix + 'cancelled', os.urandom(32768)
    keys.append(key)
    with ThreadPoolExecutor(max_workers=1) as pool:
        upload = pool.submit(s3.put_object, Bucket=bucket, Key=key, Body=raw)
        try:
            wait(marker.with_suffix('.hit').exists, 'timeout allocation checkpoint not reached')
            row = claimed(key)
            assert not available(row)
            try:
                upload.result(timeout=20)
            except ClientError as exc:
                assert exc.response['Error']['Code'] == 'RequestTimeout', exc.response
            else:
                raise AssertionError('blocked allocation did not time out')
            wait(lambda: available(row), 'cancelled allocation leaked its claim')
            error('404', s3.head_object, Bucket=bucket, Key=key)
        finally:
            marker.unlink(missing_ok=True)
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    assert read(key)[0] == raw
    ready = db.execute("SELECT id FROM chunks WHERE storage_id=%s AND hash=%s AND state='ready'",
                       (row[1], row[2])).fetchall()
    assert len(ready) == 1 and ready[0][0] != row[0], ready
    assert db.execute('SELECT state FROM chunks WHERE id=%s', (row[0],)).fetchone() == ('failed',)
    print('PASS timeout releases the claim and retry replaces only the abandoned allocation', flush=True)
finally:
    for point in ('chunk-allocated', 'chunk-uploading', 'chunk-stored'):
        (faults / point).unlink(missing_ok=True)
        (faults / point).with_suffix('.hit').unlink(missing_ok=True)
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
    db.close()
