"""Local/cross-node chunk exclusion, cancellation and retry with synchronous storage."""
import os
import json
import subprocess
import time
import tomllib
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import boto3
from botocore.exceptions import ClientError
from tests.integration import s3, bucket, read, error, config as client_config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
dsn = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip()
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
prefix = 'chunk-claim-' + uuid.uuid4().hex + '-'
db = psycopg.connect(dsn, autocommit=True)
single = db.execute('SELECT deployment_mode FROM mokyu_meta WHERE singleton').fetchone()[0] == 'single'
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
                assert available(row), point + ' retained a database claim during storage I/O'
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
        print('PASS ' + point + ' retains allocation ownership without a database transaction', flush=True)

    if single:
        config = tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())
        assert config['instance']['database_connections'] == 2, 'fixture requires a two-connection pool'
        marker = faults / 'chunk-allocated'
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        batch = [prefix + 'bounded-pool-' + str(n) for n in range(2)]
        keys.extend(batch)
        payloads = [os.urandom(32768) for _ in batch]
        with ThreadPoolExecutor(max_workers=2) as pool:
            uploads = [pool.submit(s3.put_object, Bucket=bucket, Key=key, Body=raw)
                       for key, raw in zip(batch, payloads)]
            try:
                wait(lambda: db.execute("SELECT count(*) FROM chunks c JOIN streams s ON s.id=c.owner_stream "
                                        "JOIN buckets b ON b.id=s.bucket_id WHERE b.name=%s AND s.object_key=ANY(%s) "
                                        "AND c.state='preparing'", (bucket, batch)).fetchone()[0] == 2,
                     'independent allocations exhausted the two-connection fixture pool')
                assert not any(upload.done() for upload in uploads)
                error('404', s3.head_object, Bucket=bucket, Key=prefix + 'not-found')
            finally:
                marker.unlink(missing_ok=True)
            for upload in uploads:
                upload.result(timeout=20)
        for key, raw in zip(batch, payloads):
            assert read(key)[0] == raw
        print('PASS two suspended allocations leave the two-connection database pool available', flush=True)

        def cli(*args):
            return json.loads(subprocess.check_output(
                [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli', *args],
                text=True, timeout=30))
        name = 'claim-' + uuid.uuid4().hex[:12]
        storage = cli('storage', 'create', name, 'local', '--init', '--key', 'blocks',
                      '--path', str(Path(os.environ['MOKYU_TEST_DATA']) / name))
        cli('bucket', 'create', name, '--storage', storage['id'])
        credential = cli('cred', 'create', name)
        other = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                             aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                             config=client_config)
        key, raw = prefix + 'storage-scope', os.urandom(32768)
        keys.append(key)
        marker.touch()
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                uploads = [pool.submit(client.put_object, Bucket=target, Key=key, Body=raw)
                           for client, target in ((s3, bucket), (other, name))]
                try:
                    wait(lambda: db.execute("SELECT count(DISTINCT c.storage_id) FROM chunks c "
                                            "JOIN streams s ON s.id=c.owner_stream WHERE s.object_key=%s "
                                            "AND c.state='preparing'", (key,)).fetchone()[0] == 2,
                         'same content in different stores was serialized or reused across stores')
                finally:
                    marker.unlink(missing_ok=True)
                for upload in uploads:
                    upload.result(timeout=20)
            assert read(key)[0] == other.get_object(Bucket=name, Key=key)['Body'].read() == raw
            copies = db.execute('SELECT c.id,c.storage_id FROM chunks c JOIN extents e ON e.chunk_id=c.id '
                                'JOIN objects o ON o.stream_id=e.stream_id WHERE o.object_key=%s', (key,)).fetchall()
            assert len(copies) == 2 and copies[0][0] != copies[1][0] and copies[0][1] != copies[1][1]
            print('PASS concurrent identical content in different stores keeps independent allocations', flush=True)
        finally:
            marker.unlink(missing_ok=True)
            other.delete_object(Bucket=name, Key=key)

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
            assert available(row), 'storage I/O retained a database transaction'
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
