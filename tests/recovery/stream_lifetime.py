"""Read/write lifetime protection while another request overwrites, deletes and reclaims."""
import http.client
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import psycopg


def run(s3, bucket, db, faults, gc, single):
    prefix = 'stream-lifetime-' + uuid.uuid4().hex
    marker = faults / 'read-pinned'
    keys = []

    def scalar(query, args=()):
        row = db.execute(query, args).fetchone()
        return row[0] if row else None

    def wait(predicate):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if value := predicate():
                return value
            time.sleep(.05)
        raise AssertionError('stream lifetime condition timed out')

    def version(key):
        return scalar('SELECT o.stream_id FROM objects o JOIN buckets b ON b.id=o.bucket_id '
                      'WHERE b.name=%s AND o.object_key=%s', (bucket, key))

    def retained(stream):
        return scalar('SELECT EXISTS(SELECT 1 FROM streams WHERE id=%s)', (stream,))

    def reclaimed(stream):
        gc()
        return not retained(stream)

    def lease_calls():
        return scalar("SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                      "WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                      "AND query ~ '^(INSERT INTO|UPDATE|DELETE FROM) stream_leases'")

    before = lease_calls() if single else None
    try:
        for operation in ('get', 'head', 'copy'):
            key, target = prefix + '-' + operation, prefix + '-' + operation + '-copy'
            keys.extend((key, target))
            raw = os.urandom(32768)
            s3.put_object(Bucket=bucket, Key=key, Body=raw)
            stream = version(key)

            def read():
                if operation == 'get':
                    return s3.get_object(Bucket=bucket, Key=key)['Body'].read()
                if operation == 'head':
                    return s3.head_object(Bucket=bucket, Key=key)['ContentLength']
                return s3.copy_object(Bucket=bucket, Key=target, CopySource={'Bucket': bucket, 'Key': key})

            marker.with_suffix('.hit').unlink(missing_ok=True)
            marker.touch()
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(read)
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or pending.done())
                    assert not pending.done(), 'read missed its pin barrier'
                    leased = scalar("SELECT EXISTS(SELECT 1 FROM stream_leases WHERE stream_id=%s "
                                    "AND lease_type='read' AND expires_at>now())", (stream,))
                    assert leased is not single, 'read used the wrong coordination mode'
                    if operation != 'copy':
                        s3.put_object(Bucket=bucket, Key=key, Body=b'new version')
                    s3.delete_object(Bucket=bucket, Key=key)
                    gc()
                    assert retained(stream), 'GC reclaimed an active reader'
                finally:
                    marker.unlink(missing_ok=True)
                result = pending.result(timeout=30)
            if operation == 'get':
                assert result == raw
            elif operation == 'head':
                assert result == len(raw)
            else:
                assert s3.get_object(Bucket=bucket, Key=target)['Body'].read() == raw
            wait(lambda: reclaimed(stream))
            changes = 'delete and GC' if operation == 'copy' else 'overwrite, delete and GC'
            print('PASS ' + operation + ' retains its original stream through ' + changes, flush=True)

        key = prefix + '-writer'
        keys.append(key)
        target = urlsplit(s3.generate_presigned_url('put_object', Params={
            'Bucket': bucket, 'Key': key, 'ContentLength': 10}, HttpMethod='PUT', ExpiresIn=60))
        connection = http.client.HTTPConnection(target.hostname, target.port, timeout=10)
        try:
            connection.putrequest('PUT', target.path + '?' + target.query)
            connection.putheader('Content-Length', '10')
            connection.endheaders()
            connection.send(b'x')
            stream = wait(lambda: scalar("SELECT s.id FROM streams s JOIN buckets b ON b.id=s.bucket_id "
                                         "WHERE b.name=%s AND s.object_key=%s AND s.state='writing'", (bucket, key)))
            leased = scalar("SELECT EXISTS(SELECT 1 FROM stream_leases WHERE stream_id=%s "
                            "AND lease_type='write' AND expires_at>now())", (stream,))
            assert leased is not single, 'write used the wrong coordination mode'
            gc()
            assert scalar('SELECT state FROM streams WHERE id=%s', (stream,)) == 'writing'
        finally:
            connection.close()
        if not single:
            db.execute("UPDATE stream_leases SET expires_at=now()-interval '1 second' "
                       "WHERE stream_id=%s AND lease_type='write'", (stream,))
        wait(lambda: reclaimed(stream))
        s3.put_object(Bucket=bucket, Key=key, Body=b'retried')
        assert s3.get_object(Bucket=bucket, Key=key)['Body'].read() == b'retried'
        print('PASS active writer survives GC; cancellation releases protection and permits retry', flush=True)
        if single:
            assert lease_calls() == before, 'single runtime wrote or expired stream leases'
            assert scalar('SELECT count(*) FROM stream_leases') == 0
            print('PASS single reads, writes, copy and cleanup perform no stream lease writes', flush=True)
    finally:
        marker.unlink(missing_ok=True)
        for key in keys:
            s3.delete_object(Bucket=bucket, Key=key)


if __name__ == '__main__':
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    from tests.integration import s3, bucket
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']

    def gc():
        return json.loads(subprocess.check_output(command + ['gc', 'run'], text=True, timeout=30))

    with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True) as db:
        db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
        single = db.execute('SELECT deployment_mode FROM mokyu_meta WHERE singleton').fetchone()[0] == 'single'
        run(s3, bucket, db, Path(os.environ['MOKYU_TEST_FAULT_DIR']), gc, single)
