"""Paged reads and copies on a disposable local-storage gateway with pg_stat_statements."""
import hashlib
import json
import math
import os
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import requests
from botocore.exceptions import ClientError, IncompleteReadError, ResponseStreamingError
from urllib3.exceptions import ProtocolError
from tests.integration import s3, bucket, read

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
prefix = 'chunk-reads-' + uuid.uuid4().hex[:10]
seed, source, copied = [prefix + suffix for suffix in ('-seed', '-source', '-copy')]
url = os.environ['MOKYU_TEST_WEB']


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))


def mapping_calls():
    prefixes = ('SELECT e.*,c.id,', 'SELECT offset_bytes FROM extents')
    return sum(calls for query, calls in db.execute('''SELECT query,calls FROM pg_stat_statements
        WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())''')
               if query.startswith(prefixes))


def version(bucket_id, key):
    return db.execute('SELECT stream_id FROM objects WHERE bucket_id=%s AND object_key=%s',
                      (bucket_id, key)).fetchone()[0]


def extents(stream):
    return db.execute('''SELECT offset_bytes,length,chunk_id,source_offset FROM extents
        WHERE stream_id=%s ORDER BY offset_bytes''', (stream,)).fetchall()


try:
    raw = os.urandom(4096)
    s3.put_object(Bucket=bucket, Key=seed, Body=raw)
    bucket_id = db.execute('SELECT id FROM buckets WHERE name=%s', (bucket,)).fetchone()[0]
    original = version(bucket_id, seed)
    chunk = extents(original)[0][2]
    segment, count = raw[17:-17], 513
    payload = segment * count
    stream = uuid.uuid4()
    # Repeat a valid sliced chunk to cross both page limits without a large upload.
    with db.transaction():
        db.execute('''INSERT INTO streams SELECT (jsonb_populate_record(NULL::streams,
            to_jsonb(s)||jsonb_build_object('id',%s::text,'object_key',%s::text,'size',%s::bigint,
                'etag',%s::text,'checksums','{}'::jsonb))).* FROM streams s WHERE id=%s''',
            (str(stream), source, len(payload), hashlib.md5(payload).hexdigest(), original))
        db.execute('''INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset)
            SELECT %s,i*%s,%s,%s,17 FROM generate_series(0,%s) i''',
            (stream, len(segment), len(segment), chunk, count - 1))
        db.execute('INSERT INTO objects(bucket_id,object_key,stream_id,write_token) VALUES(%s,%s,%s,%s)',
                   (bucket_id, source, stream, stream))
    before = mapping_calls()
    assert read(source)[0] == payload
    assert mapping_calls() - before == math.ceil(count / 64) + 1
    for start, end in [(0, 1), (len(payload) - 37, len(payload)),
                       (63 * len(segment) + 5, 65 * len(segment) + 13)]:
        before = mapping_calls()
        assert read(source, Range=f'bytes={start}-{end - 1}')[0] == payload[start:end]
        assert mapping_calls() - before == 2
    assert read(source, Range='bytes=-71')[0] == payload[-71:]
    assert read(source)[0] == payload
    print('PASS 513 sliced extents, bounded metadata queries and full/Range reads', flush=True)

    try:
        db.execute("UPDATE chunks SET state='failed' WHERE id=%s", (chunk,))
        try:
            read(source)
        except ClientError as exc:
            assert exc.response['ResponseMetadata']['HTTPStatusCode'] >= 500, exc.response
        except (IncompleteReadError, ResponseStreamingError, ProtocolError):
            pass
        else:
            raise AssertionError('read accepted missing ready chunk metadata')
    finally:
        db.execute("UPDATE chunks SET state='ready' WHERE id=%s", (chunk,))
    assert read(source)[0] == payload
    print('PASS unavailable chunk metadata fails closed and restored metadata reads correctly', flush=True)

    before = db.execute('SELECT count(*) FROM chunks').fetchone()[0]
    s3.copy_object(Bucket=bucket, Key=copied, CopySource={'Bucket': bucket, 'Key': source})
    assert extents(version(bucket_id, copied)) == extents(stream)
    assert db.execute('SELECT count(*) FROM chunks').fetchone()[0] == before
    assert read(copied)[0] == payload
    print('PASS same-storage copy crosses 500 extents without rewriting chunks or slice offsets', flush=True)

    cli('key', 'create', prefix, 'block', '-a', 'aes-256-gcm')
    target_storage = cli('storage', 'create', prefix, 'local', '--init', '--key', prefix,
                         '--path', str(Path(os.environ['MOKYU_TEST_DATA']) / prefix))
    target = cli('bucket', 'create', prefix, '--storage', target_storage['id'])
    admin = requests.Session()
    reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url},
                       json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=10)
    assert reply.status_code == 200, reply.status_code
    admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    reply = admin.post(url + '/api/v1/media/actions', json={
        'bucket': str(bucket_id), 'action': 'copy', 'target_bucket': target['id'],
        'confirm_storage_migration': True, 'objects': [{'client_id': str(uuid.uuid4()),
            'key': source, 'version': str(stream), 'target_key': copied}]}, timeout=30)
    assert reply.status_code == 200, reply.text
    result = reply.json()['results'][0]
    assert result['status'] == 202, result
    deadline = time.monotonic() + 60
    while True:
        task = cli('task', 'show', result['task_id'])
        assert task['state'] not in ('failed', 'paused'), task
        if task['state'] == 'completed':
            break
        assert time.monotonic() < deadline, task
        time.sleep(.1)
    output = version(target['id'], copied)
    rows = extents(output)
    assert len(rows) == count and all(row[2] != chunk and row[3] == 0 for row in rows), rows
    assert [(row[0], row[1]) for row in rows] == [(i * len(segment), len(segment)) for i in range(count)]
    targets = db.execute('''SELECT DISTINCT c.storage_id FROM extents e JOIN chunks c ON c.id=e.chunk_id
        WHERE e.stream_id=%s''', (output,)).fetchall()
    assert targets == [(uuid.UUID(target_storage['id']),)], targets
    reply = admin.get(url + f"/api/v1/buckets/{target['id']}/object/content",
                      params={'key': copied, 'version': str(output)}, timeout=30)
    assert reply.status_code == 200 and reply.content == payload, reply.status_code
    print('PASS cross-storage copy writes independent chunks and preserves all sliced bytes', flush=True)
finally:
    db.close()
