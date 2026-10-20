"""Native physical source reads and integrity checks on an isolated S3 storage."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
import psycopg
from tests.integration import s3, bucket, read

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = os.environ['MOKYU_TEST_STORAGE']
fixture = json.loads(Path(os.environ['MOKYU_TEST_BACKEND_CREDENTIALS']).read_text())
backend = boto3.client('s3', endpoint_url=fixture['endpoint'], region_name='us-east-1',
    aws_access_key_id=fixture['access'], aws_secret_access_key=fixture['secret'])
prefix = 'physical-' + uuid.uuid4().hex[:12] + '/'


def cli(*args):
    reply = subprocess.run(command + list(args), capture_output=True, text=True, timeout=20)
    assert reply.returncode == 0, reply.stderr
    return json.loads(reply.stdout)


def wait(predicate):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise AssertionError('physical source test timed out')


def pending(key):
    return db.execute('''SELECT EXISTS(SELECT 1 FROM objects o JOIN buckets b ON b.id=o.bucket_id
        JOIN extents e ON e.stream_id=o.stream_id JOIN pending_uploads p ON p.chunk_id=e.chunk_id
        WHERE b.name=%s AND o.object_key=%s)''', (bucket, key)).fetchone()[0]


def inspect(key, mode, expected=()):
    job = cli('integrity', 'check', '--bucket', bucket, '--key', key, '--mode', mode)['task_id']
    def finished():
        state = cli('task', 'show', job)
        assert state['state'] not in ('failed', 'paused'), state
        return state if state['state'] == 'completed' else None
    wait(finished)
    records = db.execute("SELECT id,detail FROM tasks WHERE (id=%s OR parent_task_id=%s) AND detail ? 'issues'", (job, job)).fetchall()
    assert len(records) == 1, records
    task, detail = records[0]
    codes = db.execute('SELECT code FROM integrity_issues WHERE task_id=%s', (task,)).fetchall()
    assert {row[0] for row in codes} == set(expected), (mode, detail, codes)
    assert detail['issues'] == len(codes)
    return task


def profile(value):
    current = cli('storage', 'profile', 'show', name)
    current.update(value)
    file = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'physical-profile.json'
    file.write_text(json.dumps(current))
    cli('storage', 'profile', 'set', name, str(file))


original = cli('storage', 'profile', 'show', name)
content = {prefix + 'chunk': os.urandom(32768), prefix + 'pack': os.urandom(6 * 1024 * 1024 + 17)}
try:
    profile({'upload_cache': True, 'pack_enabled': True})
    cli('storage', 'check', name)
    cli('maintenance', 'pack-creation', 'stop')
    s3.put_object(Bucket=bucket, Key=prefix + 'chunk', Body=content[prefix + 'chunk'])
    wait(lambda: not pending(prefix + 'chunk'))
    cli('maintenance', 'pack-creation', 'resume')
    s3.put_object(Bucket=bucket, Key=prefix + 'pack', Body=content[prefix + 'pack'])
    wait(lambda: not pending(prefix + 'pack'))
    sources = {}
    for suffix in ('chunk', 'pack'):
        row = db.execute('''SELECT c.pack_id,l.encoding_id,p.id,s.prefix
            FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id
            JOIN chunks c ON c.id=e.chunk_id JOIN storages s ON s.id=c.storage_id
            LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready'
            LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready'
            WHERE b.name=%s AND o.object_key=%s ORDER BY e.offset_bytes LIMIT 1''', (bucket, prefix + suffix)).fetchone()
        pack, standalone, packed, namespace = row
        assert (pack is not None) == (suffix == 'pack'), row
        location = (packed if pack else standalone).hex
        path = '/'.join(filter(None, [namespace.rstrip('/'), 'packs' if pack else '', location[-2:], location]))
        sources[suffix] = {'Bucket': fixture['bucket'], 'Key': path}
    profile({'cache_enabled': False, 'upload_cache': False})
    for key, raw in content.items():
        before = cli('status')['backend_gets']
        assert read(key)[0] == raw
        assert read(key, Range='bytes=13-4096')[0] == raw[13:4097]
        assert cli('status')['backend_gets'] > before
        for mode in ('metadata', 'head', 'full'):
            child = inspect(key, mode)
            if key.endswith('/pack'):
                assert db.execute('SELECT count(*) FROM integrity_packs WHERE task_id=%s', (child,)).fetchone()[0] == 1
    print('PASS standalone and pack GET/Range, metadata/head/full checks and one persisted inspection per pack', flush=True)
    for suffix, remote in sources.items():
        encoded = backend.get_object(**remote)['Body'].read()
        try:
            backend.delete_object(**remote)
            inspect(prefix + suffix, 'metadata')
            inspect(prefix + suffix, 'head', ['remote_missing'])
            inspect(prefix + suffix, 'full', ['remote_missing'])
            backend.put_object(**remote, Body=encoded[:-1])
            inspect(prefix + suffix, 'head', ['length_mismatch'])
            inspect(prefix + suffix, 'full', ['length_mismatch'])
            backend.put_object(**remote, Body=bytes([encoded[0] ^ 1]) + encoded[1:])
            inspect(prefix + suffix, 'full', ['authentication_failed'])
        finally:
            backend.put_object(**remote, Body=encoded)
        assert read(prefix + suffix)[0] == content[prefix + suffix]
    print('PASS missing, truncated and corrupted physical objects are classified without changing logical data', flush=True)
finally:
    profile({'cache_enabled': original['cache_enabled'], 'upload_cache': original['upload_cache'],
             'pack_enabled': original['pack_enabled']})
    cli('maintenance', 'pack-creation', 'resume')
    db.close()
