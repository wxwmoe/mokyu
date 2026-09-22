"""Reference/GC checks. Requires an isolated database and the gateway admin socket.

The test deliberately ages test-only rows to exercise GC without waiting 48h.
Never use a database that contains user data.
"""
import hashlib
import json
import os
import pty
import subprocess
import time
from pathlib import Path
import boto3
import psycopg
from botocore.config import Config

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
binary = os.environ['MGW_TEST_BINARY']
configuration = os.environ['MGW_TEST_CONFIG']
endpoint = os.environ['MGW_TEST_ENDPOINT']
db = psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
config = Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                retries={'max_attempts': 0}, request_checksum_calculation='when_required',
                response_checksum_validation='when_required')


def cli(*args):
    result = subprocess.run([binary, '--config', configuration, 'cli', *args],
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def danger(identity, *args):
    refused = subprocess.run([binary, '--config', configuration, 'cli', *args],
                             capture_output=True, text=True, input='')
    assert refused.returncode != 0 and 'require a terminal' in refused.stderr, refused.stderr
    master, slave = pty.openpty()
    process = subprocess.Popen([binary, '--config', configuration, 'cli', *args],
                               stdin=slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    os.close(slave)
    try:
        os.write(master, (identity + '\nDELETE\n').encode())
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr
        return json.loads(stdout)
    finally:
        os.close(master)


def scalar(sql, params=()):
    return db.execute(sql, params).fetchone()[0]


def wait(fn, seconds=30):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if fn():
            return
        time.sleep(.2)
    raise AssertionError('condition did not become true')


def client(bucket):
    value = cli('credential', 'create', bucket)
    return boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                        aws_access_key_id=value['access_key'], aws_secret_access_key=value['secret_key'],
                        config=config)


def task_done(task):
    result = cli('task', 'show', task)
    assert result['state'] != 'failed', result
    return result['state'] == 'completed'


# Different part boundaries and ordinary PUT must produce exactly the same map.
maps = []
for key in ['multipart', 'same-put']:
    rows = db.execute('''SELECT e.offset_bytes,e.length,e.source_offset,c.hash,c.raw_size
        FROM objects o JOIN buckets b ON b.id=o.bucket_id JOIN extents e ON e.stream_id=o.stream_id
        JOIN chunks c ON c.id=e.chunk_id WHERE b.name='test-media' AND o.key=%s
        ORDER BY e.offset_bytes''', (key,)).fetchall()
    assert rows
    maps.append(rows)
assert maps[0] == maps[1]
print('PASS canonical whole-object CDC agrees for PUT and out-of-order multipart', flush=True)

names = ['gc-first-' + os.urandom(4).hex(), 'gc-second-' + os.urandom(4).hex()]
for name in names:
    cli('bucket', 'create', name)
clients = [client(name) for name in names]
payload = os.urandom(128 * 1024)
for name, s3 in zip(names, clients):
    s3.put_object(Bucket=name, Key='shared', Body=payload)
chunk = scalar('''SELECT e.chunk_id FROM buckets b JOIN objects o ON o.bucket_id=b.id
    JOIN extents e ON e.stream_id=o.stream_id WHERE b.name=%s''', (names[0],))
other = scalar('''SELECT e.chunk_id FROM buckets b JOIN objects o ON o.bucket_id=b.id
    JOIN extents e ON e.stream_id=o.stream_id WHERE b.name=%s''', (names[1],))
assert chunk == other
db.execute("UPDATE chunks SET unreferenced_at=now()-interval '4 days' WHERE id=%s", (chunk,))
cli('gc', 'run')
assert scalar('SELECT state FROM chunks WHERE id=%s', (chunk,)) == 'ready'
db.execute('UPDATE chunks SET unreferenced_at=NULL WHERE id=%s', (chunk,))
first = cli('bucket', 'purge', names[0])
task = danger(names[0], 'bucket', 'purge', names[0], '--execute')['task_id']
wait(lambda: task_done(task))
with clients[1].get_object(Bucket=names[1], Key='shared')['Body'] as body:
    assert body.read() == payload
assert scalar('SELECT unreferenced_at FROM chunks WHERE id=%s', (chunk,)) is None
clients[1].delete_object(Bucket=names[1], Key='shared')
wait(lambda: scalar('SELECT unreferenced_at IS NOT NULL FROM chunks WHERE id=%s', (chunk,)))
cli('gc', 'run')
assert scalar('SELECT state FROM chunks WHERE id=%s', (chunk,)) == 'ready'
db.execute("UPDATE chunks SET unreferenced_at=now()-interval '4 days' WHERE id=%s", (chunk,))
cli('gc', 'pause')
cli('gc', 'run')
assert scalar('SELECT state FROM chunks WHERE id=%s', (chunk,)) == 'ready'
cli('gc', 'resume')
cli('gc', 'run')
assert scalar('SELECT state FROM chunks WHERE id=%s', (chunk,)) == 'deleted'
print('PASS cross-bucket dedup, purge, last-reference grace, paused and resumed GC', flush=True)

# A destructive sweep requires a completed preview, exact prefix and maintenance.
cli('maintenance', 'enable')
preview = cli('backend', 'sweep', '--older-than', '1s')['task_id']
wait(lambda: task_done(preview))
scope = cli('task', 'show', preview)['detail']['prefix']
task = danger(scope, 'backend', 'sweep', '--execute', '--preview', preview,
              '--older-than', '1s')['task_id']
wait(lambda: task_done(task))
cli('maintenance', 'disable')
creds = json.loads(Path(os.environ['MGW_TEST_CREDENTIALS']).read_text())
s3 = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                  aws_access_key_id=creds['access_key'], aws_secret_access_key=creds['secret_key'], config=config)
with s3.get_object(Bucket='test-media', Key='same-put')['Body'] as body:
    digest = hashlib.sha256(body.read()).hexdigest()
with s3.get_object(Bucket='test-media', Key='multipart')['Body'] as body:
    assert hashlib.sha256(body.read()).hexdigest() == digest
print('PASS maintenance sweep and protection of all indexed current data', flush=True)

wait(lambda: scalar("SELECT count(*) FROM fragments f WHERE NOT EXISTS(SELECT 1 FROM extents WHERE fragment_id=f.id)") == 0)
actual = sum(p.stat().st_size for p in (Path(os.environ['MGW_TEST_DATA']) / 'multipart').iterdir() if p.is_file())
assert cli('status')['local_bytes'][0] == actual
assert scalar("SELECT count(*) FROM extents e LEFT JOIN chunks c ON c.id=e.chunk_id WHERE e.chunk_id IS NOT NULL AND c.state<>'ready'") == 0
help_text = subprocess.check_output([binary, 'cli', '--help'], text=True)
assert not any('\u3400' <= c <= '\u9fff' for c in help_text)
print('PASS local quota accounting, reference integrity and English CLI help', flush=True)
db.close()
