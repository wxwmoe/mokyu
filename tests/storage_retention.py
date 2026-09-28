"""Physical retention, legacy timestamps and sweep checks on a disposable gateway."""
import fcntl
import json
import os
import pty
import signal
import subprocess
import time
import tomllib
import uuid
from pathlib import Path

import boto3
import psycopg
from integration import s3, bucket, read

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
config = Path(os.environ['MOKYU_TEST_CONFIG'])
original = config.read_text()
command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config), 'cli']
settings = tomllib.loads(original)['backend']
backend = boto3.client('s3', endpoint_url=settings['endpoint'], region_name=settings['region'],
                      aws_access_key_id=settings['access_key'], aws_secret_access_key=settings['secret_key'])
pid = int(os.environ['MOKYU_TEST_GATEWAY_PID'])
backend_pid = int(os.environ['MOKYU_TEST_BACKEND_PID'])
process = None


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True, stderr=subprocess.DEVNULL))


def scalar(query, args=()):
    return db.execute(query, args).fetchone()[0]


def wait(predicate):
    for _ in range(300):
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('condition timed out')


def done(job):
    def finished():
        state = cli('task', 'show', job)
        assert state['state'] not in ('failed', 'paused'), state
        return state if state['state'] == 'completed' else None
    return wait(finished)


def task(*args):
    return done(cli(*args)['task_id'])


def execute_sweep(preview, expected_error=None):
    master, slave = pty.openpty()
    child = subprocess.Popen(command + ['backend', 'sweep', '--execute', '--preview', preview,
                                         '--older-than', '1s'], stdin=slave,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    os.close(slave)
    try:
        os.write(master, (settings['prefix'] + '/\nDELETE\n').encode())
        out, err = child.communicate(timeout=30)
        if expected_error:
            assert child.returncode and expected_error in err, err
            return
        assert child.returncode == 0, err
        return done(json.loads(out)['task_id'])
    finally:
        os.close(master)


def stop():
    os.kill(pid, signal.SIGKILL)
    if process:
        process.wait(timeout=10)
    def unlocked():
        with (Path(os.environ['MOKYU_TEST_DATA']) / 'gateway.lock').open('rb') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(unlocked)


def start(minimum):
    global process, pid
    text = original.replace('allow_http=true', 'allow_http=true\nmin_storage_duration="' + minimum
                            + '"\nrequest_timeout="1s"\nretry_timeout="1s"\nmax_retries=0')
    config.write_text(text)
    process = subprocess.Popen(command[:-1] + ['serve', '--maintenance'],
                               stdout=open(Path(os.environ['MOKYU_TEST_RESULTS']) / 'storage-retention.log', 'a'),
                               stderr=subprocess.STDOUT)
    pid = process.pid
    def ready():
        assert process.poll() is None, 'gateway exited; inspect storage-retention.log'
        try:
            return cli('status')['maintenance']
        except subprocess.CalledProcessError:
            return False
    wait(ready)


def physical_key(kind, storage):
    return settings['prefix'] + '/' + kind + '/' + storage.hex[:2] + '/' + storage.hex


try:
    assert cli('gc', 'status')['min_storage_duration'] == '0s'
    raw = os.urandom(7 * 1024 * 1024 + 17)
    s3.put_object(Bucket=bucket, Key='retained', Body=raw)
    locations = db.execute('SELECT id,storage_id,nonce,stored_at FROM chunk_locations ORDER BY id').fetchall()
    assert all(row[3] is not None for row in locations)
    puts = cli('status')['backend_puts']
    s3.put_object(Bucket=bucket, Key='retained-copy', Body=raw)
    assert cli('status')['backend_puts'] == puts
    assert locations == db.execute('SELECT id,storage_id,nonce,stored_at FROM chunk_locations ORDER BY id').fetchall()
    s3.delete_object(Bucket=bucket, Key='retained-copy')
    wait(lambda: (cli('gc', 'run'), scalar("SELECT count(*)=0 FROM streams WHERE object_key='retained-copy'"))[1])
    task('pack', 'run')
    pack = db.execute("SELECT id,storage_id,nonce,stored_at FROM packs WHERE state='ready'").fetchone()
    assert pack and pack[3] is not None and read('retained')[0] == raw

    # Missing storage timestamps require conservative remote age checks.
    stop()
    with db.transaction():
        db.execute('UPDATE chunk_locations SET stored_at=NULL')
        db.execute('UPDATE packs SET stored_at=NULL')
    start('30d')
    assert db.execute('SELECT id,storage_id,nonce FROM chunk_locations ORDER BY id').fetchall() == [r[:3] for r in locations]
    assert db.execute('SELECT id,storage_id,nonce,stored_at FROM packs').fetchone() == (*pack[:3], None)
    assert scalar('SELECT bool_and(stored_at IS NULL) FROM chunk_locations')
    assert read('retained')[0] == raw
    cli('maintenance', 'disable')

    # New unpack output gets a confirmed timestamp; legacy sources use remote HEAD.
    task('pack', 'unpack', str(pack[0]), '--execute')
    assert scalar("SELECT bool_and(stored_at IS NOT NULL) FROM chunk_locations WHERE state='ready'")
    for table in ('chunk_locations', 'packs'):
        db.execute(f"UPDATE {table} SET created_at=now()-interval '100 days',unreferenced_at=now()-interval '3 days' WHERE state='retired'")
    before = cli('status')['backend_deletes']
    cli('gc', 'run')
    assert cli('status')['backend_deletes'] == before
    for table in ('chunk_locations', 'packs'):
        assert scalar(f"SELECT bool_and(stored_at>now()-interval '1 day' AND state='retired') FROM {table} WHERE state<>'ready'")
    assert read('retained')[0] == raw
    print('PASS default, metadata reuse, write timestamps and legacy remote age', flush=True)

    # A failed HEAD is not absence; a missing abandoned physical source is safe to clear.
    location, storage = locations[0][:2]
    db.execute('UPDATE chunk_locations SET stored_at=NULL WHERE id=%s', (location,))
    before = cli('status')
    os.kill(backend_pid, signal.SIGSTOP)
    try:
        cli('gc', 'run')
        assert scalar('SELECT state FROM chunk_locations WHERE id=%s', (location,)) == 'retired'
        assert scalar('SELECT stored_at FROM chunk_locations WHERE id=%s', (location,)) is None
        assert cli('status')['backend_deletes'] == before['backend_deletes']
        assert cli('status')['runtime']['gc_failures'] > before['runtime']['gc_failures']
    finally:
        os.kill(backend_pid, signal.SIGCONT)
    backend.delete_object(Bucket=settings['bucket'], Key=physical_key('chunks', storage))
    cli('gc', 'run')
    assert scalar('SELECT state FROM chunk_locations WHERE id=%s', (location,)) == 'deleted'

    # Both clocks must expire, independently for packs and independent chunks.
    for table, ident in [('packs', pack[0]), ('chunk_locations', locations[1][0])]:
        db.execute(f"UPDATE {table} SET stored_at=now()-interval '31 days',unreferenced_at=now() WHERE id=%s", (ident,))
        cli('gc', 'run')
        assert scalar(f'SELECT state FROM {table} WHERE id=%s', (ident,)) == 'retired'
        db.execute(f"UPDATE {table} SET unreferenced_at=now()-interval '3 days' WHERE id=%s", (ident,))
        cli('gc', 'run')
        assert scalar(f'SELECT state FROM {table} WHERE id=%s', (ident,)) == 'deleted'

    # Last-reference removal must not let logical GC delete a young physical block.
    s3.put_object(Bucket=bucket, Key='young', Body=os.urandom(1024))
    chunk = scalar("SELECT e.chunk_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id WHERE o.key='young'")
    s3.delete_object(Bucket=bucket, Key='young')
    wait(lambda: (cli('gc', 'run'), scalar('SELECT unreferenced_at IS NOT NULL FROM chunks WHERE id=%s', (chunk,)))[1])
    db.execute("UPDATE chunks SET unreferenced_at=now()-interval '3 days' WHERE id=%s", (chunk,))
    cli('gc', 'run')
    assert scalar('SELECT state FROM chunks WHERE id=%s', (chunk,)) == 'deleted'
    assert scalar('SELECT state FROM chunk_locations WHERE chunk_id=%s', (chunk,)) == 'retired'
    print('PASS HEAD failure/missing source, independent age/grace and young physical protection', flush=True)

    # Manual deletion obeys the same policy, including a changed or resumed preview.
    orphan = physical_key('packs', uuid.uuid4())
    backend.put_object(Bucket=settings['bucket'], Key=orphan, Body=b'unindexed')
    time.sleep(1.2)
    cli('maintenance', 'enable')
    preview = task('backend', 'sweep', '--older-than', '1s')
    assert preview['detail']['candidates'] == 0
    assert preview['detail']['min_storage_duration_seconds'] == 30 * 86400
    assert execute_sweep(preview['id'])['detail']['candidates'] == 0
    stop()
    start('0s')
    execute_sweep(preview['id'], expected_error='preview again')
    preview = task('backend', 'sweep', '--older-than', '1s')
    assert preview['detail']['candidates'] == 1
    # Persist an approved task before restart under a longer minimum.
    resumed = uuid.uuid4()
    detail = {**preview['detail'], 'dry_run': False, 'candidates': 0, 'bytes': 0, 'samples': []}
    db.execute("INSERT INTO tasks(id,kind,state,detail) VALUES(%s,'sweep','paused',%s::jsonb)", (resumed, json.dumps(detail)))
    stop()
    start('30d')
    cli('task', 'resume', str(resumed))
    assert done(str(resumed))['detail']['candidates'] == 0
    backend.head_object(Bucket=settings['bucket'], Key=orphan)
    stop()
    start('0s')
    preview = task('backend', 'sweep', '--older-than', '1s')
    assert execute_sweep(preview['id'])['detail']['candidates'] == 1
    cli('maintenance', 'disable')
    cli('gc', 'run')
    assert scalar('SELECT state FROM chunk_locations WHERE chunk_id=%s', (chunk,)) == 'deleted'
    assert read('retained')[0] == raw
    print('PASS sweep preview/execute/resume protection and zero-duration compatibility', flush=True)
finally:
    os.kill(backend_pid, signal.SIGCONT)
    if process and process.poll() is None:
        process.terminate()
        process.wait(timeout=40)
    config.write_text(original)
    db.close()
