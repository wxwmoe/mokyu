"""Physical retention, legacy timestamps and sweep checks on a disposable gateway."""
import fcntl
import json
import os
import signal
import subprocess
import time
import tomllib
import uuid
from pathlib import Path

import boto3
import psycopg
import requests
from tests.integration import s3, bucket, read

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
config = Path(os.environ['MOKYU_TEST_CONFIG'])
original = config.read_text()
command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config), 'cli']
backend_file = os.environ.get('MOKYU_TEST_BACKEND')
settings = json.loads(Path(backend_file).read_text()) if backend_file else tomllib.loads(original)['backend']
storage_name = os.environ['MOKYU_TEST_STORAGE']
storage_id = db.execute('SELECT id FROM storages WHERE name=%s', (storage_name,)).fetchone()[0]
backend = boto3.client('s3', endpoint_url=settings['endpoint'], region_name=settings['region'],
                      aws_access_key_id=settings['access_key'], aws_secret_access_key=settings['secret_key'])
pid_file = Path(os.environ['MOKYU_TEST_PID_FILE'])
pid = int(pid_file.read_text())
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


url = os.environ['MOKYU_TEST_WEB']
admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url},
                   json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=15)
assert reply.status_code == 200
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
assert admin.post(url + '/api/v1/account/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}).status_code == 204


def post(path, data, status=200):
    reply = admin.post(url + '/api/v1/maintenance/' + path, json=data, timeout=30)
    assert reply.status_code == status, (path, reply.status_code, reply.text)
    return reply.json()


def sweep():
    return done(post('sweep', {'older_than': '1s', 'storage_id': str(storage_id)})['task_id'])


def approval(scan):
    return post('sweep/preview', {'task_id': scan['id']})


def execute_sweep(preview, status=200):
    result = post('sweep/execute', {'preview_id': preview['id'], 'confirmation': preview['confirmation']}, status)
    return done(result['task_id']) if status == 200 else result


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


def restart(minimum, clear_age=False):
    global process, pid
    profile = cli('storage', 'profile', 'show', storage_name)
    profile.update(min_storage_duration=minimum, request_timeout='1s', retry_timeout='1s', max_retries=0)
    profile_file = Path(os.environ['MOKYU_TEST_RESULTS']) / 'retention-profile.json'
    profile_file.write_text(json.dumps(profile))
    cli('storage', 'profile', 'set', storage_name, str(profile_file))
    cli('maintenance', 'enable')
    stop()
    if clear_age:
        with db.transaction():
            db.execute('UPDATE chunk_encodings SET stored_at=NULL WHERE storage_id=%s', (storage_id,))
            db.execute('UPDATE packs SET stored_at=NULL WHERE storage_id=%s', (storage_id,))
    with open(Path(os.environ['MOKYU_TEST_RESULTS']) / 'storage-retention.log', 'a') as log:
        process = subprocess.Popen(command[:-1], stdout=log, stderr=log, start_new_session=True)
    pid = process.pid
    pid_file.write_text(str(pid))
    def ready():
        assert process.poll() is None, 'gateway exited; inspect storage-retention.log'
        try:
            return cli('status')['maintenance']
        except subprocess.CalledProcessError:
            return False
    wait(ready)
    assert cli('storage', 'profile', 'show', storage_name)['min_storage_duration'] == minimum


def physical_key(source_type, storage):
    prefix = 'packs/' if source_type == 'packs' else ''
    return settings['prefix'].rstrip('/') + '/' + prefix + storage.hex[:2] + '/' + storage.hex


try:
    assert cli('storage', 'profile', 'show', storage_name)['min_storage_duration'] == '0s'
    raw = os.urandom(15 * 1024 * 1024 + 17)
    s3.put_object(Bucket=bucket, Key='retained', Body=raw)
    locations = db.execute('SELECT encoding_id,location_id,nonce,stored_at FROM chunk_encodings ORDER BY encoding_id').fetchall()
    assert all(row[3] is not None for row in locations)
    puts = cli('status')['backend_puts']
    s3.put_object(Bucket=bucket, Key='retained-copy', Body=raw)
    assert cli('status')['backend_puts'] == puts
    assert locations == db.execute('SELECT encoding_id,location_id,nonce,stored_at FROM chunk_encodings ORDER BY encoding_id').fetchall()
    s3.delete_object(Bucket=bucket, Key='retained-copy')
    wait(lambda: (cli('gc', 'run'), scalar("SELECT count(*)=0 FROM streams WHERE object_key='retained-copy'"))[1])
    task('pack', 'run')
    pack = db.execute("SELECT id,location_id,nonce,stored_at FROM packs WHERE state='ready'").fetchone()
    assert pack and pack[3] is not None and read('retained')[0] == raw

    # Missing storage timestamps require conservative remote age checks.
    restart('30d', clear_age=True)
    assert db.execute('SELECT encoding_id,location_id,nonce FROM chunk_encodings ORDER BY encoding_id').fetchall() == [r[:3] for r in locations]
    assert db.execute('SELECT id,location_id,nonce,stored_at FROM packs').fetchone() == (*pack[:3], None)
    assert scalar('SELECT bool_and(stored_at IS NULL) FROM chunk_encodings')
    assert read('retained')[0] == raw
    cli('maintenance', 'disable')

    # New unpack output gets a confirmed timestamp; legacy sources use remote HEAD.
    unpack = post('unpack/preview', {'pack_id': str(pack[0])})
    done(post('unpack/execute', {'preview_id': unpack['id'], 'confirmation': unpack['confirmation']})['task_id'])
    assert scalar("SELECT bool_and(stored_at IS NOT NULL) FROM chunk_encodings WHERE state='ready'")
    for table in ('chunk_encodings', 'packs'):
        db.execute(f"UPDATE {table} SET created_at=now()-interval '100 days',unreferenced_at=now()-interval '3 days' WHERE state='retired'")
    before = cli('status')['backend_deletes']
    cli('gc', 'run')
    assert cli('status')['backend_deletes'] == before
    for table in ('chunk_encodings', 'packs'):
        assert scalar(f"SELECT bool_and(stored_at>now()-interval '1 day' AND state='retired') FROM {table} WHERE state<>'ready'")
    assert read('retained')[0] == raw
    print('PASS default, metadata reuse, write timestamps and legacy remote age', flush=True)

    # A failed HEAD is not absence; a missing abandoned physical source is safe to clear.
    location, storage = locations[0][:2]
    db.execute('UPDATE chunk_encodings SET stored_at=NULL WHERE encoding_id=%s', (location,))
    before = cli('status')
    os.kill(backend_pid, signal.SIGSTOP)
    try:
        cli('gc', 'run')
        assert scalar('SELECT state FROM chunk_encodings WHERE encoding_id=%s', (location,)) == 'retired'
        assert scalar('SELECT stored_at FROM chunk_encodings WHERE encoding_id=%s', (location,)) is None
        assert cli('status')['backend_deletes'] == before['backend_deletes']
        assert cli('status')['runtime']['gc_failures'] > before['runtime']['gc_failures']
    finally:
        os.kill(backend_pid, signal.SIGCONT)
    backend.head_object(Bucket=settings['bucket'], Key=physical_key('chunks', storage))
    backend.delete_object(Bucket=settings['bucket'], Key=physical_key('chunks', storage))
    cli('gc', 'run')
    assert scalar('SELECT state FROM chunk_encodings WHERE encoding_id=%s', (location,)) == 'deleted'

    # Both clocks must expire, independently for packs and independent chunks.
    for table, id_column, ident in [('packs', 'id', pack[0]), ('chunk_encodings', 'encoding_id', locations[1][0])]:
        db.execute(f"UPDATE {table} SET stored_at=now()-interval '31 days',unreferenced_at=now() WHERE {id_column}=%s", (ident,))
        cli('gc', 'run')
        assert scalar(f'SELECT state FROM {table} WHERE {id_column}=%s', (ident,)) == 'retired'
        db.execute(f"UPDATE {table} SET unreferenced_at=now()-interval '3 days' WHERE {id_column}=%s", (ident,))
        cli('gc', 'run')
        assert scalar(f'SELECT state FROM {table} WHERE {id_column}=%s', (ident,)) == 'deleted'

    # Last-reference removal must not let logical GC delete a young physical block.
    s3.put_object(Bucket=bucket, Key='young', Body=os.urandom(1024))
    chunk = scalar("SELECT e.chunk_id FROM extents e JOIN objects o ON o.stream_id=e.stream_id WHERE o.object_key='young'")
    s3.delete_object(Bucket=bucket, Key='young')
    wait(lambda: (cli('gc', 'run'), scalar('SELECT unreferenced_at IS NOT NULL FROM chunks WHERE id=%s', (chunk,)))[1])
    db.execute("UPDATE chunks SET unreferenced_at=now()-interval '3 days' WHERE id=%s", (chunk,))
    cli('gc', 'run')
    assert scalar('SELECT state FROM chunks WHERE id=%s', (chunk,)) == 'deleted'
    assert scalar('SELECT state FROM chunk_encodings WHERE chunk_id=%s', (chunk,)) == 'retired'
    print('PASS HEAD failure/missing source, independent age/grace and young physical protection', flush=True)

    # Manual deletion obeys the same policy, including a changed or resumed preview.
    orphan = physical_key('packs', uuid.uuid4())
    backend.put_object(Bucket=settings['bucket'], Key=orphan, Body=b'unindexed')
    time.sleep(1.2)
    cli('maintenance', 'enable')
    preview = sweep()
    assert preview['detail']['candidates'] == 0
    assert preview['detail']['min_storage_duration_seconds'] == 30 * 86400
    approved = approval(preview)
    assert approved['impact']['minimum_storage_seconds'] == 30 * 86400
    assert execute_sweep(approved)['detail']['candidates'] == 0
    stale = approval(preview)
    restart('0s')
    execute_sweep(stale, status=412)
    preview = sweep()
    assert preview['detail']['candidates'] == 1
    # Persist an approved task before restart under a longer minimum.
    resumed = uuid.uuid4()
    detail = {**preview['detail'], 'dry_run': False, 'candidates': 0, 'bytes': 0, 'samples': []}
    db.execute("INSERT INTO tasks(id,task_type,state,detail) VALUES(%s,'sweep','paused',%s::jsonb)", (resumed, json.dumps(detail)))
    restart('30d')
    cli('task', 'resume', str(resumed))
    assert done(str(resumed))['detail']['candidates'] == 0
    backend.head_object(Bucket=settings['bucket'], Key=orphan)
    restart('0s')
    preview = sweep()
    assert execute_sweep(approval(preview))['detail']['candidates'] == 1
    cli('maintenance', 'disable')
    cli('gc', 'run')
    assert scalar('SELECT state FROM chunk_encodings WHERE chunk_id=%s', (chunk,)) == 'deleted'
    assert read('retained')[0] == raw
    print('PASS sweep preview/execute/resume protection and zero-duration compatibility', flush=True)
finally:
    os.kill(backend_pid, signal.SIGCONT)
    if process and process.poll() is None:
        process.terminate()
        process.wait(timeout=40)
    db.close()
