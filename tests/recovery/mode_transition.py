"""Deployment changes drain work and require a restart on an isolated runtime."""
import json
import os
import signal
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import requests
from tests.integration import bucket, error, read, s3

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
binary = os.environ['MOKYU_TEST_BINARY']
original_config = Path(os.environ['MOKYU_TEST_CONFIG'])
current_config = original_config
data = Path(os.environ['MOKYU_TEST_DATA'])
pid_file = Path(os.environ['MOKYU_TEST_PID_FILE'])
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
storage = os.environ['MOKYU_TEST_STORAGE']
url = os.environ['MOKYU_TEST_WEB']
admin = requests.Session()
reply = admin.post(url + '/api/v1/auth/login', headers={'Origin': url},
                   json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=15)
assert reply.status_code == 200
admin.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
assert admin.post(url + '/api/v1/account/reauth', json={'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=15).status_code == 204
process = None


def cli(*args, rejected=None):
    result = subprocess.run([binary, '--config', str(current_config), 'cli', *args], capture_output=True, text=True, timeout=30)
    if rejected:
        assert result.returncode != 0, (rejected, args, result.stdout)
        return
    assert result.returncode == 0, (args, result.stderr)
    return json.loads(result.stdout)


def scalar(query, args=()):
    return db.execute(query, args).fetchone()[0]


def wait(predicate):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('mode transition condition timed out')


def drained():
    return scalar("SELECT NOT EXISTS(SELECT 1 FROM tasks WHERE state IN ('queued','running','paused') OR lease_token IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM stream_leases WHERE expires_at>now())")


def alive(pid):
    stat = Path(f'/proc/{pid}/stat')
    return stat.exists() and not stat.read_text().rsplit(') ', 1)[1].startswith('Z ')


def stop():
    pid = int(pid_file.read_text())
    os.kill(pid, signal.SIGINT)
    wait(lambda: not alive(pid))
    if process:
        process.wait(timeout=5)


def start(config, role=None, ready=True):
    global current_config, process
    current_config = config
    command = [binary, '--config', str(config), *([role] if role else [])]
    with (data / 'mode-runtime.log').open('a') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True,
                                   env={**os.environ, 'MOKYU_TEST_FAULT_DIR': str(faults)})
    pid_file.write_text(str(process.pid))
    def available():
        assert process.poll() is None, 'mode runtime exited'
        return subprocess.run([binary, '--config', str(config), 'cli', 'status', *(['--ready'] if ready else [])], capture_output=True, timeout=10).returncode == 0
    wait(available)


def control_config(name):
    control_data = data / name
    control_data.mkdir()
    text = original_config.read_text().replace(str(data), str(control_data))
    lines = text.splitlines()
    at = lines.index('[instance]')
    end = next((i for i in range(at + 1, len(lines)) if lines[i].startswith('[')), len(lines))
    lines[at + 1:end] = [line for line in lines[at + 1:end] if not line.startswith('name =')]
    lines.insert(at + 1, 'name = ' + json.dumps(name))
    config = data / (name + '.toml')
    config.write_text('\n'.join(lines) + '\n')
    return config


def rejected_mode(reason):
    cli('cluster', 'mode', 'cluster', rejected=reason)
    assert cli('status')['deployment_mode'] == 'single'
    assert not cli('status')['mode_mismatch']


marker = faults / 'read-pinned'
committed = faults / 'mode-committed'
try:
    for policy in ('pack', 'reuse', 'reclaim', 'repack', 'range', 'gc', 'cleanup'):
        cli('maintenance', 'pause', policy)
    wait(drained)
    record = cli('storage', 'show', storage)
    uploads = cli('settings', 'get', 'uploads')
    uploads['staging_storage_id'] = record['id']
    settings_file = data / 'mode-uploads.json'
    settings_file.write_text(json.dumps(uploads))
    cli('settings', 'set', 'uploads', str(settings_file))
    raw = os.urandom(1024 * 1024 + 17)
    s3.put_object(Bucket=bucket, Key='mode-source', Body=raw)
    wait(drained)

    db.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
    query = "SELECT COALESCE(sum(calls),0) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND query='SELECT deployment_mode FROM mokyu_meta WHERE singleton'"
    before = scalar(query)
    for _ in range(20):
        assert s3.head_object(Bucket=bucket, Key='mode-source')['ContentLength'] == len(raw)
    assert scalar(query) == before
    print('PASS single HEAD requests do not query deployment mode', flush=True)
    rejected_mode('enable maintenance')

    upload = s3.create_multipart_upload(Bucket=bucket, Key='mode-upload')['UploadId']
    s3.upload_part(Bucket=bucket, Key='mode-upload', UploadId=upload, PartNumber=1, Body=b'local fragment')
    cli('maintenance', 'enable')
    rejected_mode('finish or abort uploads')
    cli('maintenance', 'disable')
    s3.abort_multipart_upload(Bucket=bucket, Key='mode-upload', UploadId=upload)
    cli('maintenance', 'enable')
    wait(drained)

    paused = uuid.uuid4()
    db.execute("INSERT INTO tasks(id,task_type,state) VALUES(%s,'usage','paused')", (paused,))
    rejected_mode('finish queued, running and paused tasks')
    db.execute("UPDATE tasks SET state='failed' WHERE id=%s", (paused,))
    chunk = scalar("SELECT e.chunk_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id WHERE o.object_key='mode-source' LIMIT 1")
    db.execute('INSERT INTO pending_uploads(chunk_id,offset_bytes,staged_size,next_retry_at) VALUES(%s,0,1,now())', (chunk,))
    try:
        rejected_mode('flush pending uploads')
    finally:
        db.execute('DELETE FROM pending_uploads WHERE chunk_id=%s', (chunk,))
    print('PASS maintenance, unfinished upload/task and pending-source guards preserve the old mode', flush=True)

    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(lambda: read('mode-source')[0])
        try:
            wait(marker.with_suffix('.hit').exists)
            rejected_mode('wait for active reads and writes')
        finally:
            marker.unlink(missing_ok=True)
        assert pending.result(timeout=30) == raw
    wait(drained)
    revision = scalar('SELECT deployment_mode_revision FROM mokyu_meta')
    cli('cluster', 'mode', 'cluster')
    status = cli('status')
    assert status['mode_mismatch'] and status['deployment_mode'] == 'cluster'
    assert scalar('SELECT deployment_mode_revision FROM mokyu_meta') == revision + 1
    assert scalar("SELECT detail->>'execution_mode' FROM tasks WHERE id=%s", (paused,)) == 'single'
    cli('status', '--ready', rejected='mode_mismatch')
    error('ServiceUnavailable', s3.get_object, Bucket=bucket, Key='mode-source')
    control = control_config('mode-control')
    blocked = subprocess.run([binary, '--config', str(control), 'control'], capture_output=True, text=True, timeout=20)
    assert blocked.returncode != 0 and 'incompatible mode' in blocked.stderr
    print('PASS active read rejection, committed mode and exclusive owner require the old runtime to stop', flush=True)

    stop()
    start(control, 'control')
    cli('task', 'resume', str(paused), rejected='different deployment mode')
    wait(drained)
    with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True) as other:
        assert other.execute('SELECT pg_try_advisory_lock_shared(734922709851001)').fetchone()[0]
        cli('cluster', 'mode', 'single', rejected='stop every other runtime')
        assert cli('status')['deployment_mode'] == 'cluster'
    cli('cluster', 'mode', 'single')
    assert cli('status')['mode_mismatch']
    with psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True) as other:
        assert not other.execute('SELECT pg_try_advisory_lock_shared(734922709851001)').fetchone()[0]
    stop()
    start(original_config)
    cli('maintenance', 'disable')
    assert read('mode-source')[0] == raw
    print('PASS cluster owner exclusion, incompatible failed-task recovery and cluster-to-single restart', flush=True)

    cli('maintenance', 'enable')
    wait(drained)
    stop()
    control = control_config('mode-bootstrap')
    start(control, 'control', ready=False)
    assert cli('status')['mode_mismatch']
    committed.with_suffix('.hit').unlink(missing_ok=True)
    committed.touch()
    try:
        try:
            admin.post(url + '/api/v1/cluster/mode', json={'mode': 'cluster'}, timeout=1)
        except requests.Timeout:
            pass
        else:
            raise AssertionError('mode response should be interrupted after commit')
        wait(committed.with_suffix('.hit').exists)
        assert scalar('SELECT deployment_mode FROM mokyu_meta') == 'cluster'
    finally:
        committed.unlink(missing_ok=True)
    assert cli('status')['mode_mismatch']
    cli('status', '--ready', rejected='mode_mismatch')
    stop()
    start(control, 'control')
    wait(drained)
    cli('cluster', 'mode', 'single')
    stop()
    start(original_config)
    cli('maintenance', 'disable')
    assert read('mode-source')[0] == raw
    print('PASS an interrupted committed response leaves a previously restricted control unready until restart', flush=True)
finally:
    marker.unlink(missing_ok=True)
    committed.unlink(missing_ok=True)
    if process and process.poll() is None:
        stop()
    db.close()
