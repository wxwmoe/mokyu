"""Maintenance, multipart reclamation and admin authentication regressions.

Requires an isolated fault-injection build, boto3, requests and psycopg; uses
the same MOKYU_TEST_* environment as cleanup_concurrency.py plus MOKYU_TEST_WEB.
"""
import errno
import json
import os
import pty
import random
import select
import signal
import subprocess
import termios
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import psycopg
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
web = os.environ['MOKYU_TEST_WEB']
prefix = 'regression-' + uuid.uuid4().hex[:12]


def cli(*args, password=None, success=True):
    p = subprocess.run(command + list(args), input=password, capture_output=True, text=True, timeout=20)
    assert (p.returncode == 0) == success, p.stderr
    return json.loads(p.stdout) if success else p.stderr


def scalar(sql, args=()):
    return db.execute(sql, args or None).fetchone()[0]


def wait(fn):
    deadline = time.monotonic() + 20
    while not fn():
        assert time.monotonic() < deadline, 'condition timed out'
        time.sleep(.02)


def arm(name):
    marker = faults / name
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    return marker


def login(user, password):
    session = requests.Session()
    response = session.post(web + '/api/login', headers={'Origin': web},
                            json={'username': user, 'password': password}, timeout=20)
    return session, response


def interactive(args, entries, success):
    pid, fd = pty.fork()
    if pid == 0:
        os.execv(command[0], command + args)
    output = bytearray()
    prompts = [b'Password: ', b'Confirm password: ']
    next_entry = 0
    status = None
    deadline = time.monotonic() + 15
    try:
        while status is None:
            assert time.monotonic() < deadline, 'terminal prompt timed out'
            if select.select([fd], [], [], .05)[0]:
                try:
                    output.extend(os.read(fd, 4096))
                except OSError as exc:
                    assert exc.errno == errno.EIO
            if next_entry < len(entries) and prompts[next_entry] in output:
                # The prompt can be flushed just before echo is disabled.
                wait(lambda: not termios.tcgetattr(fd)[3] & termios.ECHO)
                os.write(fd, entries[next_entry])
                next_entry += 1
            ended, code = os.waitpid(pid, os.WNOHANG)
            if ended:
                status = os.waitstatus_to_exitcode(code)
        assert (status == 0) == success, output.decode(errors='replace')
        assert termios.tcgetattr(fd)[3] & termios.ECHO, 'terminal echo was not restored'
        for value in entries:
            if value != b'\x04':
                assert value.rstrip(b'\n') not in output, 'password was echoed'
    finally:
        if status is None:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        os.close(fd)


# Both user commands share hidden input; failures must not create/update users.
user = prefix + '-tty'
old, new = 'old-test-password-123', 'new-test-password-456'
interactive(['user', 'create', user], [(old + '\n').encode()] * 2, True)
assert login(user, old)[1].status_code == 200
interactive(['user', 'password', user], [(new + '\n').encode()] * 2, True)
assert login(user, old)[1].status_code == 403
assert login(user, new)[1].status_code == 200
interactive(['user', 'password', user], [(old + '\n').encode(), (new + '\n').encode()], False)
assert login(user, new)[1].status_code == 200
interactive(['user', 'create', prefix + '-eof'], [b'\x04'], False)
assert scalar('SELECT count(*) FROM web_users WHERE username=%s', (prefix + '-eof',)) == 0
assert '--password-stdin' in cli('user', 'create', prefix + '-pipe', password=old + '\n', success=False)
cli('user', 'create', prefix + '-pipe', '--password-stdin', password=old + '\n')
assert login(prefix + '-pipe', old)[1].status_code == 200
print('PASS hidden create/reset, mismatch, EOF, echo restoration and stdin input', flush=True)

# Reset/disable wins after password verification, before session insertion.
with ThreadPoolExecutor(max_workers=2) as pool:
    for action in ('password', 'disable', 'delete'):
        user = prefix + '-' + action
        cli('user', 'create', user, '--password-stdin', password=old + '\n')
        existing, response = login(user, old)
        assert response.status_code == 200
        marker = arm('login-verified')
        pending = pool.submit(login, user, old)
        try:
            wait(lambda: marker.with_suffix('.hit').exists())
            args = ['user', action, user]
            if action == 'password':
                args.append('--password-stdin')
            cli(*args, password=new + '\n' if action == 'password' else None)
            assert existing.get(web + '/api/session').status_code == 403
        finally:
            marker.unlink(missing_ok=True)
        assert pending.result()[1].status_code == 403
        assert scalar('SELECT count(*) FROM sessions s JOIN web_users u ON u.id=s.user_id WHERE u.username=%s', (user,)) == 0
        if action == 'password':
            assert login(user, new)[1].status_code == 200

    # Login owns the row first: reset waits and then revokes that new session.
    user = prefix + '-login-first'
    cli('user', 'create', user, '--password-stdin', password=old + '\n')
    marker = arm('login-before-session')
    pending = pool.submit(login, user, old)
    try:
        wait(lambda: marker.with_suffix('.hit').exists())
        resetting = pool.submit(cli, 'user', 'password', user, '--password-stdin', password=new + '\n')
        wait(lambda: scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'UPDATE web_users SET password_hash%')"))
    finally:
        marker.unlink(missing_ok=True)
    session, response = pending.result()
    assert response.status_code == 200
    resetting.result()
    assert session.get(web + '/api/session').status_code == 403
    assert login(user, new)[1].status_code == 200
print('PASS password reset, disable and delete races in both transaction orders', flush=True)


def client(bucket):
    credentials = cli('credential', 'create', bucket)
    return boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                        aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                        config=Config(retries={'max_attempts': 0}, read_timeout=30,
                                      request_checksum_calculation='when_required', response_checksum_validation='when_required'))


# A selected purge must respect maintenance even after the worker picked it.
marker = arm('purge-before-batch')
tasks = []
try:
    for kind in ('objects', 'uploads', 'empty'):
        bucket = prefix + '-' + kind
        bid = cli('bucket', 'create', bucket)['id']
        s3 = client(bucket)
        if kind == 'objects':
            s3.put_object(Bucket=bucket, Key='keep', Body=b'keep this object')
        elif kind == 'uploads':
            upload = s3.create_multipart_upload(Bucket=bucket, Key='pending')['UploadId']
            s3.upload_part(Bucket=bucket, Key='pending', UploadId=upload, PartNumber=1, Body=b'pending')
        task = uuid.uuid4()
        with db.transaction():
            db.execute("UPDATE buckets SET state='purging' WHERE id=%s", (bid,))
            db.execute("INSERT INTO tasks(id,kind,bucket_id,state) VALUES(%s,'purge',%s,'paused')", (task, bid))
        cli('task', 'resume', str(task))
        tasks.append((task, bid))
    wait(lambda: marker.with_suffix('.hit').exists())
    cli('maintenance', 'enable')
    assert cli('status')['maintenance']
finally:
    marker.unlink(missing_ok=True)
for task, bid in tasks:
    assert scalar('SELECT state FROM tasks WHERE id=%s', (task,)) == 'paused'
    assert scalar('SELECT count(*) FROM buckets WHERE id=%s', (bid,)) == 1
    cli('task', 'resume', str(task), success=False)
assert scalar('SELECT count(*) FROM objects WHERE bucket_id=%s AND stream_id IS NOT NULL', (tasks[0][1],)) == 1
assert scalar("SELECT count(*) FROM uploads WHERE bucket_id=%s AND state='active'", (tasks[1][1],)) == 1
preview = cli('backend', 'sweep')['task_id']
wait(lambda: scalar('SELECT state FROM tasks WHERE id=%s', (preview,)) == 'completed')
cli('maintenance', 'disable')
for task, bid in tasks:
    assert scalar('SELECT state FROM tasks WHERE id=%s', (task,)) == 'paused'
    cli('task', 'resume', str(task))
for task, bid in tasks:
    wait(lambda: scalar('SELECT state FROM tasks WHERE id=%s', (task,)) == 'completed')
    assert scalar('SELECT count(*) FROM buckets WHERE id=%s', (bid,)) == 0
print('PASS maintenance pauses all purge stages, allows sweep and requires explicit resume', flush=True)

# Whole-file boundaries replace early right-part chunks, including complete ones.
bucket = prefix + '-multipart'
cli('bucket', 'create', bucket)
s3 = client(bucket)
cli('gc', 'pause')
left = b'L' * (8 * 1024 * 1024 + 100 * 1024)
for shared in (False, True):
    found = False
    for seed in range(8):
        right = random.Random(f'{prefix}-{shared}-{seed}').randbytes(6 * 1024 * 1024)
        key = f'boundary-{shared}-{seed}'
        upload = s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
        s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=2, Body=right)
        sid = scalar('SELECT stream_id FROM parts WHERE upload_id=%s AND part_number=2', (upload,))
        before = db.execute('SELECT e.chunk_id,e.offset_bytes,c.raw_size FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=%s ORDER BY e.offset_bytes', (sid,)).fetchall()
        if shared:
            for chunk, offset, length in before:
                s3.put_object(Bucket=bucket, Key=f'shared-{seed}-{chunk}', Body=right[offset:offset + length])
        s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=1, Body=left)
        remaining = {row[0] for row in db.execute('SELECT chunk_id FROM extents WHERE stream_id=%s', (sid,))}
        replaced = [c for c, _, _ in before if c not in remaining]
        for chunk, _, _ in before:
            mark = scalar('SELECT unreferenced_at FROM chunks WHERE id=%s', (chunk,))
            if chunk in replaced and not shared:
                assert mark is not None, 'fully replaced chunk lacks its GC timestamp'
            else:
                assert mark is None, 'a retained/shared chunk was marked unreferenced'
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
        if shared:
            for chunk, offset, length in before:
                assert s3.get_object(Bucket=bucket, Key=f'shared-{seed}-{chunk}')['Body'].read() == right[offset:offset + length]
        if replaced:
            found = True
            if not shared:
                wait(lambda: scalar('SELECT count(*) FROM extents WHERE chunk_id=ANY(%s)', (replaced,)) == 0)
                db.execute("UPDATE chunks SET unreferenced_at=now()-interval '4 days' WHERE id=ANY(%s)", (replaced,))
                cli('gc', 'resume')
                cli('gc', 'run')
                wait(lambda: scalar("SELECT count(*) FROM chunks WHERE id=ANY(%s) AND state<>'deleted'", (replaced,)) == 0)
                cli('gc', 'pause')
            break
    assert found, 'fixture did not exercise a fully replaced chunk'

# Abort can invalidate a predecessor after its bytes have been encoded for reuse.
key = 'abort-during-seed'
upload = s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=1,
               Body=random.Random(0).randbytes(2 * 1024 * 1024))
first_new_id = scalar('SELECT COALESCE(max(id),0) FROM chunks')
marker = arm('multipart-before-seed')
with ThreadPoolExecutor(max_workers=1) as pool:
    pending = pool.submit(s3.upload_part, Bucket=bucket, Key=key, UploadId=upload,
                          PartNumber=2, Body=b'R' * (5 * 1024 * 1024))
    try:
        wait(lambda: marker.with_suffix('.hit').exists())
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
    finally:
        marker.unlink(missing_ok=True)
    from botocore.exceptions import ClientError
    try:
        pending.result()
        raise AssertionError('aborted upload unexpectedly completed its part')
    except ClientError as exc:
        assert exc.response['Error']['Code'] in ('NoSuchUpload', 'OperationAborted')
assert scalar("SELECT count(*) FROM chunks c WHERE id>%s AND state='ready' AND unreferenced_at IS NULL AND NOT EXISTS(SELECT 1 FROM extents WHERE chunk_id=c.id)", (first_new_id,)) == 0
cli('gc', 'resume')
print('PASS multipart full replacement, partial/shared references, concurrent abort and GC', flush=True)
db.close()
