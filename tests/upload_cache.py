"""Durable upload cache and async packing on an explicitly disposable gateway."""
import json
import os
import subprocess
import time
from pathlib import Path

import psycopg
from integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
marker = faults / 'upload-before-work'


def cli(*args):
    return json.loads(subprocess.check_output(command + ['cli', *args], text=True))


def wait(predicate, message):
    for _ in range(600):
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError(message + ': ' + json.dumps(cli('cache', 'status')))


def scalar(sql, params=()):
    return db.execute(sql, params).fetchone()[0]


def pending():
    return scalar('SELECT count(*) FROM pending_uploads')


def hold():
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()


try:
    hold()
    before = cli('status')['backend_puts']
    raw = os.urandom(6 * 1024 * 1024 + 777)
    s3.put_object(Bucket=bucket, Key='pending', Body=raw)
    assert pending() >= 2
    assert cli('status')['backend_puts'] == before
    assert s3.get_object(Bucket=bucket, Key='pending')['Body'].read() == raw
    assert s3.get_object(Bucket=bucket, Key='pending', Range='bytes=4096-8191')['Body'].read() == raw[4096:8192]
    state = cli('cache', 'status')
    assert state['effective_upload_bytes'] == 128 * 1024 * 1024 // 5, state
    assert state['reserved_bytes'] == len(raw), state
    wait(marker.with_suffix('.hit').exists, 'upload worker did not reach barrier')
    for mode in ('metadata', 'head', 'full'):
        job = cli('integrity', 'check', '--mode', mode, '--bucket', bucket, '--key', 'pending')['task_id']
        wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'), 'pending inspection did not finish')
        inspection = cli('task', 'show', job)
        assert inspection['state'] == 'completed' and inspection['detail']['issues'] == 0, inspection
    assert cli('status')['backend_gets'] == 0
    s3.copy_object(Bucket=bucket, Key='copy', CopySource={'Bucket': bucket, 'Key': 'pending'})
    assert scalar('SELECT count(*) FROM cache_pins') == 2 * pending()
    assert cli('cache', 'status')['reserved_bytes'] == len(raw)
    marker.unlink()
    wait(lambda: pending() == 0, 'async packing did not drain')
    assert scalar("SELECT count(*) FROM packs WHERE state='ready'") == 1
    assert scalar("SELECT count(*) FROM chunk_locations WHERE state='ready'") == 0
    assert cli('status')['backend_puts'] == before + 1
    assert cli('cache', 'status')['reserved_bytes'] == 0
    assert s3.get_object(Bucket=bucket, Key='copy')['Body'].read() == raw
    print('PASS local acknowledgement, shared pins, automatic quota and pack-only upload', flush=True)

    hold()
    part = os.urandom(12 * 1024 * 1024)
    upload = s3.create_multipart_upload(Bucket=bucket, Key='timeout')['UploadId']
    result = s3.upload_part(Bucket=bucket, Key='timeout', UploadId=upload, PartNumber=1, Body=part)
    assert pending() > 0
    # Age only the oldest complete CDC segment. New bytes do not renew its deadline.
    db.execute("UPDATE pending_uploads SET created_at=now()-interval '61 seconds',next_retry_at=now()")
    marker.unlink()
    wait(lambda: pending() == 0, 'expired multipart CDC chunks did not drain')
    assert scalar("SELECT count(*) FROM chunk_locations WHERE state='ready'") > 0
    assert scalar('SELECT count(*) FROM fragments') > 0
    s3.complete_multipart_upload(Bucket=bucket, Key='timeout', UploadId=upload,
                                MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': result['ETag']}]})
    assert s3.get_object(Bucket=bucket, Key='timeout')['Body'].read() == part
    wait(lambda: pending() == 0, 'completed multipart pending tail did not drain')
    print('PASS multipart timeout flushes complete CDC chunks and preserves the unfinished tail', flush=True)

    hold()
    other = os.urandom(9 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key='flush', Body=other)
    wait(marker.with_suffix('.hit').exists, 'upload worker did not reach barrier')
    cli('maintenance', 'enable')
    marker.unlink()
    wait(lambda: not scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE kind='upload' AND state='running')"), 'automatic worker did not pause')
    assert pending() > 0
    before_packs = scalar("SELECT count(*) FROM packs WHERE state='ready'")
    job = cli('cache', 'flush')['task_id']
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'), 'flush task did not finish')
    assert cli('task', 'show', job)['state'] == 'completed', cli('task', 'show', job)
    assert pending() == 0
    assert scalar("SELECT count(*) FROM packs WHERE state='ready'") == before_packs
    assert s3.get_object(Bucket=bucket, Key='flush')['Body'].read() == other
    cli('maintenance', 'disable')
    print('PASS explicit maintenance flush creates only independent sources', flush=True)
finally:
    marker.unlink(missing_ok=True)

import requests
web = os.environ['MOKYU_TEST_WEB']
session = requests.Session()
login = session.post(web + '/api/login', headers={'Origin': web}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
login.raise_for_status()
assert session.post(web + '/api/cache/flush').status_code == 403
response = session.post(web + '/api/cache/flush', headers={'Origin': web, 'X-CSRF-Token': login.json()['csrf_token']})
response.raise_for_status()
assert response.json()['task_id']
print('PASS upload cache management authentication and CSRF', flush=True)
