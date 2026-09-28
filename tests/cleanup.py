"""Retention checks against an isolated gateway/database, never a real deployment.

Uses integration.py settings plus MOKYU_TEST_BINARY, MOKYU_TEST_CONFIG and
MOKYU_TEST_DATABASE_FILE. Configure cleanup interval=1h, batch_size=3,
max_duration=1s, multipart idle_timeout=72h; leave history retention defaults.
"""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db_url = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip()
db = psycopg.connect(db_url, autocommit=True)
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(retries={'max_attempts': 0}, s3={'addressing_style': 'path'},
                                request_checksum_calculation='when_required', response_checksum_validation='when_required'))


def cli(*args, check=True):
    p = subprocess.run([os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli', *args],
                       capture_output=True, text=True, timeout=15)
    if check:
        assert p.returncode == 0, p.stderr
        return json.loads(p.stdout)
    return p


def scalar(sql, params=()):
    return db.execute(sql, params).fetchone()[0]


def upload(key, body, complete=False):
    uid = s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
    part = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=1, Body=body)
    manifest = {'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]}
    if complete:
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=uid, MultipartUpload=manifest)
    return uid, manifest


cli('gc', 'pause')
policy = cli('cleanup', 'status')
assert policy['deleted_chunk_retention'] == '7d' and policy['upload_retention'] == '24h'
assert policy['task_retention'] == '30d' and policy['batch_size'] == 3 and policy['max_duration'] == '1s'
body = os.urandom(512 * 1024)
finished, manifest = upload('cleanup/finished', body, True)
active, _ = upload('cleanup/active', body)
aborted, _ = upload('cleanup/aborted', body)
s3.abort_multipart_upload(Bucket=bucket, Key='cleanup/aborted', UploadId=aborted)
db.execute("UPDATE uploads SET created_at=now()-interval '100 days' WHERE id=%s", (finished,))
fingerprints = db.execute('SELECT * FROM key_fingerprints ORDER BY key_id').fetchall()

chunk_ids = []
for state, age in [('deleted', 8), ('deleted', 1), ('ready', 99), ('failed', 99), ('deleting', 99), ('preparing', 99), ('uploading', 99)]:
    chunk_ids.append(scalar("""INSERT INTO chunks(storage_id,hash,raw_size,algorithm,key_id,state,created_at,deleted_at)
        VALUES(%s,%s,1,'none','',%s,now()-interval '100 days',now()-%s*interval '1 day') RETURNING id""",
        (uuid.uuid4(), os.urandom(32), state, age)))
task_ids = []
for state, age in [('completed', 31), ('completed', 1), ('paused', 99), ('failed', 99), ('running', 99), ('queued', 99)]:
    tid = uuid.uuid4()
    db.execute("INSERT INTO tasks(id,kind,state,updated_at) VALUES(%s,'sweep',%s,now()-%s*interval '1 day')", (tid, state, age))
    task_ids.append(tid)
user = scalar("SELECT id FROM web_users WHERE username='tester'")
expired, valid = os.urandom(32), os.urandom(32)
for token, age in [(expired, -1), (valid, 1)]:
    db.execute("INSERT INTO sessions(token_hash,user_id,csrf_hash,expires_at) VALUES(%s,%s,%s,now()+%s*interval '1 day')", (token, user, os.urandom(32), age))
high_id = scalar('SELECT last_value FROM chunks_id_seq')

# A referenced row must survive even if its deletion state is inconsistent.
referenced = scalar("SELECT e.chunk_id FROM uploads u JOIN extents e ON e.stream_id=u.output_stream WHERE u.id=%s LIMIT 1", (finished,))
db.execute("UPDATE chunks SET state='deleted',deleted_at=now()-interval '8 days' WHERE id=%s", (referenced,))
report = cli('cleanup', 'run')
assert report['last_run']['last_error'] is None
assert scalar('SELECT count(*) FROM chunks WHERE id=%s', (referenced,)) == 1
db.execute("UPDATE chunks SET state='ready',deleted_at=NULL WHERE id=%s", (referenced,))
assert not scalar('SELECT EXISTS(SELECT 1 FROM chunks WHERE id=%s)', (chunk_ids[0],))
assert scalar('SELECT count(*) FROM chunks WHERE id=ANY(%s)', (chunk_ids[1:],)) == 6
assert not scalar('SELECT EXISTS(SELECT 1 FROM tasks WHERE id=%s)', (task_ids[0],))
assert scalar('SELECT count(*) FROM tasks WHERE id=ANY(%s)', (task_ids[1:],)) == 5
assert not scalar('SELECT EXISTS(SELECT 1 FROM sessions WHERE token_hash=%s)', (expired,))
assert scalar('SELECT EXISTS(SELECT 1 FROM sessions WHERE token_hash=%s)', (valid,))
assert scalar('SELECT last_value FROM chunks_id_seq') == high_id
assert db.execute('SELECT * FROM key_fingerprints ORDER BY key_id').fetchall() == fingerprints
assert scalar('SELECT count(*) FROM uploads WHERE id=ANY(%s::uuid[])', ([finished, active, aborted],)) == 3
s3.complete_multipart_upload(Bucket=bucket, Key='cleanup/finished', UploadId=finished, MultipartUpload=manifest)
assert s3.get_object(Bucket=bucket, Key='cleanup/finished')['Body'].read() == body
print('PASS state/reference guards, deletion timestamp, session expiry, sequence and key fingerprints', flush=True)

# Terminal retention is independent of the 72h active-upload timeout.
db.execute("UPDATE uploads SET touched_at=now()-interval '25 hours' WHERE id=ANY(%s::uuid[])", ([finished, active, aborted],))
cli('cleanup', 'run')
assert scalar('SELECT count(*) FROM uploads WHERE id=ANY(%s::uuid[])', ([finished, aborted],)) == 0
assert scalar('SELECT state FROM uploads WHERE id=%s', (active,)) == 'active'
try:
    s3.complete_multipart_upload(Bucket=bucket, Key='cleanup/finished', UploadId=finished, MultipartUpload=manifest)
except ClientError as error:
    assert error.response['Error']['Code'] == 'NoSuchUpload'
else:
    raise AssertionError('expired Complete retry journal must be absent')
assert s3.get_object(Bucket=bucket, Key='cleanup/finished')['Body'].read() == body
print('PASS Complete retry retention expires independently of object data and active uploads', flush=True)

# Audit real statement sizes and slow each batch enough to exercise the deadline.
db.execute('CREATE TABLE cleanup_test_audit (n bigint)')
db.execute("""CREATE FUNCTION cleanup_test_count() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN INSERT INTO cleanup_test_audit SELECT count(*) FROM removed;
    PERFORM pg_sleep(0.03); RETURN NULL; END $$""")
db.execute('CREATE TRIGGER cleanup_test AFTER DELETE ON tasks REFERENCING OLD TABLE AS removed FOR EACH STATEMENT EXECUTE FUNCTION cleanup_test_count()')
db.execute("INSERT INTO tasks(id,kind,state,updated_at) SELECT gen_random_uuid(),'sweep','completed',now()-interval '40 days' FROM generate_series(1,137)")
report = cli('cleanup', 'run')['last_run']
assert report['budget_exhausted'] and report['last_error'] is None, report
assert report['duration_ms'] < 4000, report
assert 0 < scalar('SELECT max(n) FROM cleanup_test_audit') <= 3
remaining = scalar("SELECT count(*) FROM tasks WHERE state='completed' AND updated_at<now()-interval '30 days'")
assert 0 < remaining < 137, remaining
db.execute('DROP TRIGGER cleanup_test ON tasks')
for _ in range(10):
    cli('cleanup', 'run')
    if scalar("SELECT count(*) FROM tasks WHERE state='completed' AND updated_at<now()-interval '30 days'") == 0:
        break
else:
    raise AssertionError('bounded batches did not resume to completion')
db.execute('DROP FUNCTION cleanup_test_count()')
db.execute('DROP TABLE cleanup_test_audit')
print('PASS batch limit, time budget, rollback of unfinished batch and resumable progress', flush=True)

# Lock failure is visible and does not prevent reads or a later cleanup run.
with psycopg.connect(db_url) as blocker:
    blocker.execute('LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE')
    assert cli('cleanup', 'run', check=False).returncode != 0
    assert cli('cleanup', 'status')['last_run']['last_error'] == 'cleanup_failed'
    assert s3.get_object(Bucket=bucket, Key='cleanup/finished')['Body'].read() == body
assert cli('cleanup', 'run')['last_run']['last_error'] is None
s3.put_object(Bucket=bucket, Key='cleanup/new', Body=os.urandom(300000))
assert scalar('SELECT last_value FROM chunks_id_seq') > high_id
print('PASS lock failure reporting, retry and continuing reads/writes', flush=True)
db.close()
