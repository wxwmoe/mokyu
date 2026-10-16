"""Shared multipart sources survive loss of data and publication by a worker."""
import base64
import hashlib
import json
import os
import time
import uuid
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
fixture = json.loads(Path(os.environ['MOKYU_TEST_CLUSTER_FIXTURE']).read_text())
phase = os.environ['MOKYU_TEST_CLUSTER_PHASE']
db = psycopg.connect(Path(fixture['dsn']).read_text(), autocommit=True)
credentials = json.loads(Path(fixture['credentials']).read_text())
s3 = boto3.client('s3', endpoint_url=fixture['data'], region_name='us-east-1',
                  aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, request_checksum_calculation='when_required'))
marker = Path('/data/cluster/data/faults/upload-before-work')
checkpoint = Path('/data/cluster-staging.json')


def wait(predicate):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.1)
    raise AssertionError('shared staging recovery timed out')


if phase == 'prepare':
    assert db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0] == 0
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    key = 'cluster-staging-' + uuid.uuid4().hex
    raw = [os.urandom(7 * 1024 * 1024 + 17), os.urandom(128 * 1024)]
    upload = s3.create_multipart_upload(Bucket=fixture['bucket'], Key=key)['UploadId']
    parts = []
    for number, data in enumerate(raw, 1):
        reply = s3.upload_part(Bucket=fixture['bucket'], Key=key, UploadId=upload, PartNumber=number, Body=data)
        parts.append({'PartNumber': number, 'ETag': reply['ETag']})
    db.execute('UPDATE pending_uploads SET next_retry_at=now() WHERE owner_task IS NULL')
    wait(marker.with_suffix('.hit').exists)
    assert db.execute("SELECT count(*) FROM staging_objects WHERE object_key LIKE '__mokyu/multipart/%'").fetchone()[0] > 0
    assert db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0] > 0
    combined = b''.join(raw)
    checkpoint.write_text(json.dumps({'key': key, 'upload': upload, 'parts': parts,
                                     'digest': hashlib.sha256(combined).hexdigest(),
                                     'range': base64.b64encode(combined[31:4098]).decode()}))
    print('PASS multipart parts acknowledged with data task held before shared publication', flush=True)
elif phase == 'recover':
    # The harness stops data before this phase and starts worker afterward.
    tasks = db.execute("UPDATE tasks SET lease_expires_at=now()-interval '1 second' "
                       "WHERE state='running' AND task_type='upload' AND lease_node_id IN "
                       "(SELECT id FROM nodes WHERE role='data') RETURNING id").fetchall()
    assert tasks
    state = json.loads(checkpoint.read_text())
    state['tasks'] = [str(row[0]) for row in tasks]
    checkpoint.write_text(json.dumps(state))
    marker.unlink()
    print('Expired interrupted data executions for isolated worker recovery', flush=True)
elif phase == 'drained':
    # Data remains stopped: only worker can publish these shared pending sources.
    wait(lambda: db.execute('SELECT count(*) FROM pending_uploads').fetchone()[0] == 0)
    state = json.loads(checkpoint.read_text())
    assert db.execute("SELECT bool_and(state='completed') FROM tasks WHERE id=ANY(%s::uuid[])", (state['tasks'],)).fetchone()[0]
    print('PASS worker alone recovered and published the interrupted shared pending sources', flush=True)
elif phase == 'finish':
    state = json.loads(checkpoint.read_text())
    s3.complete_multipart_upload(Bucket=fixture['bucket'], Key=state['key'], UploadId=state['upload'],
                                MultipartUpload={'Parts': state['parts']})
    raw = s3.get_object(Bucket=fixture['bucket'], Key=state['key'])['Body'].read()
    assert hashlib.sha256(raw).hexdigest() == state['digest']
    actual = s3.get_object(Bucket=fixture['bucket'], Key=state['key'], Range='bytes=31-4097')['Body'].read()
    assert actual == base64.b64decode(state['range'])
    s3.delete_object(Bucket=fixture['bucket'], Key=state['key'])
    print('PASS multipart completes after data restart with full and Range bytes intact', flush=True)
else:
    raise ValueError('unknown shared staging phase')
