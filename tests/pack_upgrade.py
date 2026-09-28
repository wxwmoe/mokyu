"""Upgrade a running schema-4 gateway; the harness supplies old/new binaries."""
import json
import fcntl
import os
import signal
import subprocess
import time
import tomllib
from pathlib import Path

import boto3
import psycopg
from integration import s3, bucket

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db=psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text(),autocommit=True)
assert db.execute('SELECT schema_version FROM gateway_meta').fetchone()[0]==4
configuration=tomllib.loads(Path(os.environ['MGW_TEST_CONFIG']).read_text())['backend']
backend=boto3.client('s3',endpoint_url=configuration['endpoint'],region_name=configuration['region'],aws_access_key_id=configuration['access_key'],aws_secret_access_key=configuration['secret_key'])
marker={'Bucket':configuration['bucket'],'Key':configuration['prefix']+'/meta.json'}
assert json.loads(backend.get_object(**marker)['Body'].read())['format_version']==1
raw=os.urandom(7*1024*1024+7)
s3.put_object(Bucket=bucket,Key='pre-upgrade',Body=raw)
original=db.execute('SELECT id,storage_id,nonce FROM chunks ORDER BY id').fetchall()
db.execute("SELECT setval('chunks_id_seq',100000)")
pid=int(os.environ['MGW_TEST_GATEWAY_PID'])
os.kill(pid,signal.SIGKILL)
for _ in range(100):
    with (Path(os.environ['MGW_TEST_DATA'])/'gateway.lock').open('rb') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            break
        except BlockingIOError:
            pass
    time.sleep(.1)
command=[os.environ['MGW_TEST_BINARY'],'--config',os.environ['MGW_TEST_CONFIG']]


def cli(*args):
    return json.loads(subprocess.check_output(command+['cli',*args],stderr=subprocess.DEVNULL,text=True))


app=subprocess.Popen(command+['serve','--maintenance'],stdout=open(Path(os.environ['MGW_TEST_RESULTS'])/'pack-upgrade.log','a'),stderr=subprocess.STDOUT)
try:
    for _ in range(160):
        assert app.poll() is None
        try:
            if cli('status')['maintenance']: break
        except subprocess.CalledProcessError: pass
        time.sleep(.1)
    else: raise AssertionError('upgrade startup timeout')
    assert db.execute('SELECT schema_version FROM gateway_meta').fetchone()[0]==7
    assert db.execute('SELECT id,storage_id,nonce FROM chunk_locations ORDER BY id').fetchall()==original
    assert db.execute('SELECT last_value FROM chunk_locations_id_seq').fetchone()[0]>=100000
    assert json.loads(backend.get_object(**marker)['Body'].read())['format_version']==1
    assert s3.get_object(Bucket=bucket,Key='pre-upgrade')['Body'].read()==raw
    cli('maintenance','disable')
    assert json.loads(backend.get_object(**marker)['Body'].read())['format_version']==2
    s3.put_object(Bucket=bucket,Key='post-upgrade',Body=b'new physical identity')
    assert db.execute('SELECT max(encoding_id)>100000 FROM chunks').fetchone()[0]
    job=cli('pack','run')['task_id']
    for _ in range(400):
        state=cli('task','show',job)['state']
        assert state!='failed'
        if state=='completed': break
        time.sleep(.1)
    else: raise AssertionError('upgrade pack timeout')
    assert s3.get_object(Bucket=bucket,Key='pre-upgrade')['Body'].read()==raw
    assert s3.get_object(Bucket=bucket,Key='pre-upgrade',Range='bytes=10-100')['Body'].read()==raw[10:101]
    print('PASS released schema and ciphertext upgrade, sequence high-water mark, maintenance marker deferral and pack conversion')
finally:
    app.terminate();app.wait(timeout=40)
