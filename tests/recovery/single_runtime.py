"""Single runtime identity, database ownership and recovery in a fresh fixture."""
import base64
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
from tests.integration import bucket, s3

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
path = Path(os.environ['MOKYU_TEST_CONFIG'])
config = tomllib.loads(path.read_text())
data = Path(os.environ['MOKYU_TEST_DATA'])
binary = os.environ['MOKYU_TEST_BINARY']
command = [binary, '--config', str(path)]
url = 'http://127.0.0.1:' + config['instance'].get('listen_control', '127.0.0.1:9002').rsplit(':', 1)[1] + '/api/v1/cli'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
key = config['instance']['deployment_key']
pid_file = Path(os.environ['MOKYU_TEST_PID_FILE'])


def cli(*args):
    result = subprocess.run(command + ['cli', *args], capture_output=True, text=True, timeout=25)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def wait(predicate):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError('runtime operation timed out')


def alive(pid):
    stat = Path(f'/proc/{pid}/stat')
    return stat.exists() and stat.read_text().rsplit(')', 1)[1].split()[0] != 'Z'


def start():
    def unlocked():
        with (data / 'gateway.lock').open('r+') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(unlocked)
    with (data / 'runtime-test.log').open('a') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True,
                                   env={**os.environ, 'MOKYU_TEST_FAULT_DIR': os.environ['MOKYU_TEST_FAULT_DIR']})
    pid_file.write_text(str(process.pid))
    def ready():
        assert process.poll() is None, 'runtime stopped before readiness'
        return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=10).returncode == 0
    wait(ready)


assert not (data / 'node.json').exists()
assert db.execute('SELECT count(*) FROM nodes').fetchone()[0] == 0
status = cli('status', '--ready')
assert not status['node_registered'] and not status['mode_mismatch']
for headers in ({}, {'x-mokyu-deployment-key': base64.urlsafe_b64encode(os.urandom(48)).decode()},
                {'x-mokyu-node-id': '00000000-0000-0000-0000-000000000001', 'x-mokyu-node-credential': key}):
    response = requests.post(url, json={'Status': {'ready': False}}, headers=headers, timeout=15)
    assert response.status_code == 403 and response.json()['error'] == 'Forbidden', response.status_code
cli('maintenance', 'pause', 'range')
event = db.execute("SELECT source,actor_id,actor_label,outcome FROM audit_events WHERE action='maintenance.policy' AND target='range' ORDER BY id DESC LIMIT 1").fetchone()
assert event == ('cli', None, 'Local CLI', 'succeeded'), event
cli('maintenance', 'resume', 'range')
print('PASS single CLI requires the deployment key, keeps CLI audit and creates no node identity', flush=True)

raw = os.urandom(3 * 1024 * 1024 + 1)
s3.put_object(Bucket=bucket, Key='runtime-source', Body=raw)
job = cli('pack', 'run')['task_id']
wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
assert cli('task', 'show', job)['state'] == 'completed'
assert s3.get_object(Bucket=bucket, Key='runtime-source')['Body'].read() == raw
assert db.execute('SELECT count(*) FROM nodes').fetchone()[0] == 0

local_name = 'local-' + uuid.uuid4().hex[:12]
local_storage = cli('storage', 'create', local_name, 'local', '--init', '--key', 'blocks',
                    '--path', str(data / local_name))
cli('bucket', 'create', local_name, '--storage', local_storage['id'])
credentials = cli('cred', 'create', local_name)
local_s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                       aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'])
local_s3.put_object(Bucket=local_name, Key='local-rewrite', Body=raw)
preview = cli('storage', 'rewrite', local_name, 'preview')
assert preview['preview']['loose_chunks'] > 0
job = cli('storage', 'rewrite', local_name, 'start', '--confirm', preview['confirmation'])['task_id']
wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'))
assert cli('task', 'show', job)['state'] == 'completed'
assert local_s3.get_object(Bucket=local_name, Key='local-rewrite')['Body'].read() == raw
print('PASS local storage rewrite completes without a registered node', flush=True)

other_data = data / 'second-runtime'
other_data.mkdir(exist_ok=True)
other_config = data / 'second.toml'
other_config.write_text(path.read_text().replace(str(data), str(other_data)))
try:
    for role in ([], ['control']):
        result = subprocess.run([binary, '--config', str(other_config), *role], capture_output=True, text=True, timeout=30)
        assert result.returncode != 0 and 'incompatible mode' in result.stderr
    print('PASS a different data directory cannot start another single or cluster runtime on the same database', flush=True)
finally:
    other_config.unlink()

owner = db.execute("SELECT pid FROM pg_locks WHERE locktype='advisory' AND mode='ExclusiveLock' "
                   'AND classid=%s AND objid=%s AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())',
                   (734922709851001 >> 32, 734922709851001 & 0xffffffff)).fetchone()[0]
pid = int(pid_file.read_text())
db.execute('SELECT pg_terminate_backend(%s)', (owner,))
wait(lambda: not alive(pid))
start()
assert s3.get_object(Bucket=bucket, Key='runtime-source')['Body'].read() == raw
assert local_s3.get_object(Bucket=local_name, Key='local-rewrite')['Body'].read() == raw
assert not (data / 'node.json').exists()
assert db.execute('SELECT count(*) FROM nodes').fetchone()[0] == 0
print('PASS ownership loss terminates the process; restart and stored bytes do not depend on node.json', flush=True)

pid = int(pid_file.read_text())
os.kill(pid, signal.SIGINT)
wait(lambda: not alive(pid))
owner = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
try:
    assert owner.execute('SELECT pg_try_advisory_lock_shared(734922709851001)').fetchone()[0]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0 and 'incompatible mode' in result.stderr
finally:
    owner.close()
    start()
print('PASS a cluster runtime owner prevents a single runtime from starting', flush=True)
