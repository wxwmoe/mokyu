"""Local fsync failures must not publish replacements or retire old sources.

Run against an isolated single-container service launched with fsync.c preloaded.
MOKYU_TEST_FSYNC_ROOT is a dedicated backend directory (no trailing slash).
MOKYU_TEST_FSYNC_FAULT and MOKYU_TEST_FSYNC_TRACE must be shared with the service.
The test uses public APIs and works with every supported single-mode database.
"""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
root = Path(os.environ['MOKYU_TEST_FSYNC_ROOT'])
fault = Path(os.environ['MOKYU_TEST_FSYNC_FAULT'])
trace = Path(os.environ['MOKYU_TEST_FSYNC_TRACE'])
name = 'durable-' + uuid.uuid4().hex[:12]


def cli(*args, value=None, succeeds=True):
    reply = subprocess.run(command + list(args), input=json.dumps(value) if value else None,
                           capture_output=True, text=True, timeout=30)
    assert (reply.returncode == 0) == succeeds, (args[:2], reply.stderr)
    return json.loads(reply.stdout) if succeeds else None


def wait(predicate, description):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if result := predicate():
            return result
        time.sleep(.1)
    raise AssertionError(description)


def arm(mode):
    trace.write_text('')
    fault.write_text(mode)


def observed(line):
    return trace.exists() and line in trace.read_text().splitlines()


def complete(reply, state='completed'):
    task = reply['task_id']
    row = wait(lambda: (row if (row := cli('task', 'show', task))['state']
                       in ('completed', 'failed', 'paused') else None), 'task did not finish')
    assert row['state'] == state, row


def profile(**changes):
    value = cli('storage', 'profile', 'show', name)
    cli('storage', 'profile', 'set', name, '-', value={**value, **changes})


def read(key, expected):
    with client.get_object(Bucket=name, Key=key)['Body'] as body:
        assert body.read() == expected


try:
    assert cli('status')['role'] == 'single'
    for mode in ('f', 'd'):
        arm(mode)
        failed = name + '-init-' + mode
        cli('storage', 'create', failed, 'local', '--init', '--path', str(root / failed), succeeds=False)
        assert observed(mode + ' 1'), 'initialization did not attempt the failing fsync'
        assert all(row['name'] != failed for row in cli('storage', 'list'))
    fault.unlink()
    trace.write_text('')
    store = cli('storage', 'create', name, 'local', '--init', '--path', str(root / name))
    assert observed('f 0') and observed('d 0'), 'marker was not synced'
    cli('bucket', 'create', name, '--storage', store['id'])
    credentials = cli('cred', 'create', name)
    client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                          aws_access_key_id=credentials['access_key'],
                          aws_secret_access_key=credentials['secret_key'],
                          config=Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 0}))
    profile(upload_cache=False, pack_enabled=False, compression_enabled=False)
    old = os.urandom(4096)
    client.put_object(Bucket=name, Key='replace', Body=old)
    for mode in ('f', 'd'):
        raw = os.urandom(4096)
        arm(mode)
        try:
            client.put_object(Bucket=name, Key='replace', Body=raw)
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] >= 500, error.response
        else:
            raise AssertionError('write acknowledged a failed fsync')
        assert observed(mode + ' 1')
        read('replace', old)
        fault.unlink()
        trace.write_text('')
        client.put_object(Bucket=name, Key='replace', Body=raw)
        assert observed('f 0') and observed('d 0')
        read('replace', raw)
        old = raw
    print('PASS file/directory fsync errors reject initialization and preserve replacement source', flush=True)

    for mode in ('f', 'd'):
        target = name + '-key-' + mode
        cli('key', 'create', target, 'block', '-a', 'aes-256-gcm')
        preview = cli('storage', 'rewrite', name, 'preview', '--key', target)
        arm(mode)
        complete(cli('storage', 'rewrite', name, 'start', '--key', target,
                     '--confirm', preview['confirmation']), state='failed')
        assert observed(mode + ' 1')
        read('replace', old)
        fault.unlink()
        preview = cli('storage', 'rewrite', name, 'preview', '--key', target)
        complete(cli('storage', 'rewrite', name, 'start', '--key', target,
                     '--confirm', preview['confirmation']))
        read('replace', old)
    print('PASS failed rewrite keeps old encodings readable and succeeds after recovery', flush=True)
finally:
    fault.unlink(missing_ok=True)
