"""Single-container storage lifecycle through public APIs, without database SQL."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import requests
from tests.integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
storage = os.environ['MOKYU_TEST_STORAGE']
block_key = os.environ['MOKYU_TEST_BLOCK_KEY']
web = os.environ['MOKYU_TEST_WEB']
session = requests.Session()
reply = session.post(web + '/api/v1/auth/login', headers={'Origin': web},
                     json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']}, timeout=20)
reply.raise_for_status()
session.headers.update({'Origin': web, 'X-CSRF-Token': reply.json()['csrf_token']})
marker = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'upload-before-work'
profile_file = Path(os.environ['MOKYU_TEST_DATA']) / 'single-lifecycle-profile.json'
key = 'lifecycle-' + uuid.uuid4().hex
copy = key + '-copy'
raw = os.urandom(7 * 1024 * 1024) + b'compressible-block\n' * 150_000


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def get(path, **params):
    reply = session.get(web + '/api/v1/' + path, params=params, timeout=20)
    reply.raise_for_status()
    return reply.json()


def wait(predicate, description):
    end = time.monotonic() + 120
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.2)
    raise AssertionError(description)


def complete(response):
    job = response['task_id']
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed', 'paused'),
         'task did not finish: ' + job)
    task = cli('task', 'show', job)
    assert task['state'] == 'completed', task
    assert not {'parent_task_id', 'lease_token', 'lease_node_id'} & task.keys()
    assert not {'role', 'children'} & task['detail'].keys()
    return task


def profile(**changes):
    value = cli('storage', 'profile', 'show', storage)
    value.update(changes)
    profile_file.write_text(json.dumps(value))
    cli('storage', 'profile', 'set', storage, str(profile_file))


def read_bytes(name):
    with s3.get_object(Bucket=bucket, Key=name)['Body'] as body:
        assert body.read() == raw
    with s3.get_object(Bucket=bucket, Key=name, Range='bytes=31-8192')['Body'] as body:
        assert body.read() == raw[31:8193]


def chunks():
    page = get(f'buckets/{bucket_id}/objects', q=key, mode='exact', search_in='path', recursive='true')
    item, = page['items']
    assert item['object_key'] == key and page['next_cursor'] is None
    page = get(f'buckets/{bucket_id}/object/chunks', key=key, version=item['id'], limit=200)
    assert page['items'] and page['next_cursor'] is None
    return page['items']


def usage_matches(size, count):
    usage = get('usage/summary', bucket=bucket_id)['usage']
    return usage and int(usage['logical_bytes']) == size and int(usage['objects']) == count


assert cli('status')['role'] == 'single'
assert cli('cache', 'status')['enabled'], 'fixture must enable the local upload cache'
bucket_id = next(row['id'] for row in get('buckets') if row['name'] == bucket)
existing = [item for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket)
            for item in page.get('Contents', [])]
logical = sum(item['Size'] for item in existing)
original = cli('storage', 'profile', 'show', storage)
try:
    wait(lambda: get('usage/runtime')['pending_entries'] == '0', 'fixture has pending uploads')
    profile(upload_cache=True, pack_enabled=False, compression_enabled=False)
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    before = cli('status')['backend_puts']
    s3.put_object(Bucket=bucket, Key=key, Body=raw)
    wait(marker.with_suffix('.hit').exists, 'upload worker did not reach the fault barrier')
    assert int(get('usage/runtime')['pending_entries']) > 0
    assert cli('status')['backend_puts'] == before
    read_bytes(key)
    s3.copy_object(Bucket=bucket, Key=copy, CopySource={'Bucket': bucket, 'Key': key})
    read_bytes(copy)
    inspection = complete(cli('integrity', 'check', '--mode', 'full', '--bucket', bucket, '--key', key))
    assert inspection['detail']['issues'] == 0, inspection
    wait(lambda: usage_matches(logical + 2 * len(raw), len(existing) + 2), 'pending usage did not converge')
    s3.delete_object(Bucket=bucket, Key=copy)
    marker.unlink()
    complete(cli('cache', 'flush'))
    wait(lambda: get('usage/runtime')['pending_entries'] == '0', 'cache did not drain')
    assert cli('cache', 'status')['reserved_bytes'] == 0
    read_bytes(key)
    print('PASS local acknowledgement, pending/copy reads, integrity, usage and durable flush', flush=True)

    # Packing requires exclusive references, including retired copies awaiting cleanup.
    complete(cli('maintenance', 'run', 'cleanup'))
    profile(upload_cache=False, pack_enabled=True, pack_max_size='32MiB')
    complete(cli('pack', 'run'))
    rows = chunks()
    packs = {row['pack_id'] for row in rows}
    assert None not in packs, rows
    for pack in packs:
        detail = get('storage/packs/' + pack, bucket=bucket_id)
        assert detail['pack']['status'] == 'ready' and detail['items']
        objects = get('storage/packs/' + pack + '/objects', bucket=bucket_id)
        assert key in {row['key'] for row in objects['items']}, objects
    read_bytes(key)
    print('PASS explicit packing, scoped members/objects and packed byte ranges', flush=True)

    profile(compression_enabled=True)
    preview = cli('storage', 'rewrite', storage, 'preview', '--key', block_key)
    assert preview['preview']['packed_chunks'] >= len(rows), preview
    complete(cli('storage', 'rewrite', storage, 'start', '--key', block_key,
                 '--confirm', preview['confirmation']))
    preview = cli('storage', 'rewrite', storage, 'preview', '--key', block_key)['preview']
    assert preview['loose_chunks'] == 0 and preview['packed_chunks'] == 0, preview
    read_bytes(key)
    inspection = complete(cli('integrity', 'check', '--mode', 'full', '--bucket', bucket, '--key', key))
    assert inspection['detail']['issues'] == 0, inspection
    reply = session.get(web + '/api/v1/tasks/' + inspection['id'] + '/report', timeout=20)
    reply.raise_for_status()
    report = [json.loads(line) for line in reply.iter_lines()]
    assert report[0]['task']['id'] == inspection['id'] and report[-1]['type'] == 'end'
    print('PASS compression policy rewrite, encrypted bytes and direct integrity report', flush=True)

    profile(pack_enabled=False)
    for pack in {row['pack_id'] for row in chunks()}:
        complete(cli('pack', 'unpack', pack, '--execute'))
    assert all(row['pack_id'] is None and row['source'] == 'chunk' for row in chunks())
    read_bytes(key)
    s3.delete_object(Bucket=bucket, Key=key)
    wait(lambda: usage_matches(logical, len(existing)), 'deleted usage did not converge')
    complete(cli('maintenance', 'run', 'cleanup'))
    print('PASS unpack, independent encrypted reads, deletion, usage and cleanup', flush=True)
finally:
    marker.unlink(missing_ok=True)
    original.pop('revision', None)
    profile(**original)
    profile_file.unlink(missing_ok=True)
    s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key}, {'Key': copy}]})
