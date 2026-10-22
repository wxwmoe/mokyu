"""Cluster maintenance convergence, refresh races and shutdown boundaries.

Uses MOKYU_TEST_CLUSTER_FIXTURE with controls/data arrays of {url, state, faults},
dsn, credentials, and checkpoint paths. Each control's state is its node.json.
Phases: enable; restart one data externally; resume; failure; race; drain.
For drain, the harness signals control[0] after checkpoint + '.stop' appears.
All five roles must have fault-injection enabled and shared fault directories.
"""
import http.client
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

import boto3
import psycopg
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
fixture = json.loads(Path(os.environ['MOKYU_TEST_CLUSTER_FIXTURE']).read_text())
phase = os.environ['MOKYU_TEST_CLUSTER_PHASE']
db = psycopg.connect(Path(fixture['dsn']).read_text(), autocommit=True)
credentials = json.loads(Path(fixture['credentials']).read_text())
controls, data = fixture['controls'], fixture['data']
checkpoint = Path(fixture['checkpoint'])


def wait(predicate, description, seconds=45):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if value := predicate():
            return value
        time.sleep(.15)
    raise AssertionError(description)


def headers(node):
    state = json.loads(Path(node['state']).read_text())
    return {'x-mokyu-node-id': state['node_id'], 'x-mokyu-node-credential': state['credential']}


def toggle(index, enabled):
    reply = requests.post(controls[index]['url'] + '/api/v1/cli', headers=headers(controls[index]),
                          json={'Maintenance': 'Enable' if enabled else 'Disable'}, timeout=30)
    assert reply.status_code == 200, (reply.status_code, reply.text)


clients = [boto3.client('s3', endpoint_url=node['url'], region_name='us-east-1',
                       aws_access_key_id=credentials['access_key'],
                       aws_secret_access_key=credentials['secret_key'],
                       config=Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 0}))
           for node in data]


def writing(enabled):
    for client in clients:
        key = 'maintenance-' + uuid.uuid4().hex
        try:
            client.put_object(Bucket=credentials['bucket'], Key=key, Body=b'maintenance check')
        except ClientError as error:
            if not enabled and error.response['Error']['Code'] == 'ServiceUnavailable':
                continue
            return False
        if not enabled:
            client.delete_object(Bucket=credentials['bucket'], Key=key)
            return False
        with client.get_object(Bucket=credentials['bucket'], Key=key)['Body'] as body:
            assert body.read() == b'maintenance check'
        client.delete_object(Bucket=credentials['bucket'], Key=key)
    return True


def reports(work, after):
    rows = db.execute('SELECT r.status->>\'work\',r.received_at FROM node_reports r '
                      'JOIN nodes n ON n.id=r.node_id WHERE n.enabled').fetchall()
    return len(rows) == len(controls) + len(data) + 1 and all(
        value == work and stamp > after for value, stamp in rows)


def converged(enabled):
    start, = db.execute('SELECT now()').fetchone()
    wait(lambda: reports('maintenance' if enabled else 'normal', start), 'node reports did not converge')
    wait(lambda: writing(not enabled), 'data write admission did not converge')


if phase == 'enable':
    toggle(0, False)
    wait(lambda: writing(True), 'initial writes unavailable')
    toggle(0, True)
    converged(True)
    print('PASS both controls, both data and worker observe enabled maintenance', flush=True)
elif phase == 'resume':
    assert writing(False), 'data restarted in maintenance accepted a write'
    toggle(1, False)
    converged(False)
    toggle(1, True)
    converged(True)
    toggle(0, False)
    converged(False)
    print('PASS maintenance-started data resumes; either control can toggle all roles', flush=True)
elif phase == 'failure':
    toggle(0, True)
    converged(True)
    start, = db.execute('SELECT now()').fetchone()
    locker = psycopg.connect(Path(fixture['dsn']).read_text())
    try:
        locker.execute('LOCK TABLE mokyu_meta IN ACCESS EXCLUSIVE MODE')
        query = ('SELECT gc_paused,maintenance,pack_creation_paused,'
                 'deployment_mode,deployment_mode_revision FROM mokyu_meta')
        def blocked():
            rows = db.execute("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                              "AND query=%s AND wait_event_type='Lock'", (query,)).fetchall()
            return rows if len(rows) == len(controls) + len(data) + 1 else None
        pids = wait(blocked, 'not every runtime query reached the database lock', seconds=20)
        for pid, in pids:
            assert db.execute('SELECT pg_cancel_backend(%s)', (pid,)).fetchone() == (True,)
        locker.rollback()
        wait(lambda: reports('maintenance', start), 'failed refresh cleared maintenance')
        assert writing(False)
    finally:
        locker.rollback()
        locker.close()
    toggle(1, False)
    converged(False)
    print('PASS canceled database reads retain maintenance and recover on the next refresh', flush=True)
elif phase == 'race':
    marker = Path(controls[0]['faults']) / 'maintenance-refresh'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    try:
        wait(marker.with_suffix('.hit').exists, 'refresh did not reach snapshot barrier')
        with ThreadPoolExecutor(max_workers=1) as pool:
            command = pool.submit(toggle, 0, True)
            try:
                time.sleep(.3)
                assert not command.done(), 'local toggle bypassed refresh coordination'
            finally:
                marker.unlink(missing_ok=True)
            command.result(timeout=30)
        converged(True)
    finally:
        marker.unlink(missing_ok=True)
        marker.with_suffix('.hit').unlink(missing_ok=True)
        toggle(1, False)
    print('PASS delayed old snapshot cannot overwrite a newer local toggle', flush=True)
elif phase == 'drain':
    toggle(0, False)
    converged(False)
    node = controls[0]
    marker = Path(node['faults']) / 'runtime-draining'
    stop = Path(str(checkpoint) + '.stop')
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    parsed = urlsplit(node['url'])
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=20)
    request_headers = {**headers(node), 'Content-Type': 'application/json'}
    try:
        connection.request('POST', '/api/v1/cli', json.dumps({'Status': {'ready': False}}), request_headers)
        response = connection.getresponse()
        assert response.status == 200
        response.read()
        stop.touch()
        wait(marker.with_suffix('.hit').exists, 'harness did not signal control', seconds=60)
        try:
            connection.request('POST', '/api/v1/cli', json.dumps({'Maintenance': 'Disable'}), request_headers)
            response = connection.getresponse()
            assert response.status == 503, (response.status, response.read())
            response.read()
        except (http.client.RemoteDisconnected, ConnectionError):
            pass
        assert db.execute("SELECT maintenance FROM mokyu_meta WHERE singleton").fetchone() == (False,)
    finally:
        connection.close()
        marker.unlink(missing_ok=True)
        marker.with_suffix('.hit').unlink(missing_ok=True)
        stop.unlink(missing_ok=True)
    print('PASS late keepalive management request cannot reopen a draining process', flush=True)
else:
    raise AssertionError('unknown maintenance test phase')
