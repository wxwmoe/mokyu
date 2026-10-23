"""Upload ownership, notification fallback, pause fencing and bounded idle work."""
import json
import os
import time
import uuid
from pathlib import Path

import boto3

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    assert fixture.backend and database.type == 'postgres'
    roots = [Path(value) for value in os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')]
    nodes = [uuid.UUID(json.loads((root.parent / 'data/node.json').read_text())['node_id']) for root in roots]
    roles = database.query('SELECT role FROM nodes WHERE enabled')
    assert sum(row['role'] == 'data' for row in roles) >= 2
    assert any(row['role'] == 'worker' for row in roles)
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_SECOND_DATA'], region_name='us-east-1',
        aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key=fixture.credentials['secret_key'],
        config=fixture.client.meta.config)
    storage = uuid.UUID(fixture.store['id'])
    markers = []

    def arm(node, name):
        path = roots[node] / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.with_suffix('.hit').unlink(missing_ok=True)
        path.touch()
        markers.append(path)
        return path

    def pending(node):
        return database.query('SELECT count(*) AS count FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id '
            'WHERE c.storage_id=%s AND p.node_id=%s', storage, nodes[node])[0]['count']

    def listeners():
        return database.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND query LIKE 'LISTEN %%mokyu_tasks%%'")

    try:
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=True, pack_enabled=False, pack_range_enabled=False, compression_enabled=False,
            cache_enabled=True, upload_cache=True, write_flush_interval='0s')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        held = arm(0, 'upload-before-prepare')
        payloads = [os.urandom(32768) for _ in range(17)]
        fixture.put('held', payloads[0])
        wait(held.with_suffix('.hit').exists, 'owner upload did not pause')
        peer.put_object(Bucket=fixture.name, Key='peer', Body=b'independent owner')
        wait(lambda: pending(1) == 0, 'another owner was blocked by the held upload')
        assert pending(0) == 1
        fixture.read('held', payloads[0])
        for number, payload in enumerate(payloads[1:]):
            fixture.put('queued-' + str(number), payload)
        assert pending(0) == len(payloads)
        connected = wait(listeners, 'task listeners were not connected')
        for row in connected:
            database.query('SELECT pg_terminate_backend(%s)', row['pid'])
        held.unlink()
        wait(lambda: pending(0) == 0, 'periodic fallback did not drain after listener loss', seconds=30)
        wait(lambda: len(listeners()) >= len(connected), 'task listeners did not reconnect', seconds=30)
        for number, payload in enumerate(payloads[1:]):
            with peer.get_object(Bucket=fixture.name, Key='queued-' + str(number))['Body'] as body:
                assert body.read() == payload
        tasks = database.query("SELECT t.state,t.execution_scope,t.target_node_id FROM tasks t "
            "JOIN chunks c ON c.id=(t.detail->>'first')::uuid WHERE t.task_type='upload' AND c.storage_id=%s", storage)
        assert len(tasks) == len(payloads) + 1, tasks
        assert all(t['execution_scope'] == 'node' and t['target_node_id'] in nodes for t in tasks), tasks
        print('PASS held node ownership, peer progress, listener reconnect and durable readback', flush=True)

        held = arm(0, 'upload-before-work')
        value = os.urandom(32768)
        fixture.put('paused', value)
        wait(held.with_suffix('.hit').exists, 'upload batch did not pause before work')
        task = database.query('SELECT t.id,t.lease_token FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id '
            'JOIN tasks t ON t.id=p.owner_task WHERE c.storage_id=%s AND p.node_id=%s', storage, nodes[0])[0]
        finished = arm(0, 'task-finished-' + str(task['lease_token']))
        fixture.cli('task', 'pause', str(task['id']))
        held.unlink()
        wait(finished.with_suffix('.hit').exists, 'paused execution did not finish its fenced attempt')
        assert database.query('SELECT state FROM tasks WHERE id=%s', task['id'])[0]['state'] == 'paused'
        assert pending(0) == 1
        finished.unlink()
        fixture.cli('task', 'resume', str(task['id']))
        wait(lambda: pending(0) == 0, 'resumed upload did not persist')
        with peer.get_object(Bucket=fixture.name, Key='paused')['Body'] as body:
            assert body.read() == value
        print('PASS paused execution cannot publish and a fresh execution can resume', flush=True)

        def transactions():
            return database.query('SELECT xact_commit+xact_rollback AS count FROM pg_stat_database '
                'WHERE datname=current_database()')[0]['count']

        time.sleep(1)
        before = transactions()
        time.sleep(6)
        delta = transactions() - before
        # Includes heartbeats and profile polls from three executors with one worker per class.
        # This coarse guard detects busy loops; comparative CPU and latency use separate benchmarks.
        assert delta <= 1200, f'idle task loops generated {delta} transactions in six seconds'
        print('PASS bounded idle database work:', delta, flush=True)
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
