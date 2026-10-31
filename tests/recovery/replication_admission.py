"""A control with a stale maintenance snapshot defers replication until refresh."""
import os
from pathlib import Path
import signal
import subprocess
import uuid

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def run(primary, secondary):
    fixture, database = Fixture(), Database()
    assert database.type == 'postgres'
    assert primary['config'].encode() in (Path('/proc') / str(primary['pid']) / 'cmdline').read_bytes()
    assert secondary['config'].encode() in (Path('/proc') / str(secondary['pid']) / 'cmdline').read_bytes()
    fixture.put('admission/source', b'maintenance snapshot')
    marker = Path(secondary['faults']) / 'maintenance-refresh'
    claimed = Path(primary['faults']) / 'control-task-claimed'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', secondary['config'], 'cli']
    reply = subprocess.run([*command, 'maintenance', 'enable'], capture_output=True, timeout=30)
    assert reply.returncode == 0, 'secondary maintenance enable failed'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    paused = backup_paused = False
    try:
        wait(marker.with_suffix('.hit').exists, 'secondary refresh did not pause', seconds=30)
        os.kill(secondary['pid'], signal.SIGSTOP)
        backup_paused = True
        fixture.cli('maintenance', 'disable')
        claimed.with_suffix('.hit').unlink(missing_ok=True)
        claimed.touch()
        task = fixture.api('/api/v1/replications', dict(source_bucket=fixture.bucket_id,
            target_bucket=fixture.bucket_id, operation='copy', scope=dict(selection='objects', keys=['admission/source']),
            target_prefix='out/', conflict='report', permissions='inherit'), expected=202)['task_id']
        wait(claimed.with_suffix('.hit').exists, 'primary did not claim the replication')
        os.kill(primary['pid'], signal.SIGSTOP)
        paused = True
        expired = database.query("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE id=%s AND lease_node_id=%s AND state='running' RETURNING id",
            uuid.UUID(task), uuid.UUID(primary['node_id']))
        assert len(expired) == 1, 'controlled expiry must affect only the paused primary task'
        os.kill(secondary['pid'], signal.SIGCONT)
        backup_paused = False
        def taken_over():
            rows = database.query('SELECT lease_node_id,state,error FROM tasks WHERE id=%s', uuid.UUID(task))
            assert rows[0]['state'] != 'failed', rows
            return str(rows[0]['lease_node_id']) == secondary['node_id']
        wait(taken_over, 'secondary did not take the expired task lease', seconds=120)
        marker.unlink(missing_ok=True)
        def complete():
            rows = database.query('SELECT state,error FROM tasks WHERE id=%s', uuid.UUID(task))
            assert rows[0]['state'] != 'failed', rows
            return rows[0]['state'] == 'completed'
        wait(complete, 'replication did not resume after maintenance refresh', seconds=35)
        os.kill(primary['pid'], signal.SIGCONT)
        paused = False
        claimed.unlink(missing_ok=True)
        fixture.read('out/admission/source', b'maintenance snapshot')
        return ['stale control maintenance state defers a controlled-expiry takeover and resumes without failing or losing the task']
    finally:
        marker.unlink(missing_ok=True)
        claimed.unlink(missing_ok=True)
        if paused:
            os.kill(primary['pid'], signal.SIGCONT)
        if backup_paused:
            os.kill(secondary['pid'], signal.SIGCONT)
        fixture.cli('maintenance', 'disable')
