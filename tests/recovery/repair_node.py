"""A real data process restart preserves other owners and safely expires its orphan.

Run inside the first data container. CLI uses the shared control configuration;
MOKYU_TEST_NODE_CONFIG and GATEWAY_PID_FILE identify only this disposable data node.
"""
import os
import signal
import subprocess
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError
from tests.recovery.repair_cluster import second_client
from tests.recovery.source_repair import Database, blocked, isolate
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    assert database.type == 'postgres' and fixture.cli('status')['role'] == 'control'
    raw = os.urandom(32768)
    original, _, _ = isolate(fixture, database, raw)
    candidate = {}

    def crash():
        candidate.update(database.candidates(fixture)[-1])
        assert candidate['owner_stream'] and candidate['state'] == 'uploading'
        os.kill(int(Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE']).read_text()), signal.SIGKILL)

    pending = blocked(fixture, 'repair-before-commit', lambda: fixture.put('interrupted', raw), crash)
    try:
        pending.result()
    except (BotoCoreError, ClientError):
        pass
    else:
        raise AssertionError('killed data acknowledged the interrupted object')
    config = os.environ['MOKYU_TEST_NODE_CONFIG']
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', config]
    with Path(os.environ['MOKYU_TEST_DATA']).joinpath('repair-restart.log').open('a') as log:
        process = subprocess.Popen(command + ['data'], stdout=log, stderr=log, start_new_session=True)
    Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE']).write_text(str(process.pid))

    def ready():
        assert process.poll() is None, 'restarted data exited'
        return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=10).returncode == 0

    wait(ready, 'data restart did not become ready')
    row = database.query('SELECT state,owner_stream,recovery_pending FROM chunk_encodings WHERE encoding_id=%s', candidate['encoding_id'])[0]
    assert row['state'] == 'uploading' and row['owner_stream'] == candidate['owner_stream'] and row['recovery_pending']
    other = second_client(fixture)
    try:
        other.put_object(Bucket=fixture.name, Key='retry', Body=raw)
        for key in ('original', 'retry'):
            fixture.read(key, raw)
        database.query("UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE stream_id=%s AND lease_type='write'", candidate['owner_stream'])
        database.query("UPDATE streams SET touched_at=now()-interval '2 days' WHERE id=%s", candidate['owner_stream'])

        def cleanup():
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            rows = database.query('SELECT state,owner_stream FROM chunk_encodings WHERE encoding_id=%s', candidate['encoding_id'])
            return not rows or (rows[0]['owner_stream'] is None and rows[0]['state'] in ('retired', 'deleting', 'deleted'))

        wait(cleanup, 'expired crashed candidate retained its owner')
        current = database.query('SELECT current_encoding_id FROM chunks WHERE id=%s', original['id'])[0]
        assert current['current_encoding_id'] not in (original['current_encoding_id'], candidate['encoding_id'])
        fixture.read('original', raw)
    finally:
        other.close()
    print('PASS real data restart preserves live ownership, another node recovers, expired orphan retires')


if __name__ == '__main__':
    main()
