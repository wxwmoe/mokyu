"""Crash recovery and finite late PUT cleanup in an isolated single runtime."""
import fcntl
import os
import signal
import subprocess
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError
from tests.recovery.source_repair import Database, blocked, isolate, location
from tests.storage.source_quarantine import Fixture, wait


def restart(fixture):
    data = Path(os.environ['MOKYU_TEST_DATA'])
    def unlocked():
        with (data / 'gateway.lock').open('r+') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(unlocked, 'dead process retained its instance lock')
    with (data / 'repair-restart.log').open('a') as log:
        process = subprocess.Popen(fixture.command[:-1], stdout=log, stderr=log, start_new_session=True)
    Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE']).write_text(str(process.pid))
    def ready():
        assert process.poll() is None, 'runtime stopped before readiness'
        return subprocess.run(fixture.command + ['status','--ready'], capture_output=True,timeout=10).returncode == 0
    wait(ready, 'restarted service was not ready')


def main():
    database = Database()
    for point in ('repair-prepared','repair-uploading','repair-stored','repair-before-commit','repair-committed'):
        fixture = Fixture()
        assert fixture.cli('status')['role'] == 'single'
        profile = fixture.cli('storage','profile','show',fixture.name)
        profile.update(upload_retention='1s',unreferenced_grace='1s',min_storage_duration='0s',deleted_chunk_retention='5s')
        fixture.cli('storage','profile','set',fixture.name,'-',value=profile)
        raw = os.urandom(32768)
        original, _, _ = isolate(fixture,database,raw)
        old = b'previous published object'
        fixture.put('replace',old)
        candidate = {}
        def crash():
            rows = database.query('SELECT encoding_id,location_id,state,owner_stream,recovery_pending FROM chunk_encodings WHERE chunk_id=%s ORDER BY encoding_id', original['id'])
            candidate.update(rows[-1])
            assert candidate['encoding_id'] != original['current_encoding_id']
            if point == 'repair-committed':
                assert candidate['state'] == 'ready' and not candidate['recovery_pending']
            else:
                assert candidate['state'] == 'uploading' and candidate['owner_stream']
            os.kill(int(Path(os.environ['MOKYU_TEST_GATEWAY_PID_FILE']).read_text()),signal.SIGKILL)
        future = blocked(fixture,point,lambda:fixture.put('replace',raw),crash)
        try:
            future.result()
        except (BotoCoreError,ClientError):
            pass
        else:
            raise AssertionError('killed upload acknowledged publication')
        restart(fixture)
        fixture.read('replace',old)
        if point != 'repair-committed':
            row = database.query('SELECT state,owner_stream,recovery_pending FROM chunk_encodings WHERE encoding_id=%s',candidate['encoding_id'])[0]
            assert row['owner_stream'] is None and row['state'] in ('retired','deleting','deleted') and row['recovery_pending']
            current = database.query('SELECT current_encoding_id FROM chunks WHERE id=%s',original['id'])[0]
            assert current['current_encoding_id'] == original['current_encoding_id']
        else:
            fixture.read('original',raw)
        if point == 'repair-stored':
            path = location(fixture,candidate['location_id'])
            def first_delete():
                fixture.finished(fixture.cli('maintenance','run','gc'))
                rows = database.query('SELECT state,recovery_pending FROM chunk_encodings WHERE encoding_id=%s',candidate['encoding_id'])
                assert rows, 'uncertain source was forgotten before the retention window'
                return rows[0]['state'] == 'deleted' and rows[0]['recovery_pending']
            wait(first_delete,'failed candidate was not deleted')
            # Simulate a backend PUT that becomes visible after cancellation and the first DELETE.
            fixture.bytes(path,raw)
            assert fixture.bytes(path) == raw
            def final_delete():
                fixture.finished(fixture.cli('maintenance','run','gc'))
                rows = database.query('SELECT state,recovery_pending FROM chunk_encodings WHERE encoding_id=%s',candidate['encoding_id'])
                return not rows or (rows[0]['state']=='deleted' and not rows[0]['recovery_pending'])
            wait(final_delete,'late write was not reconciled',seconds=45)
            assert path not in fixture.keys(), 'candidate metadata was forgotten before the repeated delete'
        fixture.put('retry',raw)
        fixture.read('retry',raw)
        fixture.read('original',raw)
        print('PASS',point,'crash preserves published objects and safely retries',flush=True)
    print('PASS unknown candidate journal survives first DELETE and removes a finite late PUT',flush=True)


if __name__ == '__main__':
    main()
