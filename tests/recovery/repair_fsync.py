"""Repair publication requires both local file and directory fsync."""
import os
from pathlib import Path

from tests.recovery.source_repair import Database, blocked, isolate, original_unchanged
from tests.storage.source_quarantine import Fixture, rejected


def main():
    fixture, database = Fixture(), Database()
    assert not fixture.backend and fixture.cli('status')['role']=='single'
    fault, trace = Path(os.environ['MOKYU_TEST_FSYNC_FAULT']),Path(os.environ['MOKYU_TEST_FSYNC_TRACE'])
    raw = os.urandom(32768)
    original, _, _ = isolate(fixture,database,raw)
    old = b'previous durable object'
    fixture.put('replace',old)
    try:
        for mode in ('f','d'):
            def fail():
                trace.write_text('')
                fault.write_text(mode)
            future = blocked(fixture,'repair-uploading',lambda:fixture.put('replace',raw),fail)
            rejected(lambda:future.result())
            assert mode+' 1' in trace.read_text().splitlines()
            original_unchanged(fixture,database,original)
            fixture.read('replace',old)
            fault.unlink()
        fixture.put('retry',raw)
        fixture.read('retry',raw)
        fixture.read('original',raw)
        print('PASS repair rejects file/directory fsync errors and succeeds after durable retry',flush=True)
    finally:
        fault.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
