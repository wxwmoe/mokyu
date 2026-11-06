"""A raw publisher must leave its stream available to a competing object claim."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    db, home = Database(), Fixture()
    assert db.type == 'postgres' and home.backend
    profile = home.cli('storage', 'profile', 'show', home.name)
    home.cli('storage', 'profile', 'set', home.name, '-', value={**profile, 'cdc_enabled': False, 'pack_range_enabled': False})
    home.faults = Path(os.environ['MOKYU_TEST_DATA_FAULT_DIR'])
    gate = home.arm('node-raw-uploaded')
    payload = os.urandom(8192)
    config = db.settings
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(home.put, 'contested', payload)
        try:
            wait(lambda: gate.with_suffix('.hit').exists() or first.done(), 'raw PUT did not prepare')
            assert not first.done(), first.exception()
            with psycopg.connect(host=config['host'], port=config.get('port', 5432), user=config['user'],
                                 password=config['password'], dbname=config['name']) as claim:
                stream, = claim.execute("SELECT write_token FROM objects WHERE bucket_id=%s AND object_key='contested' FOR UPDATE",
                                        (uuid.UUID(home.bucket_id),)).fetchone()
                gate.unlink()
                wait(lambda: db.query("SELECT 1 FROM pg_stat_activity WHERE %s=ANY(pg_blocking_pids(pid)) AND query ILIKE '%%objects%%'",
                                      claim.info.backend_pid), 'publisher did not reach the object')
                # Replacing this token checks a foreign key on the old stream inside the quota trigger.
                assert claim.execute('SELECT 1 FROM streams WHERE id=%s FOR KEY SHARE NOWAIT', (stream,)).fetchone() == (1,)
        finally:
            gate.unlink(missing_ok=True)
        first.result(timeout=40)
    home.read('contested', payload)
    print('PASS object claim can verify the old stream while raw publication waits', flush=True)


if __name__ == '__main__':
    main()
