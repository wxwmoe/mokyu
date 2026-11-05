"""An unrelated InnoDB encoding lock must not block a data node's verified repair."""
import os
from concurrent.futures import ThreadPoolExecutor

import pymysql

from tests.recovery.source_repair import Database, isolate, recovery_audit
from tests.storage.source_quarantine import Fixture, wait


def main():
    db, home = Database(), Fixture()
    assert db.type == 'mysql' and home.backend
    payload = os.urandom(32 << 10)
    original, _, _ = isolate(home, db, payload)
    home.put('other', os.urandom(32 << 10))
    other, = [row for row in db.chunks(home) if row['id'] != original['id']]
    encoding = other['current_encoding_id']

    settings = db.settings
    with pymysql.connect(host=settings['host'], port=settings.get('port', 3306), user=settings['user'],
            database=settings['name'], password=settings['password']) as locker, ThreadPoolExecutor(1) as pool:
        gate = home.arm('repair-before-commit')
        try:
            upload = pool.submit(home.put, 'retry', payload)
            wait(lambda: gate.with_suffix('.hit').exists() or upload.done(), 'repair did not reach publication')
            assert not upload.done(), upload.exception()
            with locker.cursor() as cursor:
                cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
                cursor.execute('SELECT unreferenced_at FROM chunk_encodings WHERE encoding_id=%s', (encoding,))
                stamp, = cursor.fetchone()
                cursor.execute("""SELECT encoding_id FROM chunk_encodings FORCE INDEX (encodings_gc)
                    WHERE state='ready' AND unreferenced_at <=> %s AND encoding_id=%s FOR UPDATE""", (stamp, encoding))
                assert cursor.fetchone() == (encoding,)
            gate.unlink(missing_ok=True)
            upload.result(timeout=6)
        finally:
            gate.unlink(missing_ok=True)
            locker.rollback()

    home.read('original', payload)
    home.read('retry', payload)
    recovery_audit(db, original, 's3')
    print('PASS verified repair while another chunk has a locked GC index entry', flush=True)


if __name__ == '__main__':
    main()
