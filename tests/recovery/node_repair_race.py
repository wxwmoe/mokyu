"""Two API data nodes publish overlapping repairs and keep each object's byte order."""
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3

from tests.recovery.source_repair import Database, location, recovery_audit
from tests.storage.source_quarantine import Fixture, wait


def main():
    db, home = Database(), Fixture()
    assert db.type in ('postgres', 'mysql') and home.backend
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_SECOND_DATA'], region_name='us-east-1',
        aws_access_key_id=home.credentials['access_key'], aws_secret_access_key=home.credentials['secret_key'],
        config=home.client.meta.config)
    if db.type == 'mysql':
        profile = home.cli('storage', 'profile', 'show', home.name)
        home.cli('storage', 'profile', 'set', home.name, '-', value={**profile, 'upload_concurrency': 8})
    a, b = b'R' * (4 << 20), b'S' * (4 << 20)
    home.put('a', a)
    first_chunk, = db.chunks(home)
    home.put('b', b)
    chunks = db.chunks(home)
    assert len(chunks) == 2
    for row in chunks:
        encoding, = db.query('SELECT location_id FROM chunk_encodings WHERE encoding_id=%s', row['current_encoding_id'])
        path = location(home, encoding['location_id'])
        stored = home.bytes(path)
        home.bytes(path, bytes([stored[0] ^ 1]) + stored[1:])
        scope = dict(source='chunk', storage_id=home.store['id'], id=str(row['current_encoding_id']))
        home.finished(home.execute('quarantine', home.preview('quarantine', scope)))

    if db.type == 'postgres':
        postgres_race(db, home, peer, a, b, first_chunk['id'])
        peer_key, peer_payload = 'ba', b + a
    else:
        mysql_race(db, home, peer, a, b)
        peer_key, peer_payload = 'bb', b + b
    for key, expected in [('a', a), ('b', b), ('ab', a + b), (peer_key, peer_payload)]:
        home.read(key, expected)
    for row in chunks:
        recovery_audit(db, row, 's3')
    print('PASS overlapping repairs on the first PUT, original offsets and one recovery per chunk', flush=True)


def postgres_race(db, home, peer, a, b, first_id):
    import psycopg

    def waiters(pid):
        return db.query("""SELECT pid FROM pg_stat_activity WHERE datname=current_database()
            AND %s=ANY(pg_blocking_pids(pid)) AND query ILIKE '%%chunks%%'""", pid)

    settings = db.settings
    with psycopg.connect(host=settings['host'], port=settings.get('port', 5432), user=settings['user'],
            dbname=settings['name'], password=settings['password']) as locker, ThreadPoolExecutor(2) as pool:
        control = home.arm('repair-before-commit')
        peer_gate = Path(os.environ['MOKYU_TEST_PEER_FAULT_DIR']) / 'node-write-seal'
        peer_gate.with_suffix('.hit').unlink(missing_ok=True)
        peer_gate.touch()
        try:
            first = pool.submit(home.put, 'ab', a + b)
            wait(lambda: control.with_suffix('.hit').exists() or first.done(), 'first repair did not reach publication')
            assert not first.done(), first.result()
            second = pool.submit(peer.put_object, Bucket=home.name, Key='ba', Body=b + a)
            wait(lambda: peer_gate.with_suffix('.hit').exists() or second.done(), 'peer did not prepare its repairs')
            assert not second.done(), second.result()

            locker.execute("SET LOCAL lock_timeout='5s'")
            locker.execute('SELECT id FROM chunks WHERE id=%s FOR UPDATE', (first_id,)).fetchone()
            control.unlink(missing_ok=True)
            first_pid = wait(lambda: waiters(locker.info.backend_pid), 'first repair did not wait for A')[0]['pid']
            peer_gate.unlink(missing_ok=True)
            wait(lambda: any(row['pid'] != first_pid for row in waiters(locker.info.backend_pid) + waiters(first_pid)),
                 'peer did not enter the overlapping commit')
            locker.commit()
            first.result(timeout=40)
            second.result(timeout=40)
        finally:
            control.unlink(missing_ok=True)
            peer_gate.unlink(missing_ok=True)
            locker.rollback()


def mysql_race(db, home, peer, a, b):
    first_gate = Path(os.environ['MOKYU_TEST_DATA_FAULT_DIR']) / 'node-write-seal'
    peer_gate = Path(os.environ['MOKYU_TEST_PEER_FAULT_DIR']) / 'node-write-seal'
    with ThreadPoolExecutor(2) as pool:
        try:
            for gate in [first_gate, peer_gate]:
                gate.with_suffix('.hit').unlink(missing_ok=True)
                gate.touch()
            second = pool.submit(peer.put_object, Bucket=home.name, Key='bb', Body=b + b)
            first = pool.submit(home.put, 'ab', a + b)
            wait(lambda: peer_gate.with_suffix('.hit').exists() or second.done(), 'peer did not prepare B')
            assert not second.done(), second.exception()
            wait(lambda: first_gate.with_suffix('.hit').exists() or first.done(), 'first node did not prepare A and B')
            assert not first.done(), first.exception()
            assert len(db.candidates(home)) == 3
        finally:
            first_gate.unlink(missing_ok=True)
            peer_gate.unlink(missing_ok=True)
        first.result(timeout=40)
        second.result(timeout=40)


if __name__ == '__main__':
    main()
