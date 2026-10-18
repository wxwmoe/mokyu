"""A paused PostgreSQL standby cannot revive a quarantined physical source."""
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import requests

from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    fixture = Fixture()
    primary = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
    replica = psycopg.connect(Path(os.environ['MOKYU_TEST_REPLICA_DATABASE_FILE']).read_text(), autocommit=True)
    assert replica.execute('SELECT pg_is_in_recovery()').fetchone()[0]
    raw = os.urandom(193 * 1024)
    fixture.client.put_object(Bucket=fixture.name, Key='original', Body=raw, ACL='public-read')
    stream, chunk, encoding = wait(lambda: primary.execute("SELECT o.stream_id,e.chunk_id,c.current_encoding_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE o.bucket_id=%s AND o.object_key=%s AND NOT EXISTS(SELECT 1 FROM pending_uploads WHERE chunk_id=c.id)", (fixture.bucket_id, 'original')).fetchone(), 'upload did not become durable')
    assert primary.execute('SELECT count(*) FROM extents WHERE stream_id=%s', (stream,)).fetchone()[0] == 1
    with primary.transaction():
        primary.execute('DELETE FROM extents WHERE stream_id=%s', (stream,))
        for offset in range(0, len(raw), 1024):
            primary.execute('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id,source_offset) VALUES(%s,%s,1024,%s,%s)', (stream, offset, chunk, offset))
    physical = wait(fixture.keys, 'source was not listed')[0]
    original = fixture.bytes(physical)
    scope = dict(storage_id=fixture.store['id'], source='chunk', id=str(encoding))
    url = os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/original'
    marker = fixture.arm('read-replica-page')
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(requests.get, url, headers={'Accept-Encoding':'identity', 'Cache-Control':'no-store'}, timeout=35)
            try:
                wait(lambda: marker.with_suffix('.hit').exists(), 'public read did not reach replica paging')
                lsn = primary.execute('SELECT pg_current_wal_insert_lsn()').fetchone()[0]
                wait(lambda: replica.execute('SELECT pg_last_wal_replay_lsn()>=%s::pg_lsn', (lsn,)).fetchone()[0], 'replica did not catch up')
                replica.execute('SELECT pg_wal_replay_pause()')
                fixture.bytes(physical, b'!')
                fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
                fixture.bytes(physical, original)
                assert replica.execute('SELECT quarantined_at IS NULL FROM chunk_encodings WHERE encoding_id=%s', (encoding,)).fetchone()[0]
                assert primary.execute('SELECT quarantined_at IS NOT NULL FROM chunk_encodings WHERE encoding_id=%s', (encoding,)).fetchone()[0]
                rejected(lambda: fixture.client.copy_object(Bucket=fixture.name, Key='copy', CopySource={'Bucket':fixture.name, 'Key':'original'}))
            finally:
                marker.unlink(missing_ok=True)
            try:
                response = pending.result(timeout=35)
                # Already verified in-flight bytes may finish; they must never change.
                assert response.status_code >= 400 or response.content == raw
            except requests.RequestException:
                pass
        try:
            cold = requests.get(url, headers={'Accept-Encoding':'identity', 'Cache-Control':'no-store'}, timeout=30)
        except requests.exceptions.ChunkedEncodingError:
            pass  # Streaming headers may precede source selection; the body must terminate.
        else:
            assert cold.status_code >= 400, 'a stale replica revived a quarantined cold source'
    finally:
        marker.unlink(missing_ok=True)
        replica.execute('SELECT pg_wal_replay_resume()')
        replica.close()
        primary.close()
    fixture.recheck(scope)
    fixture.read('original', raw)
    print('PASS paused standby cannot authorize copies or revive quarantined cold sources', flush=True)


if __name__ == '__main__':
    main()
