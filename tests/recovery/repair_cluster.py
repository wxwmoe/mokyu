"""Two real data nodes recover one source; leases and revocation fence publication."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
from tests.recovery.source_repair import Database, blocked, isolate, original_unchanged
from tests.storage.source_quarantine import Fixture, rejected, wait


def second_client(fixture):
    return boto3.client('s3',endpoint_url=os.environ['MOKYU_TEST_SECOND_DATA'],region_name='us-east-1',
        aws_access_key_id=fixture.credentials['access_key'],aws_secret_access_key=fixture.credentials['secret_key'],
        config=fixture.client.meta.config)


def main():
    database = Database()
    assert database.type == 'postgres'
    fixture = Fixture()
    assert fixture.cli('status')['role'] == 'control'
    raw = os.urandom(32768)
    original, _, _ = isolate(fixture,database,raw)
    other = second_client(fixture)
    marker = fixture.arm('repair-before-commit')
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(fixture.put,'first',raw)
        try:
            wait(lambda:marker.with_suffix('.hit').exists() or first.done(),'first data did not hold its candidate')
            assert not first.done(),first.exception()
            candidate = database.candidates(fixture)[-1]
            fixture.finished(fixture.cli('maintenance','run','gc'))
            assert database.query('SELECT state,owner_stream FROM chunk_encodings WHERE encoding_id=%s',candidate['encoding_id'])[0]['owner_stream'] is not None
            # Lose only the first writer's advisory-lock connection, keeping its verified I/O alive.
            claims = database.query("SELECT DISTINCT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() AND l.locktype='advisory' AND a.query LIKE %s", 'SELECT pg_advisory_xact_lock(%734922709851001%')
            assert len(claims)==1, 'fixture must have exactly one held chunk claim'
            database.query('SELECT pg_terminate_backend(%s)',claims[0]['pid'])
            second = pool.submit(other.put_object,Bucket=fixture.name,Key='second',Body=raw)
            second.result(timeout=45)
            current = database.chunks(fixture)[0]
            assert current['current_encoding_id'] not in (original['current_encoding_id'],candidate['encoding_id'])
        finally:
            marker.unlink(missing_ok=True)
        first.result(timeout=45)
    sources = database.query("SELECT state,owner_stream,recovery_pending FROM chunk_encodings WHERE chunk_id=%s AND encoding_id<>%s",original['id'],original['current_encoding_id'])
    assert len(sources)==2 and sum(row['state']=='ready' for row in sources)==1
    assert all(row['owner_stream'] is None and not row['recovery_pending'] for row in sources)
    for key in ('first','second','original'):
        fixture.read(key,raw)
        with other.get_object(Bucket=fixture.name,Key=key)['Body'] as body:
            assert body.read()==raw
    print('PASS two data candidates after lost coordination publish one source and reuse the winner',flush=True)

    for reason in ('credential','lease','draining'):
        fixture = Fixture()
        original, _, _ = isolate(fixture,database,raw)
        node = None
        def revoke():
            nonlocal node
            candidate = database.candidates(fixture)[-1]
            if reason=='credential':
                fixture.cli('cred','disable',fixture.credentials['access_key'])
            elif reason=='lease':
                database.query("UPDATE stream_leases SET expires_at=now()-interval '1 second' WHERE stream_id=%s AND lease_type='write'",candidate['owner_stream'])
            else:
                import json
                from pathlib import Path
                node = uuid.UUID(json.loads(Path(os.environ['MOKYU_TEST_NODE_STATE']).read_text())['node_id'])
                database.query("UPDATE nodes SET status='draining' WHERE id=%s",node)
        try:
            future = blocked(fixture,'repair-before-commit',lambda:fixture.put('rejected',raw),revoke)
            rejected(lambda:future.result())
            original_unchanged(fixture,database,original)
        finally:
            if reason=='credential':
                fixture.cli('cred','enable',fixture.credentials['access_key'])
            if node:
                database.query("UPDATE nodes SET status='registered' WHERE id=%s",node)
        other = second_client(fixture)
        other.put_object(Bucket=fixture.name,Key='retry',Body=raw)
        fixture.read('original',raw)
        print('PASS',reason,'change rejects old repair and another data node safely recovers',flush=True)


if __name__ == '__main__':
    main()
