"""Two real data nodes exercise chunk claims with four connections each."""
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    target = Fixture()
    database = Database()
    assert database.type == 'postgres' and target.backend
    peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_SECOND_DATA'], region_name='us-east-1',
        aws_access_key_id=target.credentials['access_key'], aws_secret_access_key=target.credentials['secret_key'],
        config=target.client.meta.config)
    store = uuid.UUID(target.store['id'])
    payloads = [os.urandom(32768) for _ in range(2)]
    wait(lambda: len(database.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND query LIKE %s", 'LISTEN %')) >= 5,
         'both data listeners and the control listener must be connected')

    def ready():
        try:
            target.put('probe', b'readable before claims')
            return True
        except Exception:
            return False
    wait(ready, 'data did not become writable', seconds=60)
    marker = target.arm('chunk-allocated')
    with ThreadPoolExecutor(max_workers=4) as pool:
        first = pool.submit(target.put, 'first', payloads[0])
        second = reader = None
        try:
            wait(lambda: marker.with_suffix('.hit').exists() or first.done(), 'first allocation did not pause')
            assert not first.done(), first.exception()
            second = pool.submit(target.put, 'second', payloads[1])
            wait(lambda: database.query("SELECT id FROM streams WHERE bucket_id=%s AND object_key='second'", uuid.UUID(target.bucket_id)),
                 'second writer did not start')
            wait(lambda: len(database.query("SELECT id FROM chunks WHERE storage_id=%s AND state='preparing'", store)) == 2,
                 'independent writers retained database connections')
            start = time.monotonic()
            reader = pool.submit(target.read, 'probe', b'readable before claims')
            reader.result(timeout=3)
            print(json.dumps(dict(probe_ms=(time.monotonic()-start)*1000)), flush=True)
            assert not first.done() and not second.done()
            peer.put_object(Bucket=target.name, Key='peer', Body=b'independent node')
        finally:
            marker.unlink(missing_ok=True)
        outcomes=[]
        for future in (first,second,reader):
            if future is not None:
                future.result(timeout=35)
                outcomes.append('ok')
        print(json.dumps(dict(outcomes=outcomes)),flush=True)
    for key, payload in zip(('first','second'),payloads):
        target.read(key,payload)

    for same in (True,False):
        data = os.urandom(32768)
        other = data if same else os.urandom(32768)
        marker = target.arm('chunk-allocated')
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(target.put, 'left-'+str(same), data)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or first.done(), 'cross-node writer did not pause')
                assert not first.done(),first.exception()
                second = pool.submit(peer.put_object,Bucket=target.name,Key='right-'+str(same),Body=other)
                if same:
                    time.sleep(.2)
                    assert not second.done(), 'same content bypassed its database claim'
                else:
                    second.result(timeout=10)
            finally:
                marker.unlink(missing_ok=True)
            first.result(timeout=30)
            second.result(timeout=30)
        target.read('left-'+str(same),data)
        target.read('right-'+str(same),other)
        rows=database.query("SELECT e.chunk_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id WHERE o.bucket_id=%s AND o.object_key IN (%s,%s) ORDER BY o.object_key",uuid.UUID(target.bucket_id),'left-'+str(same),'right-'+str(same))
        assert len(rows)==2 and (rows[0]['chunk_id']==rows[1]['chunk_id'])==same
    print('PASS durable claims preserve full readback, same-content exclusion and cross-node independence',flush=True)

    for point in ('chunk-allocated', 'chunk-uploading', 'chunk-stored'):
        payload = os.urandom(32768)
        marker = target.arm(point)
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(target.put, 'expired-' + point, payload)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or first.done(), point + ' did not pause')
                assert not first.done(), first.exception()
                old = database.query("SELECT c.id,c.current_encoding_id,c.owner_stream FROM chunks c JOIN streams s ON s.id=c.owner_stream WHERE s.bucket_id=%s AND s.object_key=%s",
                                     uuid.UUID(target.bucket_id), 'expired-' + point)[0]
                database.query("UPDATE stream_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE stream_id=%s AND lease_type='write'", old['owner_stream'])
                peer.put_object(Bucket=target.name, Key='replacement-' + point, Body=payload)
            finally:
                marker.unlink(missing_ok=True)
            rejected(lambda: first.result(timeout=35))
        target.read('replacement-' + point, payload)
        assert database.query("SELECT count(*) n FROM extents WHERE stream_id=%s", old['owner_stream'])[0]['n'] == 0
        assert not database.query("SELECT 1 FROM objects WHERE bucket_id=%s AND object_key=%s AND stream_id IS NOT NULL",
                                  uuid.UUID(target.bucket_id), 'expired-' + point)
        if point != 'chunk-allocated':
            assert database.query("SELECT recovery_pending FROM chunk_encodings WHERE encoding_id=%s", old['current_encoding_id'])[0]['recovery_pending']
        print('PASS', point, 'expired writer cannot publish or forget an unknown source', flush=True)


if __name__ == '__main__':
    main()
