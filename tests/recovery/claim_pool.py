"""Two real data nodes exercise chunk claims with four connections each."""
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

import boto3

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    target = Fixture()
    database = Database()
    assert database.type == 'postgres' and target.backend
    baseline = os.environ.get('MOKYU_TEST_EXPECT_CLAIM_BLOCKED') == '1'
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
            time.sleep(.2)
            start = time.monotonic()
            reader = pool.submit(target.read, 'probe', b'readable before claims')
            if baseline:
                try:
                    reader.result(timeout=1)
                except TimeoutError:
                    print('PASS negative control: two claims plus LISTEN sessions exhaust the pool', flush=True)
                else:
                    raise AssertionError('baseline did not reproduce pool starvation')
            else:
                reader.result(timeout=3)
                print(json.dumps(dict(probe_ms=(time.monotonic()-start)*1000)), flush=True)
                assert not first.done() and not second.done()
                chunks = database.query("SELECT id FROM chunks WHERE storage_id=%s AND state='preparing'", store)
                assert len(chunks) == 1, 'queued claim consumed the query connection'
                # A different node still progresses while this node has its only claim paused.
                peer.put_object(Bucket=target.name, Key='peer', Body=b'independent node')
        finally:
            marker.unlink(missing_ok=True)
        outcomes=[]
        for future in (first,second,reader):
            if future is not None:
                try:
                    future.result(timeout=35)
                    outcomes.append('ok')
                except Exception as error:
                    assert baseline, repr(error)
                    outcomes.append(type(error).__name__)
        print(json.dumps(dict(baseline=baseline,outcomes=outcomes)),flush=True)
    for key, payload in zip(('first','second'),payloads):
        if baseline:
            target.put(key,payload)
        target.read(key,payload)
    if baseline:
        return

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
    print('PASS bounded claims preserve full readback, same-content exclusion and cross-node independence',flush=True)


if __name__ == '__main__':
    main()
