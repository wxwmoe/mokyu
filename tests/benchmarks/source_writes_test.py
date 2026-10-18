"""Readback failures cannot turn into successful write latency samples."""
import io
from tests.benchmarks.source_writes import operation,percentiles,sql_snapshot,sql_changes,SQL_METRICS,drain


class Fixture:
    def put_object(self,**values):
        pass

    def get_object(self,**values):
        return dict(Body=io.BytesIO(b'wrong'),ContentLength=5)


try:
    operation(Fixture(),'bucket','key','source',b'expected','dedup')
except AssertionError as error:
    assert str(error)=='readback body mismatch'
else:
    raise AssertionError('corrupt readback was accepted as a timing sample')
assert all(value is None for value in percentiles([]).values())
assert percentiles([3,1,2])==dict(p50_ms=2,p95_ms=3,p99_ms=3)

class Statistics:
    def execute(self,query):
        return self

    def fetchall(self):
        return [(user,42,123,top,*([10]*len(SQL_METRICS)),'stamp','SELECT '+('x'*2000))
                for user,top in [(1,True),(2,True),(1,False)]]


snapshot = sql_snapshot(Statistics())
assert len(snapshot)==3
changes = sql_changes({},snapshot)
assert len(changes)==3 and len(changes[0]['query'])>1000
assert sum(row['calls'] for row in changes if row['toplevel'])==20
assert sql_changes(snapshot,snapshot)==[]
assert sql_changes(None,None) is None
for changed in [{}, {key:dict(row,stats_since='reset') for key,row in snapshot.items()}]:
    try:
        sql_changes(snapshot,changed)
    except AssertionError:
        pass
    else:
        raise AssertionError('statistics eviction or reset was accepted')
reduced = {key:dict(row,calls=0) for key,row in snapshot.items()}
try:
    sql_changes(snapshot,reduced)
except AssertionError as error:
    assert 'reset' in str(error)
else:
    raise AssertionError('statistics reset was accepted')


class Queue(Statistics):
    def __init__(self,pending):
        self.pending = pending

    def fetchone(self):
        return (self.pending,0,100,0)


assert drain(Queue(0),0)['complete']
assert not drain(Queue(1),0)['complete']
samples=[]
assert drain(Queue(0),0,lambda:samples.append(True))['complete'] and samples==[True]
print('PASS readback, percentiles, SQL identity/reset and bounded drain')
