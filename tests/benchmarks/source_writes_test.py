"""Readback failures cannot turn into successful write latency samples."""
import io
from tests.benchmarks.source_writes import operation,percentiles


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
print('PASS write benchmark rejects false success and does not fabricate empty percentiles')
