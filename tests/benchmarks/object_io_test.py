"""Operation timing excludes write readback and includes the entire multipart lifecycle."""
import io
import json
import sqlite3
import time
import zlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.benchmarks.object_io import backlog, disk, drain_raw, measure, memory, payload, readback, request


class Client:
    def __init__(self, body=b'data', fail=False):
        self.body, self.fail, self.calls = body, fail, []

    def put_object(self, **args):
        self.calls.append('put')

    def get_object(self, **args):
        self.calls.append('get')
        return dict(Body=io.BytesIO(self.body), ContentLength=len(self.body))

    def head_object(self, **args):
        self.calls.append('head')
        return dict(ContentLength=len(self.body))

    def create_multipart_upload(self, **args):
        self.calls.append('create')
        return dict(UploadId='upload')

    def upload_part(self, **args):
        self.calls.append('part')
        return dict(ETag='etag')

    def complete_multipart_upload(self, **args):
        self.calls.append('complete')
        if self.fail:
            raise ValueError('completion failed')

    def abort_multipart_upload(self, **args):
        self.calls.append('abort')


class TimingTests(unittest.TestCase):
    def test_rate_bounds_arrivals_and_keeps_retry_errors_separate(self):
        from botocore.exceptions import ClientError

        class Store(Client):
            def __init__(self):
                super().__init__()
                self.arrivals = []

            def put_object(self, **args):
                if args['Key'].endswith('/source'):
                    self.body = args['Body']
                    return
                self.arrivals.append(time.perf_counter())
                raise ClientError(dict(Error={'Code': 'SlowDown'},
                    ResponseMetadata={'HTTPHeaders': {'retry-after': '60'}}), 'PutObject')

            def close(self):
                pass

        client = Store()
        with patch('tests.benchmarks.object_io.boto3.client', return_value=client):
            result = measure(dict(operation='put', size=32, seconds=1, max_operations=100,
                concurrency=2, requests_per_second=4, max_total_bytes=3232, seed=1,
                prefix='test', endpoint='http://fixture'),
                dict(bucket='bucket', access_key='access', secret_key='secret'))
        self.assertEqual(result['attempts'], 4)
        self.assertEqual(result['acknowledged'], 0)
        self.assertEqual(result['errors'], {'SlowDown': 4})
        self.assertEqual(result['retry_after'], {'60': 4})
        self.assertIsNone(result['latency']['p95_ms'])
        self.assertGreater(max(client.arrivals) - min(client.arrivals), .7)

    def test_overwrite_verifies_final_versions_without_counting_reads_as_writes(self):
        class Store:
            def __init__(self):
                self.objects = {}
                self.reads = 0

            def put_object(self, **args):
                self.objects[args['Key']] = args['Body']

            def get_object(self, **args):
                self.reads += 1
                body = self.objects[args['Key']]
                return dict(Body=io.BytesIO(body), ContentLength=len(body))

            def close(self):
                pass

        client = Store()
        with patch('tests.benchmarks.object_io.boto3.client', return_value=client):
            result = measure(dict(operation='put', size=32, seconds=1, max_operations=8,
                concurrency=1, hot_keys=1, repeat_every=2, max_total_bytes=1024, seed=1, prefix='test', endpoint='http://fixture'),
                dict(bucket='bucket', access_key='access', secret_key='secret'))
        self.assertEqual(result['errors'], {})
        self.assertEqual(result['acknowledged'], 8)
        self.assertEqual(result['verification']['objects'], 1)
        self.assertEqual(client.reads, 2)
        self.assertEqual(client.objects['test/hot-0-0'][-16:], (1).to_bytes(16, 'big'))

    def test_background_drain_cannot_hide_failure_or_timeout(self):
        from unittest.mock import Mock
        database = Mock()
        database.query.side_effect = [[dict(source='raw_uploads', pending=2, failed=0)], []]
        with patch('tests.benchmarks.object_io.time.sleep'):
            result = drain_raw(database, 'bucket', 1)
        self.assertTrue(result['completed'])
        self.assertEqual(result['before']['pending'], 2)
        self.assertEqual(result['history'][-1]['pending'], 0)
        database.query.side_effect = None
        for pending, failed in [(1, 1), (1, 0)]:
            database.query.return_value = [dict(source='raw_uploads', pending=pending, failed=failed)]
            result = drain_raw(database, 'bucket', 0)
            self.assertFalse(result['completed'])
            self.assertEqual(result['after']['pending'], 1)

    def test_decimal_database_aggregates_remain_json_numbers(self):
        from decimal import Decimal
        from unittest.mock import Mock
        database = Mock()
        database.query.return_value = [dict(source='pack_inputs', pending=2, failed=Decimal(1))]
        result = backlog(database, 'bucket')
        self.assertEqual(json.loads(json.dumps(result)),
            dict(pending=2, failed=1, sources={'pack_inputs': dict(pending=2, failed=1)}))

    def test_write_has_no_hidden_read(self):
        client = Client()
        request(client, 'bucket', 'key', b'data', 'put')
        self.assertEqual(client.calls, ['put'])
        readback(client, 'bucket', 'key', b'data')
        self.assertEqual(client.calls, ['put', 'get'])

    def test_complete_is_not_the_whole_upload(self):
        client = Client()
        with patch('tests.benchmarks.object_io.time.perf_counter', side_effect=range(8)):
            elapsed, stages = request(client, 'bucket', 'key', b'data', 'multipart')
        self.assertEqual(elapsed, 7000)
        self.assertEqual(stages, dict(create=1000, part=1000, complete=1000))
        self.assertEqual(client.calls, ['create', 'part', 'complete'])

    def test_failed_completion_aborts(self):
        client = Client(fail=True)
        with self.assertRaises(ValueError):
            request(client, 'bucket', 'key', b'data', 'multipart')
        self.assertEqual(client.calls[-1], 'abort')

    def test_bad_reads_are_not_success(self):
        with self.assertRaisesRegex(AssertionError, 'body mismatch'):
            request(Client(b'bad!'), 'bucket', 'key', b'data', 'get')
        with self.assertRaisesRegex(AssertionError, 'HEAD length'):
            request(Client(b'bad'), 'bucket', 'key', b'data', 'head')

    def test_payload_sizes(self):
        for size in (0, 1, 15, 16, 32):
            self.assertEqual(len(payload(b'x' * size, 1)), size)

    def test_full_payload_changes_every_block_and_is_reproducible(self):
        base = b'x' * (2 * 1024 * 1024 + 19)
        for mode in ('full', 'compressible'):
            first, second = payload(base, 1, mode), payload(base, 2, mode)
            self.assertEqual(first, payload(base, 1, mode))
            self.assertEqual(len(first), len(base))
            for offset in range(0, len(base), 4096):
                self.assertNotEqual(first[offset:offset+4096], second[offset:offset+4096])
        self.assertLess(len(zlib.compress(payload(base, 1, 'compressible'))), len(base) // 20)
        self.assertGreater(len(zlib.compress(payload(base, 1, 'full'))), len(base) * .99)
        self.assertIs(payload(base, 2, 'duplicate'), base)
        self.assertEqual(payload(base, 1, 'tail')[:-16], base[:-16])
        for mode in ('full', 'compressible', 'duplicate'):
            for size in (0, 1, 15, 16, 32):
                self.assertEqual(len(payload(base[:size], 1, mode)), size)

    def test_backlog_includes_retired_sources_and_pack_inputs(self):
        connection = sqlite3.connect(':memory:')
        self.addCleanup(connection.close)
        connection.row_factory = sqlite3.Row
        connection.executescript('''
            CREATE TABLE buckets(id, name, storage_id);
            CREATE TABLE streams(id, bucket_id);
            CREATE TABLE raw_sources(id, storage_id, processing_task, durability, state, quarantine_code);
            CREATE TABLE raw_processing(source_id, last_error);
            CREATE TABLE raw_uploads(source_id, last_error);
            CREATE TABLE pending_uploads(stream_id);
            CREATE TABLE chunk_encodings(storage_id, sync_pending, state, quarantine_code);
            CREATE TABLE chunks(id, storage_id);
            CREATE TABLE tasks(id, state);
            CREATE TABLE pack_inputs(chunk_id, task_id);
            INSERT INTO buckets VALUES(1,'fixture',10),(2,'unrelated',20);
            INSERT INTO raw_sources VALUES(1,10,1,'buffered','ready',NULL),(2,20,NULL,'buffered','ready',NULL);
            INSERT INTO raw_processing VALUES(1,NULL);
            INSERT INTO raw_uploads VALUES(1,NULL);
            INSERT INTO chunk_encodings VALUES(10,1,'ready',NULL),(20,1,'ready',NULL);
            INSERT INTO chunks VALUES(1,10);
            INSERT INTO tasks VALUES(1,'queued'),(2,'failed');
            INSERT INTO pack_inputs VALUES(1,2);
        ''')
        class Database:
            def query(self, statement, *parameters):
                return [dict(row) for row in connection.execute(statement.replace('%s', '?'), parameters)]
        result = backlog(Database(), 'fixture')
        self.assertEqual(result['pending'], 6)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(set(result['sources']), {'raw_processing', 'raw_uploads', 'raw_sync', 'chunk_sync', 'cdc', 'pack_inputs'})

    def test_memory_sampling_covers_drain_and_excludes_readback(self):
        class Store(Client):
            def put_object(self, **args):
                self.body = args['Body']
            def close(self):
                pass
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for name, content in {'memory.current':'1', 'memory.stat':'anon 1\nfile 0\nfile_dirty 0\nfile_writeback 0\n',
                'cpu.stat':'usage_usec 0\n', 'io.stat':'8:0 rbytes=0 wbytes=0\n', 'cpu.max':'max 100000',
                'memory.max':'1024', 'cpuset.cpus.effective':'0'}.items():
                (root / name).write_text(content)
            drains = 0
            def drain(*args):
                nonlocal drains
                drains += 1
                if drains == 2:
                    (root / 'memory.current').write_text('80')
                    time.sleep(.25)
                return dict(completed=True, seconds=0)
            with patch('tests.benchmarks.object_io.boto3.client', return_value=Store()), \
                 patch('tests.benchmarks.object_io.drain_raw', side_effect=drain), \
                 patch('tests.benchmarks.object_io.backlog', return_value=dict(pending=0, failed=0)), \
                 patch('tests.recovery.source_repair.Database'), \
                 patch.dict('os.environ', MOKYU_TEST_ALLOW_STATE_CHANGES='isolated-only'):
                result = measure(dict(operation='put', size=32, seconds=1, max_operations=1, concurrency=1,
                    max_total_bytes=64, seed=1, prefix='test', endpoint='http://fixture', raw_drain_seconds=1,
                    resources={'app': {'cgroup': str(root)}}, payload_mode='full', requests_per_second=10),
                    dict(bucket='bucket', access_key='access', secret_key='secret'))
            self.assertEqual(result['errors'], {})
            self.assertEqual(result['resources']['app']['sampled_memory_current_max'], 1)
            self.assertEqual(result['raw_processing']['resources']['app']['sampled_memory_current_max'], 80)
            self.assertGreater(result['raw_processing']['total_seconds'], result['seconds'])
            self.assertTrue(result['backlog_history'])
            self.assertEqual(result['target_requests_per_second'], 10)

    def test_resources_keep_devices_separate(self):
        with TemporaryDirectory() as directory:
            path = Path(directory)
            (path / 'memory.stat').write_text('anon 1\nfile 2\nfile_dirty 3\nfile_writeback 4\nother 5\n')
            (path / 'io.stat').write_text('8:0 rbytes=10 wbytes=20\n8:1 rbytes=10 wbytes=20\n')
            self.assertEqual(memory(path), dict(anon=1, file=2, file_dirty=3, file_writeback=4))
            self.assertEqual(disk(path), {'8:0': dict(rbytes=10, wbytes=20),
                                          '8:1': dict(rbytes=10, wbytes=20)})


if __name__ == '__main__':
    unittest.main()
