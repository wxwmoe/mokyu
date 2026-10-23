"""Benchmark accounting and failure checks; run inside a Linux cgroup v2 container."""
import gzip
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import brotli
import zstandard

from tests.benchmarks.public_reads import evict_pages, measure, sql_counts, status

RAW = b'Exact benchmark body accounting.\n' * 2000
ENCODED = {'identity': RAW, 'gzip': gzip.compress(RAW), 'br': brotli.compress(RAW),
           'zstd': zstandard.ZstdCompressor().compress(RAW)}


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def do_GET(self):
        encoding = self.headers['Accept-Encoding']
        body = b'wrong' if self.path == '/wrong' else ENCODED[encoding]
        if self.path == '/missing':
            body = b''
        self.send_response(404 if self.path == '/missing' else 200)
        self.send_header('Content-Length', str(len(body)))
        if encoding != 'identity':
            self.send_header('Content-Encoding', encoding)
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    do_HEAD = do_GET


class Accounting(unittest.TestCase):
    def test_snapshot_counts_cover_both_projections_without_hiding_queries(self):
        connection = Mock()
        connection.execute.return_value.fetchall.return_value = [
            ('SELECT s.*,b.settings_revision,e.offset_bytes AS cache_offset FROM objects o', 3),
            ('SELECT s.id,s.bucket_id,s.metadata,e.offset_bytes AS cache_offset FROM objects o', 7),
            ('SELECT s.* FROM streams s', 11),
            ('SELECT e.*,c.id FROM extents e', 5),
            ('INSERT INTO stream_leases(stream_id) VALUES($1)', 2),
        ]
        self.assertEqual(sql_counts(connection), {'source_pages': 5, 'lease_insert': 2, 'snapshot': 10})
        self.assertIsNone(sql_counts(None))

    def test_busy_pages_cannot_silently_be_counted_as_cold(self):
        def resident(address, size, pages):
            pages[0] = 1
            return 0

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'owned.bin'
            path.write_bytes(b'x' * 4096)
            with patch.dict(os.environ, MOKYU_TEST_ALLOW_STATE_CHANGES='isolated-only'), \
                    patch('tests.benchmarks.public_reads.ctypes.CDLL') as library, \
                    patch('tests.benchmarks.public_reads.time.monotonic', side_effect=[0, 2]):
                library.return_value.mincore = resident
                with self.assertRaisesRegex(AssertionError, 'remain resident'):
                    evict_pages([str(path)])

    def test_status_preserves_policy_counters_and_optional_legacy_fields(self):
        with patch('tests.benchmarks.public_reads.requests.get') as get:
            get.return_value.json.return_value = {'version': 'test', 'public_read_cache': {'hits': 2},
                'file_delivery': {'planned_bytes': 17}, 'unrelated': True}
            self.assertEqual(status('http://fixture/status'), {'version': 'test',
                'public_read_cache': {'hits': 2}, 'file_delivery': {'planned_bytes': 17}})

    def test_eviction_is_scoped_to_the_supplied_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'owned.bin'
            body = os.urandom(1024 * 1024)
            with path.open('wb') as file:
                file.write(body)
                file.flush()
                os.fsync(file.fileno())
            with patch.dict(os.environ, MOKYU_TEST_ALLOW_STATE_CHANGES='isolated-only'):
                evict_pages([str(path)])
            self.assertEqual(path.read_bytes(), body)

    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.expected = Path(cls.directory.name) / 'expected.bin'
        cls.expected.write_bytes(RAW)
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        cls.directory.cleanup()

    def run_case(self, encoding='identity', path='/', warmup=0, method='GET', seconds=0):
        fixture = {'concurrency': 2, 'requests': 8, 'warmup': warmup,
                   'resources': {'client': {'cgroup': '/sys/fs/cgroup'}}}
        if seconds:
            fixture.update(seconds=seconds, requests=100000)
        case = {'name': 'self-check', 'urls': [f'http://127.0.0.1:{self.server.server_port}{path}'],
                'encoding': encoding, 'expected_file': str(self.expected), 'method': method}
        return measure(fixture, case, None)

    def test_timed_head_counts_actual_requests_and_no_body(self):
        result = self.run_case(method='HEAD', seconds=1)
        self.assertEqual(result['errors'], {})
        self.assertGreater(result['seconds'], .9)
        self.assertLess(result['attempts'], 100000)
        self.assertEqual(result['attempts'], result['successes'])
        self.assertEqual(result['successful_wire_body_bytes'], 0)
        self.assertIn('file', result['resources']['client']['sampled_memory_stat_max'])

    def test_hot_set_visits_each_url(self):
        urls = [f'http://127.0.0.1:{self.server.server_port}/hot-{i}' for i in range(128)]
        seen = set()
        original = Handler.do_GET

        def record(handler):
            seen.add(handler.path)
            original(handler)

        with patch.object(Handler, 'do_GET', record):
            result = measure(dict(concurrency=4, requests=len(urls), warmup=0, resources={}),
                dict(name='hot-set', urls=urls, encoding='identity', expected_file=str(self.expected)), None)
        self.assertEqual(result['errors'], {})
        self.assertEqual(result['successes'], len(urls))
        self.assertEqual(len(seen), len(urls))

    def test_encoded_bytes_are_not_decoded_length(self):
        for encoding in ENCODED:
            with self.subTest(encoding=encoding):
                result = self.run_case(encoding, warmup=1)
                self.assertEqual(result['errors'], {})
                self.assertEqual(result['successes'], 8)
                self.assertEqual(result['successful_wire_body_bytes'], 8 * len(ENCODED[encoding]))
                self.assertEqual(result['decoded_size'], len(RAW))
                self.assertGreater(result['ttfb']['p50_ms'], 0)
                self.assertIsNone(result['sql_delta'])

    def test_encoded_head_has_headers_without_a_compressed_body(self):
        for encoding in ENCODED:
            with self.subTest(encoding=encoding):
                result = self.run_case(encoding, method='HEAD')
                self.assertEqual(result['errors'], {})
                self.assertEqual(result['successes'], 8)
                self.assertEqual(result['successful_wire_body_bytes'], 0)

    def test_failures_are_counted_without_fake_percentiles(self):
        for path in ['/wrong', '/missing']:
            with self.subTest(path=path):
                result = self.run_case(path=path)
                self.assertEqual(sum(result['errors'].values()), 8)
                self.assertEqual(result['successes'], 0)
                self.assertEqual(result['successful_wire_body_bytes'], 0)
                self.assertIsNone(result['latency']['p95_ms'])

    def test_warmup_failure_does_not_start_workers(self):
        with self.assertRaisesRegex(AssertionError, 'body mismatch'):
            self.run_case(path='/wrong', warmup=1)


if __name__ == '__main__':
    unittest.main()
