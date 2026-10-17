"""Benchmark accounting and failure checks; run inside a Linux cgroup v2 container."""
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest

import brotli
import zstandard

from tests.benchmarks.public_reads import measure

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
        self.wfile.write(body)


class Accounting(unittest.TestCase):
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

    def run_case(self, encoding='identity', path='/', warmup=0):
        fixture = {'concurrency': 2, 'requests': 8, 'warmup': warmup,
                   'resources': {'client': {'cgroup': '/sys/fs/cgroup'}}}
        case = {'name': 'self-check', 'urls': [f'http://127.0.0.1:{self.server.server_port}{path}'],
                'encoding': encoding, 'expected_file': str(self.expected)}
        return measure(fixture, case, None)

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
