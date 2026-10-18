"""Operation timing excludes write readback and includes the entire multipart lifecycle."""
import io
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.benchmarks.object_io import disk, memory, payload, readback, request


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
