"""Stream large Multipart parts through a disposable raw backend.

Uses source_quarantine fixture variables and records Complete latency and RSS.
Run inside the isolated service container so its PID file is meaningful.
"""
import argparse
import base64
import hashlib
import json
import os
import tempfile
import threading
import time
from pathlib import Path

from tests.storage.source_quarantine import Fixture


def rss(pid):
    if pid is None:
        return None
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith('VmRSS:'):
            return int(line.split()[1]) * 1024
    raise AssertionError('service RSS is unavailable')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--part-mib', type=int, default=96)
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--label', default='candidate')
    parser.add_argument('--local', action='store_true')
    parser.add_argument('--order', choices=['sequential', 'reverse'], default='reverse')
    arguments = parser.parse_args()
    assert 5 <= arguments.part_mib <= 512
    assert arguments.repeat > 0
    fixture = Fixture()
    assert bool(fixture.backend) != arguments.local, 'backend does not match --local selection'
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    pid_file = os.environ.get('MOKYU_TEST_PID_FILE')
    pid = int(Path(pid_file).read_text()) if pid_file else None
    results = []
    with tempfile.TemporaryDirectory(prefix='raw-streaming-', dir=Path(os.environ['MOKYU_TEST_CONFIG']).parent) as directory:
        parts = []
        sizes = [arguments.part_mib * 1024 * 1024] * 2 + [1024 * 1024 + 37]
        full = hashlib.sha256()
        for number, size in enumerate(sizes):
            path = Path(directory) / str(number)
            digest = hashlib.md5()
            with path.open('wb') as output:
                remaining = size
                while remaining:
                    block = os.urandom(min(256 * 1024, remaining))
                    output.write(block)
                    digest.update(block)
                    full.update(block)
                    remaining -= len(block)
            parts.append((path, digest.digest()))
        for iteration in range(arguments.repeat):
            key = f'streaming-{iteration}'
            upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key=key)['UploadId']
            manifest = []
            upload_started = time.monotonic()
            for index in ([0, 1, 2] if arguments.order == 'sequential' else [2, 1, 0]):
                path, digest = parts[index]
                with path.open('rb') as body:
                    result = fixture.client.upload_part(Bucket=fixture.name, Key=key, UploadId=upload,
                        PartNumber=index + 1, Body=body, ContentMD5=base64.b64encode(digest).decode())
                manifest.append(dict(PartNumber=index + 1, ETag=result['ETag']))
            upload_elapsed = time.monotonic() - upload_started
            initial = rss(pid)
            peaks = [initial or 0]
            sample_errors = []
            done = threading.Event()

            def sample():
                try:
                    while not done.wait(.02):
                        peaks.append(rss(pid) or 0)
                except Exception as error:
                    sample_errors.append(str(error))

            monitor = threading.Thread(target=sample)
            monitor.start()
            started = time.monotonic()
            try:
                result = fixture.client.complete_multipart_upload(Bucket=fixture.name, Key=key, UploadId=upload,
                    MultipartUpload={'Parts': sorted(manifest, key=lambda part: part['PartNumber'])})
            finally:
                elapsed = time.monotonic() - started
                done.set()
                monitor.join()
            assert not sample_errors, sample_errors
            expected_etag = '"' + hashlib.md5(b''.join(digest for _, digest in parts)).hexdigest() + '-3"'
            assert result['ETag'] == expected_etag, result
            actual = hashlib.sha256()
            with fixture.client.get_object(Bucket=fixture.name, Key=key)['Body'] as body:
                for block in iter(lambda: body.read(256 * 1024), b''):
                    actual.update(block)
            assert actual.digest() == full.digest()
            boundary = sizes[0]
            with parts[0][0].open('rb') as left, parts[1][0].open('rb') as right:
                left.seek(boundary - 37)
                expected = left.read() + right.read(41)
            with fixture.client.get_object(Bucket=fixture.name, Key=key,
                    Range=f'bytes={boundary - 37}-{boundary + 40}')['Body'] as body:
                assert body.read() == expected
            results.append(dict(label=arguments.label, iteration=iteration,
                backend='local' if arguments.local else 's3', order=arguments.order,
                upload_seconds=upload_elapsed, total_write_seconds=upload_elapsed + elapsed,
                part_mib=arguments.part_mib, bytes=sum(sizes), complete_seconds=elapsed,
                complete_mib_per_second=sum(sizes) / 1024 ** 2 / elapsed,
                rss_before_bytes=initial, rss_peak_bytes=max(peaks) if pid else None,
                verified_sha256=actual.hexdigest()))
            print(json.dumps(results[-1]), flush=True)
            fixture.client.delete_object(Bucket=fixture.name, Key=key)
        print('PASS streamed Multipart, original ETag, complete checksum and boundary Range', flush=True)


if __name__ == '__main__':
    main()
