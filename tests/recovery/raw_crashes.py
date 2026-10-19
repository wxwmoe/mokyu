"""Own an isolated service to check raw publication crashes and fsync failures.

Requires an initialized, stopped service and the source_quarantine fixture env.
Run as python -m tests.recovery.raw_crashes inside its disposable container.
"""
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'raw-crashes.log').open('a')
    process = None

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect raw-crashes.log'
            try:
                result = subprocess.run(command + ['cli', 'status', '--ready'],
                                        capture_output=True, text=True, timeout=5)
            except subprocess.TimeoutExpired:
                return False
            return result.returncode == 0

        wait(ready, 'service did not recover', seconds=45)

    try:
        start()
        fixture = Fixture()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, pack_range_enabled=False, upload_retention='1s', unreferenced_grace='1s',
                       min_storage_duration='0s')
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        expected = b'previous durable value'
        fixture.put('original', expected)
        points = ('raw-part-uploaded' if fixture.backend else 'raw-file-durable',
                  'raw-file-published', 'object-published')
        for point in points:
            pattern = point.encode() + b'\n'
            data = (pattern * (7 * 1024 * 1024 // len(pattern) + 1))[:7 * 1024 * 1024] if fixture.backend else pattern * 2048
            marker = fixture.arm(point)
            with ThreadPoolExecutor(max_workers=1) as executor:
                request = executor.submit(fixture.put, 'original', data)
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or request.done(),
                         'write did not reach ' + point)
                    assert not request.done(), 'write escaped its publication barrier'
                    stop(crash=True)
                finally:
                    marker.unlink(missing_ok=True)
                try:
                    request.result(timeout=20)
                except Exception:
                    pass
                else:
                    raise AssertionError('crashed request unexpectedly returned success')
            start()
            if point == 'object-published':
                expected = data
            fixture.read('original', expected)
            print('PASS raw crash boundary ' + point, flush=True)
        expected = b'acknowledged raw content' * 4096
        fixture.put('original', expected)
        stop(crash=True)
        start()
        fixture.read('original', expected)
        print('PASS acknowledged raw write survives process restart', flush=True)

        if fixture.backend:
            upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='original')['UploadId']
            parts = [b'multipart replay\n' * (1024 * 1024), b'last part']
            manifest = []
            for number, part in enumerate(parts, start=1):
                reply = fixture.client.upload_part(Bucket=fixture.name, Key='original', UploadId=upload,
                                                    PartNumber=number, Body=part)
                manifest.append(dict(PartNumber=number, ETag=reply['ETag']))

            def complete():
                return fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='original',
                    UploadId=upload, MultipartUpload={'Parts': manifest})

            marker = fixture.arm('raw-part-uploaded')
            with ThreadPoolExecutor(max_workers=1) as executor:
                request = executor.submit(complete)
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or request.done(),
                         'Complete did not reach its upstream part boundary')
                    assert not request.done(), request.exception()
                    stop(crash=True)
                finally:
                    marker.unlink(missing_ok=True)
                try:
                    request.result(timeout=20)
                except Exception:
                    pass
                else:
                    raise AssertionError('crashed Complete unexpectedly returned success')
            start()
            fixture.read('original', expected)
            listed = fixture.client.list_parts(Bucket=fixture.name, Key='original', UploadId=upload)['Parts']
            assert [part['ETag'] for part in listed] == [part['ETag'] for part in manifest]
            complete()
            expected = b''.join(parts)
            fixture.read('original', expected)
            print('PASS streamed Complete survives an uploaded upstream part and retries from retained staging', flush=True)

        if (fault_file := os.environ.get('MOKYU_TEST_FSYNC_FAULT')) and not fixture.backend:
            assert os.environ.get('LD_PRELOAD'), 'fsync test requires its Linux interposer'
            fault = Path(fault_file)
            trace = Path(os.environ['MOKYU_TEST_FSYNC_TRACE'])
            try:
                for mode in ('f', 'd'):
                    trace.write_text('')
                    fault.write_text(mode)
                    rejected(lambda: fixture.put('original', b'failed ' + mode.encode()))
                    assert mode + ' 1' in trace.read_text().splitlines()
                    fault.unlink()
                    fixture.read('original', expected)
            finally:
                fault.unlink(missing_ok=True)
            print('PASS file and directory fsync errors preserve the previous object', flush=True)

        def reclaimed():
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            uploads = fixture.backend.list_multipart_uploads(Bucket=fixture.remote_bucket, Prefix=fixture.prefix).get('Uploads', []) if fixture.backend else []
            return len(fixture.keys()) == 1 and not uploads

        wait(reclaimed, 'crashed raw sources or partial files were not reclaimed', seconds=60)
        fixture.read('original', expected)
        print('PASS GC removes unpublished files and upstream multipart uploads while retaining acknowledged content', flush=True)
    finally:
        stop()
        log.close()


if __name__ == '__main__':
    main()
