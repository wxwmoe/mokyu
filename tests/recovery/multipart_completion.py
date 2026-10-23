"""Complete keeps work after HTTP timeout/disconnect and fences recovered executions.

Uses the source_quarantine fixture. Single mode owns the stopped service; cluster
mode uses two running data nodes and the cluster_cache runner's restart signals.
Use MOKYU_TEST_COMPLETE_TIMEOUT_SECONDS=12 for fast checks or 300 for the real deadline.
Cluster data processes and this fixture must use the same value.
"""
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from xml.sax.saxutils import escape

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    cluster = bool(os.environ.get('MOKYU_TEST_PEER_ENDPOINT'))
    timeout_seconds = int(os.environ.get('MOKYU_TEST_COMPLETE_TIMEOUT_SECONDS', '12'))
    assert timeout_seconds in (12, 300)
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    marker = faults / 'multipart-completing'
    log = (faults / 'multipart-completion.log').open('a')
    process = None

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   env={**os.environ, 'MOKYU_TEST_COMPLETE_TIMEOUT_SECONDS': str(timeout_seconds)})
        def ready():
            assert process.poll() is None, 'service stopped; inspect multipart-completion.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                  timeout=5).returncode == 0
        wait(ready, 'single service did not become ready')

    def stop():
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=30)

    if not cluster:
        start()
    try:
        if proxy := os.environ.get('MOKYU_TEST_PROXY_ENDPOINT'):
            os.environ['MOKYU_TEST_ENDPOINT'] = proxy
        fixture, database = Fixture(), Database()
        config = fixture.client.meta.config.merge(Config(retries={'total_max_attempts': 1}, read_timeout=30))
        def client(endpoint):
            return boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                                aws_access_key_id=fixture.credentials['access_key'],
                                aws_secret_access_key=fixture.credentials['secret_key'], config=config)
        fixture.client = client(os.environ['MOKYU_TEST_ENDPOINT'])
        peer = client(os.environ.get('MOKYU_TEST_PEER_ENDPOINT', os.environ['MOKYU_TEST_ENDPOINT']))

        def prepare(key, size=256 * 1024 + 13):
            data = os.urandom(size)
            upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key=key)['UploadId']
            reply = fixture.client.upload_part(Bucket=fixture.name, Key=key, UploadId=upload,
                                               PartNumber=1, Body=data)
            return dict(Bucket=fixture.name, Key=key, UploadId=upload,
                        MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': reply['ETag']}]}), data

        def state(args):
            return database.query('SELECT state,manifest_hash,completion_token,output_stream FROM uploads WHERE id=%s', uuid.UUID(args['UploadId']))[0]

        def arm():
            marker.with_suffix('.hit').unlink(missing_ok=True)
            marker.touch()

        def request(args):
            url = fixture.client.generate_presigned_url('complete_multipart_upload',
                Params={name: args[name] for name in ('Bucket', 'Key', 'UploadId')}, HttpMethod='POST', ExpiresIn=max(300, timeout_seconds + 120))
            etag = escape(args['MultipartUpload']['Parts'][0]['ETag'])
            body = '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>' + etag + '</ETag></Part></CompleteMultipartUpload>'
            return requests.post(url, data=body, headers={'Content-Type': 'application/xml'},
                                 stream=True, timeout=(5, 15))

        args, data = prepare('timeout')
        arm()
        started = time.monotonic()
        response = request(args)
        assert response.status_code == 200 and response.headers['Cache-Control'] == 'no-store'
        assert response.headers.get('X-Accel-Buffering') == ('no' if not proxy else None)
        assert 'Retry-After' not in response.headers
        wait(marker.with_suffix('.hit').exists, 'completion did not start')
        original = state(args)
        assert original['state'] == 'completing'
        fixture.put('foreground', b'foreground still admitted')
        frames = [(time.monotonic() - started, chunk) for chunk in response.iter_content(chunk_size=None)]
        response.close()
        whitespace = [stamp for stamp, chunk in frames if chunk and not chunk.strip()]
        body = b''.join(chunk for _, chunk in frames)
        assert timeout_seconds - 2 <= time.monotonic() - started < timeout_seconds + 8, frames
        assert whitespace and all(4.5 <= b - a < 7 for a, b in zip([0, *whitespace[:-1]], whitespace)), whitespace
        assert b'<Code>InternalError</Code>' in body and b'Please retry the same request.' in body and b'<RequestId>' in body, body
        assert state(args) == original, 'HTTP timeout changed the persisted execution'
        assert len(peer.list_parts(Bucket=fixture.name, Key=args['Key'], UploadId=args['UploadId'])['Parts']) == 1
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(peer.complete_multipart_upload, **args)
            time.sleep(.5)
            assert not pending.done() and state(args)['completion_token'] == original['completion_token']
            marker.unlink()
            assert pending.result(timeout=25)['ResponseMetadata']['HTTPStatusCode'] == 200
        fixture.read(args['Key'], data)
        completed = state(args)
        assert completed['state'] == 'completed'
        peer.complete_multipart_upload(**args)
        assert state(args)['output_stream'] == completed['output_stream']
        print('PASS 5s keepalive, late XML timeout, retained parts/work, joined retry and persisted result replay', flush=True)

        args, data = prepare('disconnect')
        arm()
        response = request(args)
        assert response.status_code == 200
        next(response.iter_content(chunk_size=None))
        response.close()
        wait(marker.with_suffix('.hit').exists, 'disconnected completion did not start')
        assert state(args)['state'] == 'completing'
        marker.unlink()
        wait(lambda: state(args)['state'] == 'completed', 'disconnect canceled accepted work')
        fixture.read(args['Key'], data)
        print('PASS disconnect preserves work and foreground admission', flush=True)

        capacity = fixture.cli('status')['resources']['upload_concurrency']
        bounded = [prepare('admission-' + str(index)) for index in range(capacity + 1)]
        arm()
        with ThreadPoolExecutor(max_workers=capacity) as pool:
            active = [pool.submit(fixture.client.complete_multipart_upload, **args) for args, _ in bounded[:-1]]
            try:
                wait(lambda: all(state(args)['state'] == 'completing' for args, _ in bounded[:-1]), 'completion slots did not fill')
                excess = bounded[-1][0]
                try:
                    fixture.client.complete_multipart_upload(**excess)
                    raise AssertionError('completion quota was unbounded')
                except ClientError as error:
                    assert error.response['Error']['Code'] == 'SlowDown', error.response
                    assert error.response['ResponseMetadata']['HTTPStatusCode'] == 503
                    assert error.response['ResponseMetadata']['HTTPHeaders']['retry-after'] == '60'
                fixture.put('foreground-with-full-completions', b'foreground still admitted')
                fixture.client.upload_part(Bucket=fixture.name, Key=excess['Key'], UploadId=excess['UploadId'], PartNumber=2, Body=b'foreground part')
                fixture.read('foreground-with-full-completions', b'foreground still admitted')
                assert state(excess)['state'] == 'active'
            finally:
                marker.unlink(missing_ok=True)
            for work in active:
                assert work.result(timeout=25)['ResponseMetadata']['HTTPStatusCode'] == 200
        fixture.client.complete_multipart_upload(**bounded[-1][0])
        print('PASS bounded completion jobs reject excess work before headers and retain foreground read/write capacity', flush=True)

        if cluster:
            args, data = prepare('completion-race')
            arm()
            response = request(args)
            assert response.status_code == 200
            next(response.iter_content(chunk_size=None))
            response.close()
            wait(marker.with_suffix('.hit').exists, 'race fixture did not start')
            peer_fault = Path(os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')[1]) / 'multipart-before-selection'
            peer_fault.parent.mkdir(parents=True, exist_ok=True)
            peer_fault.with_suffix('.hit').unlink(missing_ok=True)
            peer_fault.touch()
            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    replay = pool.submit(peer.complete_multipart_upload, **args)
                    wait(peer_fault.with_suffix('.hit').exists, 'peer did not pause before selecting parts')
                    marker.unlink()
                    wait(lambda: state(args)['state'] == 'completed', 'original completion did not publish')
                    output = state(args)['output_stream']
                    peer_fault.unlink()
                    assert replay.result(timeout=20)['ResponseMetadata']['HTTPStatusCode'] == 200
                    assert state(args)['output_stream'] == output
            finally:
                peer_fault.unlink(missing_ok=True)
            print('PASS a retry racing publication replays the result after selected parts are retired', flush=True)

            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(cache_enabled=False, upload_cache=True, upload_cache_size='32MiB', pack_upload_flush_timeout='300s')
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
            held = [Path(root) / 'upload-before-work' for root in os.environ['MOKYU_TEST_SOURCE_FAULT_DIRS'].split(',')]
            for fault in held:
                fault.touch()
            try:
                args, data = prepare('pending-parts', 6 * 1024 * 1024 + 13)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    complete = pool.submit(peer.complete_multipart_upload, **args)
                    wait(lambda: state(args)['state'] == 'completing', 'pending completion did not start')
                    time.sleep(1)
                    assert not complete.done()
                    assert database.query("SELECT count(*) AS n FROM streams WHERE bucket_id=%s AND object_key=%s AND stream_type='object'", uuid.UUID(fixture.bucket_id), args['Key'])[0]['n'] == 0
                    for fault in held:
                        fault.unlink()
                    assert complete.result(timeout=25)['ResponseMetadata']['HTTPStatusCode'] == 200
                assert not database.query('''SELECT p.chunk_id FROM pending_uploads p JOIN extents e ON e.chunk_id=p.chunk_id
                    WHERE e.stream_id=%s''', state(args)['output_stream'])
                fixture.read(args['Key'], data)
            finally:
                for fault in held:
                    fault.unlink(missing_ok=True)
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile['upload_cache'] = False
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
            print('PASS Complete waits for selected local parts and final output publication without occupying assembly memory', flush=True)

        args, data = prepare('restart')
        arm()
        response = request(args)
        assert response.status_code == 200
        next(response.iter_content(chunk_size=None))
        response.close()
        wait(marker.with_suffix('.hit').exists, 'restart fixture did not start')
        original = state(args)
        if cluster:
            signals = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'replacement-control'
            signals.mkdir(exist_ok=True)
            for name in ('kill-primary', 'start-primary'):
                (signals / name).unlink(missing_ok=True)
                (signals / (name + '.done')).unlink(missing_ok=True)
            (signals / 'kill-primary').touch()
            wait(lambda: (signals / 'kill-primary.done').exists(), 'runner did not stop owner')
            database.query("UPDATE uploads SET completion_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", uuid.UUID(args['UploadId']))
        else:
            stop()
        marker.unlink()
        if not cluster:
            start()
            fixture.client = client(os.environ['MOKYU_TEST_ENDPOINT'])
            peer = client(os.environ['MOKYU_TEST_ENDPOINT'])
        peer.complete_multipart_upload(**args)
        current = state(args)
        assert current['state'] == 'completed' and current['completion_token'] != original['completion_token']
        if cluster:
            (signals / 'start-primary').touch()
            wait(lambda: (signals / 'start-primary.done').exists(), 'runner did not restart owner')
        with peer.get_object(Bucket=fixture.name, Key=args['Key'])['Body'] as body:
            assert body.read() == data
        print('PASS restart or expired execution resumes the same manifest with a new fenced token', flush=True)
    finally:
        marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
