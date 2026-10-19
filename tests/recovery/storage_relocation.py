"""Own a stopped isolated service and relocate one namespace local -> S3 -> local.

MOKYU_TEST_RELOCATION_BACKEND names an isolated S3 fixture credential file.
The service database, deployment key, storage ID and bucket remain unchanged.
"""
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.config import Config

from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    assert 'MOKYU_TEST_BACKEND' not in os.environ, 'start with a local fixture'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    root = Path(os.environ['MOKYU_TEST_CONFIG']).parent
    log = (root / 'relocation.log').open('a')
    process = None

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)

        def ready():
            assert process.poll() is None, 'service exited; inspect relocation.log'
            try:
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                      timeout=5).returncode == 0
            except subprocess.TimeoutExpired:
                return False

        wait(ready, 'service did not recover', seconds=45)

    try:
        start()
        subprocess.run(command + ['cli', 'maintenance', 'disable'], check=True, capture_output=True)
        fixture = Fixture()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, deduplication_enabled=True)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        payloads = {'raw': b'raw namespace relocation\n' * 8192, 'empty': b''}
        for key, data in payloads.items():
            fixture.put(key, data)
        assert not (fixture.root / 'raw/meta.json').exists()
        assert (fixture.root / 'meta.json').is_file()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=True)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        block_key = fixture.name + '-key'
        fixture.cli('key', 'create', block_key, 'block', '-a', 'aes-256-gcm')
        fixture.cli('storage', 'key', 'set', fixture.name, block_key)
        payloads['encrypted'] = b'historical encrypted chunks\n' * 65536
        fixture.put('encrypted', payloads['encrypted'])
        fixture.cli('storage', 'key', 'clear', fixture.name)
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        backend = json.loads(Path(os.environ['MOKYU_TEST_RELOCATION_BACKEND']).read_text())
        remote = boto3.client('s3', endpoint_url=backend['endpoint'], region_name=backend['region'],
                              aws_access_key_id=backend['access_key'], aws_secret_access_key=backend['secret_key'],
                              config=Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 3},
                                            request_checksum_calculation='when_required'))
        prefix = backend.get('prefix', '').strip('/') + '/' + fixture.name + '-relocated/'
        prefix = prefix.lstrip('/')
        arguments = ('storage', 'update', fixture.name, '--type', 's3', '--endpoint', backend['endpoint'],
                     '--bucket', backend['bucket'], '--region', backend['region'], '--prefix', prefix.rstrip('/'),
                     '--access', backend['access_key'], '--secret', backend['secret_key'])
        def relocation_rejected(code):
            def check():
                reply = fixture.session.put(fixture.web + '/api/v1/storages/' + fixture.name,
                    headers=fixture.headers, json=dict(type='s3', endpoint=backend['endpoint'], bucket=backend['bucket'],
                    region=backend['region'], prefix=prefix.rstrip('/'), access_key=backend['access_key'],
                    secret_key=backend['secret_key']), timeout=30)
                # Startup cleanup can briefly own the relocation guard.
                if code != 'StorageNotDrained' and reply.status_code == 409 and reply.json()['code'] == 'StorageNotDrained':
                    return False
                assert reply.status_code == 409 and reply.json()['code'] == code, (reply.status_code, reply.text)
                return True
            wait(check, 'cleanup did not release the relocation guard', seconds=10)

        relocation_rejected('StorageNotDrained')
        assert fixture.cli('storage', 'show', fixture.name)['type'] == 'local'

        # Copy only after quiescing this fixture; no physical deletion is needed.
        fixture.cli('maintenance', 'enable')
        relocation_rejected('StorageVerificationFailed')
        for path in fixture.root.rglob('*'):
            if path.is_file():
                with path.open('rb') as body:
                    remote.put_object(Bucket=backend['bucket'], Key=prefix + path.relative_to(fixture.root).as_posix(), Body=body)
        marker_key = prefix + 'meta.json'
        identity = (fixture.root / 'meta.json').read_bytes()
        invalid = json.loads(identity)
        invalid['storage_id'] = '00000000-0000-0000-0000-000000000000'
        remote.put_object(Bucket=backend['bucket'], Key=marker_key, Body=json.dumps(invalid).encode())
        relocation_rejected('StorageVerificationFailed')
        remote.put_object(Bucket=backend['bucket'], Key=marker_key, Body=identity)
        fixture.cli('maintenance', 'disable')
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='unfinished')['UploadId']
        fixture.cli('maintenance', 'enable')
        relocation_rejected('StorageNotDrained')
        assert fixture.cli('storage', 'show', fixture.name)['type'] == 'local'
        assert not fixture.cli('status')['mode_mismatch']
        fixture.cli('maintenance', 'disable')
        fixture.client.abort_multipart_upload(Bucket=fixture.name, Key='unfinished', UploadId=upload)
        fixture.cli('maintenance', 'enable')
        marker = fixture.arm('raw-before-verify')
        with ThreadPoolExecutor(max_workers=1) as executor:
            reading = executor.submit(fixture.read, 'raw', payloads['raw'])
            try:
                wait(lambda: marker.with_suffix('.hit').exists(), 'read did not pin its raw source')
                relocation_rejected('StorageNotDrained')
            finally:
                marker.unlink(missing_ok=True)
            reading.result(timeout=30)
        print('PASS root marker alone identifies raw; relocation rejects live service, empty target, active upload and pinned read', flush=True)

        def crash_at(point, args):
            marker = fixture.arm(point)
            with ThreadPoolExecutor(max_workers=1) as executor:
                changing = executor.submit(fixture.cli, *args)
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or changing.done(), 'relocation did not reach ' + point)
                    assert not changing.done(), changing.exception()
                    stop(crash=True)
                finally:
                    marker.unlink(missing_ok=True)
                try:
                    changing.result(timeout=20)
                except Exception:
                    pass
                else:
                    raise AssertionError('crashed relocation returned success')
            start()

        crash_at('storage-relocating', arguments)
        assert fixture.cli('storage', 'show', fixture.name)['type'] == 'local'
        for key, data in payloads.items():
            fixture.read(key, data)
        print('PASS crash before relocation commit preserves the original namespace', flush=True)
        updated = fixture.cli(*arguments)
        assert updated['id'] == fixture.store['id'] and updated['type'] == 's3'
        assert fixture.cli('status')['mode_mismatch']
        rejected(lambda: fixture.read('raw', payloads['raw']))
        stop()
        start()
        assert fixture.cli('storage', 'show', fixture.name)['id'] == fixture.store['id']
        for key, data in payloads.items():
            fixture.read(key, data)
        assert fixture.client.get_object(Bucket=fixture.name, Key='raw', Range='bytes=3-39')['Body'].read() == payloads['raw'][3:40]
        fixture.cli('maintenance', 'disable')
        fixture.put('deduplicated', payloads['raw'])
        payloads['deduplicated'] = payloads['raw']
        payloads['remote-write'] = b'new remote raw bytes'
        fixture.put('remote-write', payloads['remote-write'])
        print('PASS local to S3 preserves storage ID, raw and encrypted chunks; restart fences old connections and permits new writes', flush=True)

        fixture.cli('maintenance', 'enable')
        destination = fixture.root.with_name(fixture.name + '-returned')
        destination.mkdir()
        for page in remote.get_paginator('list_objects_v2').paginate(Bucket=backend['bucket'], Prefix=prefix):
            for item in page.get('Contents', []):
                relative = item['Key'][len(prefix):]
                assert relative and '..' not in Path(relative).parts
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                with remote.get_object(Bucket=backend['bucket'], Key=item['Key'])['Body'] as body, target.open('wb') as output:
                    for data in iter(lambda: body.read(256 * 1024), b''):
                        output.write(data)
        assert not (destination / 'raw/meta.json').exists()
        returning = ('storage', 'update', fixture.name, '--type', 'local', '--path', str(destination), '--prefix', '')
        crash_at('storage-relocated', returning)
        assert fixture.cli('storage', 'show', fixture.name)['type'] == 'local'
        for key, data in payloads.items():
            fixture.read(key, data)
        fixture.cli('maintenance', 'disable')
        fixture.put('returned-write', b'new local raw bytes')
        fixture.read('returned-write', b'new local raw bytes')
        print('PASS crash after S3 to local commit resumes the new namespace without losing metadata, encryption or raw content', flush=True)
    finally:
        stop()
        log.close()


if __name__ == '__main__':
    main()
