"""Own an isolated service to verify growing local raw Multipart files and recovery."""
import hashlib
import argparse
import json
import os
import subprocess
import stat
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cross-filesystem', action='store_true')
    arguments = parser.parse_args()
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    assert not os.environ.get('MOKYU_TEST_BACKEND'), 'local storage required'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'local-multipart.log').open('a')
    process = None

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect local-multipart.log'
            try:
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True,
                                      text=True, timeout=5).returncode == 0
            except subprocess.TimeoutExpired:
                return False

        wait(ready, 'service did not recover', seconds=45)

    try:
        start()
        fixture = Fixture()
        database = Database()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False,
                       min_free_space='16MiB', upload_retention='1s', deleted_chunk_retention='1s', unreferenced_grace='1s', min_storage_duration='0s')
        profile = fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        root = Path(database.config['instance']['data_path']) / 'multipart'
        source_ids = set()
        acknowledged = {}

        def read(key, expected):
            fixture.read(key, expected)
            acknowledged[key] = len(expected), hashlib.sha256(expected).digest()

        def identifier(value):
            return uuid.UUID(str(value)) if database.type == 'postgres' else uuid.UUID(bytes=bytes(value))

        def files(upload):
            rows = database.query('SELECT id,state,source_id FROM multipart_files WHERE upload_id=%s ORDER BY id', uuid.UUID(upload))
            for row in rows:
                if row['source_id'] is None:
                    row['path'] = root / identifier(row['id']).hex
                else:
                    ident = identifier(row['source_id']).hex
                    row['path'] = (fixture.root / 'raw' / ident[:2] / ident).with_suffix('.partial')
                source_ids.add(row['path'])
            return rows

        def create(key):
            return fixture.client.create_multipart_upload(Bucket=fixture.name, Key=key)['UploadId']

        def part(key, upload, number, data):
            response = fixture.client.upload_part(Bucket=fixture.name, Key=key, UploadId=upload,
                                                  PartNumber=number, Body=data)
            assert response['ETag'] == '"' + hashlib.md5(data).hexdigest() + '"'
            return dict(PartNumber=number, ETag=response['ETag'])

        def complete(key, upload, manifest, expected):
            reply = fixture.client.complete_multipart_upload(Bucket=fixture.name, Key=key, UploadId=upload,
                MultipartUpload={'Parts': manifest}, MpuObjectSize=len(expected))
            read(key, expected)
            return reply

        def raw_path(key):
            row, = database.query('''SELECT r.source_id AS id FROM objects o JOIN raw_ranges r ON r.stream_id=o.stream_id
                WHERE o.bucket_id=%s AND o.object_key=%s''',
                uuid.UUID(fixture.bucket_id), key if database.type == 'postgres' else key.encode())
            ident = identifier(row['id']).hex
            return fixture.root / 'raw' / ident[:2] / ident

        data = [os.urandom(5 * 1024 * 1024 + 3), os.urandom(5 * 1024 * 1024 + 5), b'last part']
        upload = create('sequential')
        manifest = []
        for number, body in enumerate(data, start=1):
            manifest.append(part('sequential', upload, number, body))
            row, = files(upload)
            assert row['state'] == 'open'
            assert row['path'].stat().st_size == sum(map(len, data[:number]))
        staging = row['path']
        inode = staging.stat().st_ino
        device = staging.stat().st_dev
        complete('sequential', upload, manifest, b''.join(data))
        assert stat.S_IMODE(raw_path('sequential').stat().st_mode) == 0o644
        if arguments.cross_filesystem:
            assert raw_path('sequential').stat().st_dev != device
        else:
            assert raw_path('sequential').stat().st_ino == inode, 'continuous layout was copied'
            assert raw_path('sequential').stat().st_dev == device
        boundary = len(data[0])
        with fixture.client.get_object(Bucket=fixture.name, Key='sequential',
                Range=f'bytes={boundary - 3}-{boundary + 6}')['Body'] as body:
            assert body.read() == b''.join(data)[boundary - 3:boundary + 7]
        print('PASS local Multipart grows by received bytes, preserves cross-part Range and ' +
              ('copies across filesystems' if arguments.cross_filesystem else 'reuses its inode'), flush=True)

        upload = create('selection')
        part('selection', upload, 2, data[1])
        part('selection', upload, 3, b'unselected part')
        first = part('selection', upload, 1, data[0])
        last = part('selection', upload, 2, b'replacement')
        staging = files(upload)[0]['path']
        old_identity = (staging.stat().st_dev, staging.stat().st_ino)
        complete('selection', upload, [first, last], data[0] + b'replacement')
        assert stat.S_IMODE(raw_path('selection').stat().st_mode) == 0o644
        assert (raw_path('selection').stat().st_dev, raw_path('selection').stat().st_ino) != old_identity
        print('PASS out-of-order, replaced and unselected parts produce the exact requested layout', flush=True)

        upload = create('cancelled-part')
        first = part('cancelled-part', upload, 1, data[0])
        marker = fixture.arm('multipart-file-synced')
        with ThreadPoolExecutor(max_workers=1) as executor:
            request = executor.submit(part, 'cancelled-part', upload, 2, data[1])
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or request.done(), 'append did not reach sync barrier')
                assert not request.done(), request.exception()
                stop(crash=True)
            finally:
                marker.unlink(missing_ok=True)
            try:
                request.result(timeout=20)
            except Exception:
                pass
            else:
                raise AssertionError('uncommitted part was acknowledged')
        start()
        assert [p['PartNumber'] for p in fixture.client.list_parts(Bucket=fixture.name,
            Key='cancelled-part', UploadId=upload)['Parts']] == [1]
        last = part('cancelled-part', upload, 2, b'retried')
        files(upload)
        complete('cancelled-part', upload, [first, last], data[0] + b'retried')
        print('PASS restart preserves acknowledged ranges and ignores an uncommitted append', flush=True)

        for point in ('raw-file-durable', 'raw-file-published'):
            key = 'restart-' + point
            fixture.put(key, b'old object')
            upload = create(key)
            manifest = [part(key, upload, 1, data[0]), part(key, upload, 2, b'original tail')]
            staging = files(upload)[0]['path']
            marker = fixture.arm(point)
            with ThreadPoolExecutor(max_workers=1) as executor:
                request = executor.submit(complete, key, upload, manifest, data[0] + b'original tail')
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or request.done(), 'Complete did not reach ' + point)
                    assert not request.done(), request.exception()
                    stop(crash=True)
                finally:
                    marker.unlink(missing_ok=True)
                try:
                    request.result(timeout=20)
                except Exception:
                    pass
                else:
                    raise AssertionError('crashed Complete was acknowledged')
            start()
            read(key, b'old object')
            previous = files(upload)
            assert all(row['state'] == 'sealed' for row in previous)
            digests = {row['path']: hashlib.sha256(row['path'].read_bytes()).digest()
                       for row in previous if row['path'].exists()}
            manifest[1] = part(key, upload, 2, b'new tail')
            assert sum(row['state'] == 'open' for row in files(upload)) == 1
            for path, digest in digests.items():
                if path.exists():
                    assert hashlib.sha256(path.read_bytes()).digest() == digest, 'sealed file was reopened for appends'
            complete(key, upload, manifest, data[0] + b'new tail')
            print('PASS sealed Multipart remains immutable across ' + point + ' and replacement retry', flush=True)

        abandoned = create('aborted')
        part('aborted', abandoned, 1, b'abandoned content')
        files(abandoned)
        fixture.client.abort_multipart_upload(Bucket=fixture.name, Key='aborted', UploadId=abandoned)

        checkpoint_data = [os.urandom(16 * 1024 * 1024) for _ in range(3)] + [b'checkpoint tail']
        checkpoints = [(point, False) for point in
            ('multipart-prefix-durable', 'multipart-prefix-remapped', 'multipart-prefix-reclaimed')]
        checkpoints.append(('multipart-prefix-remapped', True))
        for point, aborted in checkpoints:
            key = point + ('-abort' if aborted else '')
            fixture.put(key, b'previous object')
            upload = create(key)
            manifest = [None] * len(checkpoint_data)
            for index in reversed(range(len(checkpoint_data))):
                manifest[index] = part(key, upload, index + 1, checkpoint_data[index])
            originals = files(upload)
            before = sum(row['path'].stat().st_blocks * 512 for row in originals)
            marker = fixture.arm(point)
            with ThreadPoolExecutor(max_workers=1) as executor:
                request = executor.submit(complete, key, upload, manifest, b''.join(checkpoint_data))
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or request.done(), 'copy did not reach ' + point)
                    assert not request.done(), request.exception()
                    row, = database.query('''SELECT r.id FROM raw_sources r JOIN streams s ON s.id=r.owner_stream
                        WHERE r.storage_id=%s AND s.object_key=%s AND r.state='writing' ''',
                        uuid.UUID(fixture.store['id']), key if database.type == 'postgres' else key.encode())
                    ident = identifier(row['id']).hex
                    checkpoint_path = (fixture.root / 'raw' / ident[:2] / ident).with_suffix('.partial')
                    source_ids.add(checkpoint_path)
                    # One copying batch may lead one confirmation batch; each is below 20 MiB.
                    wait(lambda: checkpoint_path.stat().st_size >= 32 * 1024 * 1024,
                         'copy did not fill its second batch', seconds=10)
                    time.sleep(.25)
                    assert checkpoint_path.stat().st_size <= 40 * 1024 * 1024
                    after = sum(row['path'].stat().st_blocks * 512 for row in originals)
                    print(json.dumps(dict(checkpoint=point, source_allocated_before=before,
                        source_allocated_after=after, destination_allocated=checkpoint_path.stat().st_blocks * 512,
                        destination_length=checkpoint_path.stat().st_size)), flush=True)
                    if point == 'multipart-prefix-reclaimed':
                        assert before - after >= 12 * 1024 * 1024, (before, after)
                    stop(crash=True)
                finally:
                    marker.unlink(missing_ok=True)
                try:
                    request.result(timeout=20)
                except Exception:
                    pass
                else:
                    raise AssertionError('interrupted copy was acknowledged')
            start()
            read(key, b'previous object')
            assert len(fixture.client.list_parts(Bucket=fixture.name, Key=key, UploadId=upload)['Parts']) == 4
            files(upload)
            if aborted:
                fixture.client.abort_multipart_upload(Bucket=fixture.name, Key=key, UploadId=upload)
                read(key, b'previous object')
                print('PASS Abort releases recovered checkpoint references and preserves the previous object', flush=True)
                continue
            manifest[-1] = part(key, upload, 4, b'retried tail')
            complete(key, upload, manifest, b''.join(checkpoint_data[:-1]) + b'retried tail')
            print('PASS bounded confirmation and replacement retry across ' + point, flush=True)

        if fault_file := os.environ.get('MOKYU_TEST_FSYNC_FAULT'):
            assert os.environ.get('LD_PRELOAD'), 'fsync test requires its Linux interposer'
            fault = Path(fault_file)
            trace = Path(os.environ['MOKYU_TEST_FSYNC_TRACE'])
            for mode in ('f', 'd'):
                key = 'checkpoint-sync-' + mode
                fixture.put(key, b'previous object')
                upload = create(key)
                last = part(key, upload, 2, data[1])
                first = part(key, upload, 1, data[0])
                marker = fixture.arm('multipart-prefix-before-sync')
                with ThreadPoolExecutor(max_workers=1) as executor:
                    request = executor.submit(complete, key, upload, [first, last], data[0] + data[1])
                    try:
                        wait(lambda: marker.with_suffix('.hit').exists() or request.done(), 'copy did not reach sync')
                        assert not request.done(), request.exception()
                        trace.write_text('')
                        fault.write_text(mode)
                        marker.unlink()
                        try:
                            request.result(timeout=30)
                        except ClientError:
                            pass
                        else:
                            raise AssertionError('failed confirmation was acknowledged')
                        assert mode + ' 1' in trace.read_text().splitlines()
                    finally:
                        marker.unlink(missing_ok=True)
                        fault.unlink(missing_ok=True)
                assert all(row['source_id'] is None for row in files(upload))
                read(key, b'previous object')
                complete(key, upload, [first, last], data[0] + data[1])
            print('PASS failed file/directory confirmation retains every original source and retries', flush=True)

        profile['deleted_chunk_retention'] = '1h'
        profile = fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        upload = create('deferred-reclaim')
        last = part('deferred-reclaim', upload, 2, b'final')
        part('deferred-reclaim', upload, 3, data[1])
        first = part('deferred-reclaim', upload, 1, data[0])
        row, = files(upload)
        original = row['path']
        original_id = identifier(row['id'])
        before = original.stat().st_blocks * 512
        marker = fixture.arm('local-cleanup')
        try:
            complete('deferred-reclaim', upload, [first, last], data[0] + b'final')
            wait(lambda: marker.with_suffix('.hit').exists(), 'background reclaim did not start')
            assert database.query('SELECT reclaim_pending FROM multipart_files WHERE id=%s', original_id)[0]['reclaim_pending']
            assert original.stat().st_blocks * 512 == before
            stop(crash=True)
        finally:
            marker.unlink(missing_ok=True)
        start()

        def compacted():
            rows = database.query('SELECT reclaim_pending FROM multipart_files WHERE id=%s', original_id)
            return (not rows or not rows[0]['reclaim_pending']) and (not original.exists() or
                original.stat().st_blocks * 512 < before - 4 * 1024 * 1024)

        wait(compacted, 'restart did not resume deferred reclaim', seconds=30)
        read('deferred-reclaim', data[0] + b'final')
        print('PASS Complete and reads do not wait for final reclaim; restart resumes its durable marker', flush=True)
        profile['deleted_chunk_retention'] = '1s'
        profile = fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

        if space_file := os.environ.get('MOKYU_TEST_SPACE_FAULT'):
            space = Path(space_file)
            key = 'checkpoint-low-space'
            fixture.put(key, b'previous object')
            upload = create(key)
            manifest = [None] * len(checkpoint_data)
            for index in reversed(range(len(checkpoint_data))):
                manifest[index] = part(key, upload, index + 1, checkpoint_data[index])
            marker = fixture.arm('multipart-prefix-before-sync')
            with ThreadPoolExecutor(max_workers=1) as executor:
                request = executor.submit(complete, key, upload, manifest, b''.join(checkpoint_data))
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or request.done(), 'copy did not reach confirmation')
                    assert not request.done(), request.exception()
                    space.write_text('low\n')
                    assert os.statvfs(fixture.root).f_bavail == 0, 'space interposer is inactive'
                    marker.unlink()
                    try:
                        request.result(timeout=30)
                    except ClientError:
                        pass
                    else:
                        raise AssertionError('write bypassed the free-space floor')
                finally:
                    marker.unlink(missing_ok=True)
                    space.unlink(missing_ok=True)
            files(upload)
            read(key, b'previous object')
            complete(key, upload, manifest, b''.join(checkpoint_data))
            print('PASS free-space backpressure preserves confirmed and original ranges for retry', flush=True)

        def reclaimed():
            fixture.finished(fixture.cli('maintenance', 'run', 'cleanup'))
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            remaining = database.query('SELECT COUNT(*) AS n FROM multipart_files WHERE storage_id=%s', uuid.UUID(fixture.store['id']))[0]['n']
            return remaining == 0 and not any(path.exists() for path in source_ids)

        wait(reclaimed, 'Multipart staging was not reclaimed', seconds=90)
        for key, (size, digest) in acknowledged.items():
            total = 0
            hasher = hashlib.sha256()
            with fixture.client.get_object(Bucket=fixture.name, Key=key)['Body'] as body:
                while piece := body.read(1024 * 1024):
                    total += len(piece)
                    hasher.update(piece)
            assert total == size and hasher.digest() == digest, key
        print(f'PASS staged-file GC preserves all {len(acknowledged)} acknowledged objects by length and SHA256', flush=True)
    finally:
        stop()
        log.close()


if __name__ == '__main__':
    main()
