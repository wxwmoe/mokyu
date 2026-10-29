"""Reject changed replay sources without publishing or losing Multipart parts."""
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    origin = Fixture()
    target = Fixture()
    assert target.backend, 'requires a disposable S3 backend'
    target.faults.mkdir(parents=True, exist_ok=True)
    database = Database()
    for fixture in (origin, target):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    target.cli('cred', 'grant', target.credentials['access_key'], origin.name, '--read-only')
    data = bytes(range(256)) * (8 * 1024 * 1024 // 256) + b'end'
    previous = b'previous durable target'
    origin.put('source', data)
    source = origin.keys()[0]
    target.put('copy', previous)

    def mutate_during_replay(request, read, write):
        marker = target.arm('raw-before-replay')
        original = read()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(request)
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or future.done(),
                     'request did not reach its replay boundary')
                assert not future.done(), future.exception()
                write(bytes([original[0] ^ 1]) + original[1:])
                marker.unlink()
                rejected(lambda: future.result(timeout=90))
            finally:
                marker.unlink(missing_ok=True)
                write(original)

    def copy():
        return target.client.copy_object(Bucket=target.name, Key='copy',
                                        CopySource={'Bucket': origin.name, 'Key': 'source'})

    def write_backend(value):
        def restored():
            try:
                origin.bytes(source, value)
            except ClientError as error:
                if error.response['Error']['Code'] == 'SlowDown':
                    return False
                raise
            return True
        # The cancelled upstream request may still be releasing its admission slot.
        wait(restored, 'backend did not accept the fixture write', seconds=10)

    mutate_during_replay(copy, lambda: origin.bytes(source), write_backend)
    target.read('copy', previous)
    copy()
    target.read('copy', data)
    print('PASS raw Copy rejects a source changed between validation and replay; retry preserves bytes', flush=True)

    target.put('multipart', previous)
    upload = target.client.create_multipart_upload(Bucket=target.name, Key='multipart')['UploadId']
    boundary = 5 * 1024 * 1024 + 13
    parts = (data[:boundary], data[:6 * 1024 * 1024 + 7], b'last part')
    manifest = []
    for number, part in enumerate(parts, start=1):
        reply = target.client.upload_part(Bucket=target.name, Key='multipart', UploadId=upload,
                                          PartNumber=number, Body=part)
        manifest.append(dict(PartNumber=number, ETag=reply['ETag']))
    rows = database.query(f'''SELECT COALESCE(f.file_id, f.{database.raw_location}) AS id
        FROM fragments f JOIN parts p ON p.stream_id=f.owner_stream
        WHERE p.upload_id=%s AND p.part_number=1 ORDER BY f.id''', uuid.UUID(upload))
    assert rows
    identity = rows[0]['id']
    identity = uuid.UUID(str(identity)) if database.type == 'postgres' else uuid.UUID(bytes=bytes(identity))
    path = Path(database.config['instance']['data_path']) / 'pending' / 'multipart' / 'candidate' / identity.hex

    def complete():
        return target.client.complete_multipart_upload(Bucket=target.name, Key='multipart', UploadId=upload,
                                                       MultipartUpload={'Parts': manifest})

    mutate_during_replay(complete, path.read_bytes, path.write_bytes)
    target.read('multipart', previous)
    listed = target.client.list_parts(Bucket=target.name, Key='multipart', UploadId=upload)['Parts']
    assert [part['ETag'] for part in listed] == [part['ETag'] for part in manifest]
    complete()
    target.read('multipart', b''.join(parts))
    print('PASS raw Multipart retains its previous object and staged parts after failed replay; retry succeeds', flush=True)


if __name__ == '__main__':
    main()
