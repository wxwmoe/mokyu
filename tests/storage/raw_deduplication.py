"""Raw whole-file identity, multipart staging, concurrent reuse and quarantine.

Uses the source_quarantine fixture and its isolated primary database. A disposable
S3 backend can be selected with MOKYU_TEST_BACKEND.
"""
import hashlib
import boto3
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    fixture = Fixture()
    database = Database()
    storage = uuid.UUID(fixture.store['id'])
    bucket = uuid.UUID(fixture.bucket_id)

    def configure(**changes):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(changes)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)

    def source(key, bucket_id=bucket):
        rows = database.query('''SELECT r.id,r.content_hash,r.size,r.digest,r.medium,r.upload_id
            FROM objects o JOIN raw_ranges e ON e.stream_id=o.stream_id
            JOIN raw_sources r ON r.id=e.source_id
            WHERE o.bucket_id=%s AND o.object_key=%s''', bucket_id, key if database.type == 'postgres' else key.encode())
        assert len(rows) == 1, rows
        return rows[0]

    def count(table):
        return database.query('SELECT COUNT(*) AS n FROM ' + table + ' WHERE storage_id=%s', storage)[0]['n']

    configure(cdc_enabled=False, pack_range_enabled=False, deduplication_enabled=False)
    data = bytes(range(256)) * 128
    fixture.put('off-a', data)
    fixture.put('off-b', data)
    assert source('off-a')['id'] != source('off-b')['id']
    assert source('off-a')['content_hash'] is None

    configure(deduplication_enabled=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda key: fixture.put(key, data), ['on-a', 'on-b']))
    shared = source('on-a')
    assert shared['id'] == source('on-b')['id']
    assert len(shared['content_hash']) == 32
    fixture.client.copy_object(Bucket=fixture.name, Key='on-copy',
                               CopySource={'Bucket': fixture.name, 'Key': 'on-a'})
    assert source('on-copy')['id'] == shared['id']
    different = b'!' + data[1:]
    fixture.put('different', different)
    assert source('different')['id'] != shared['id']
    print('PASS raw deduplication is optional, concurrent and shared by CopyObject', flush=True)

    large = bytes(range(256)) * (6 * 1024 * 1024 // 256 + 100)
    fixture.put('whole', large)
    whole = source('whole')
    assert whole['medium'] == ('shared_s3' if fixture.backend else 'local')
    assert whole['upload_id'] is None
    for index, boundary in enumerate([5 * 1024 * 1024, 6 * 1024 * 1024]):
        key = 'multipart-' + str(index)
        upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key=key)['UploadId']
        before = count('raw_sources')
        manifest = []
        parts = [large[:boundary], large[boundary:]]
        for number in [2, 1]:
            reply = fixture.client.upload_part(Bucket=fixture.name, Key=key, UploadId=upload,
                PartNumber=number, Body=parts[number - 1])
            manifest.append(dict(PartNumber=number, ETag=reply['ETag']))
        assert count('raw_sources') == before, 'UploadPart created a final raw source'
        assert count('chunks') == 0, 'raw UploadPart ran CDC'
        listed = fixture.client.list_parts(Bucket=fixture.name, Key=key, UploadId=upload)['Parts']
        assert [part['PartNumber'] for part in listed] == [1, 2]
        result = fixture.client.complete_multipart_upload(Bucket=fixture.name, Key=key, UploadId=upload,
            MultipartUpload={'Parts': sorted(manifest, key=lambda part: part['PartNumber'])})
        assert result['ETag'].endswith('-2"')
        current = source(key)
        assert current['id'] == whole['id'] and current['size'] == len(large)
        assert bytes(current['digest']) == hashlib.md5(large).digest()
        fixture.read(key, large)
    print('PASS raw Multipart reuses staging and deduplicates complete content across part boundaries', flush=True)

    logical = 6 * len(data) + 3 * len(large)
    unique = 4 * len(data) + len(large)

    def usage_ready():
        usage = fixture.api('/api/v1/usage/summary?bucket=' + fixture.bucket_id).get('usage')
        return usage if usage and int(usage['logical_bytes']) == logical else None

    usage = wait(usage_ready, 'raw deduplication usage did not refresh', seconds=90)
    assert Decimal(usage['unique_bytes']) == unique, usage
    assert Decimal(usage['encoded_bytes']) == unique, usage
    assert Decimal(usage['local_savings']) == logical - unique, usage
    assert Decimal(usage['shared_savings']) == Decimal(usage['encoding_savings']) == 0, usage

    other = fixture.cli('bucket', 'create', fixture.name + '-shared', '--storage', str(storage))
    credentials = fixture.cli('cred', 'create', other['name'])
    client = boto3.client('s3', endpoint_url=fixture.client.meta.endpoint_url,
        region_name='us-east-1', aws_access_key_id=credentials['access_key'],
        aws_secret_access_key=credentials['secret_key'], config=fixture.client.meta.config)
    client.put_object(Bucket=other['name'], Key='shared', Body=data)
    assert source('shared', uuid.UUID(other['id']))['id'] == shared['id']

    def shared_usage():
        value = fixture.api('/api/v1/usage/summary?bucket=' + other['id']).get('usage')
        return value if value and Decimal(value['shared_savings']) == len(data) / 2 else None

    usage = wait(shared_usage, 'raw shared attribution did not refresh', seconds=90)
    assert Decimal(usage['unique_bytes']) == len(data), usage
    assert Decimal(usage['attributed_raw_bytes']) == len(data) / 2, usage
    print('PASS raw usage counts unique files and attributes cross-bucket sharing', flush=True)

    identity = uuid.UUID(bytes=bytes(shared['id'])) if database.type != 'postgres' else uuid.UUID(str(shared['id']))
    relative = 'raw/' + identity.hex[:2] + '/' + identity.hex
    physical = fixture.prefix + relative if fixture.backend else str(fixture.root / relative)
    fixture.bytes(physical, different)
    scope = dict(storage_id=str(storage), source='raw', id=str(identity))
    fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
    fixture.put('retransmitted', data)
    recovered = source('retransmitted')
    assert recovered['id'] != shared['id'], 'deduplication reused a quarantined source'
    fixture.read('retransmitted', data)
    rejected(lambda: fixture.client.get_object(Bucket=fixture.name, Key='on-a'))
    fixture.put('healthy-copy', data)
    assert source('healthy-copy')['id'] == recovered['id']
    configure(deduplication_enabled=False)
    fixture.put('disabled-again', data)
    assert source('disabled-again')['id'] != recovered['id']
    fixture.read('healthy-copy', data)
    print('PASS bad sources are excluded and disabling deduplication preserves existing references', flush=True)


if __name__ == '__main__':
    main()
