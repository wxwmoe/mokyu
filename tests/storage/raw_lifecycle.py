"""Raw quarantine, recheck, reference rejection and metrics through real APIs."""
import os
import tomllib
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

from source_quarantine import Fixture, rejected, wait
from tests.recovery.source_repair import Database


def main():
    fixture = Fixture()
    assert fixture.backend is None, 'raw files require an isolated local backend'
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile['cdc_enabled'] = False
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    data = bytes(range(256)) * 128
    fixture.put('original', data)
    paths = fixture.keys()
    assert len(paths) == 1, paths
    physical = paths[0]
    source = str(uuid.UUID(Path(physical).name))
    database = Database()
    row, = database.query(f'SELECT id FROM raw_sources WHERE {database.raw_location}=%s', uuid.UUID(source))
    scope = dict(storage_id=fixture.store['id'], source='raw', id=str(row['id']))
    fixture.bytes(physical, bytes([data[0] ^ 1]) + data[1:])
    rejected(lambda: fixture.client.get_object(Bucket=fixture.name, Key='original'))
    old = b'preserved overwrite target'
    fixture.put('target', old)
    rejected(lambda: fixture.client.copy_object(Bucket=fixture.name, Key='target',
                         CopySource={'Bucket': fixture.name, 'Key': 'original'}))
    fixture.read('target', old)
    task = fixture.finished(fixture.cli('integrity', 'check', '--mode', 'full',
                                        '--bucket', fixture.name, '--key', 'original'))
    issues = fixture.api('/api/v1/tasks/' + task['id'] + '/issues')['items']
    assert any(item['code'] == 'hash_mismatch' and item['detail']['source'] == 'raw'
               and item['detail']['location_id'] == source for item in issues), issues
    fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
    listed = fixture.cli('integrity', 'quarantines', '--storage', fixture.store['id'], '--source', 'raw')
    assert len(listed['items']) == 1 and listed['items'][0]['id'] == scope['id'], listed
    assert listed['items'][0]['code'] == 'hash_mismatch', listed
    rejected(lambda: fixture.client.copy_object(Bucket=fixture.name, Key='target',
                         CopySource={'Bucket': fixture.name, 'Key': 'original'}))
    fixture.read('target', old)
    fixture.recheck(scope, state='failed')
    fixture.bytes(physical, data)
    rejected(lambda: fixture.client.get_object(Bucket=fixture.name, Key='original'))
    fixture.recheck(scope)
    assert not fixture.cli('integrity', 'quarantines', '--storage', fixture.store['id'], '--source', 'raw')['items']
    fixture.read('original', data)
    fixture.client.copy_object(Bucket=fixture.name, Key='recovered',
                               CopySource={'Bucket': fixture.name, 'Key': 'original'})
    fixture.read('recovered', data)
    task = fixture.finished(fixture.cli('integrity', 'check', '--mode', 'full',
                                        '--bucket', fixture.name, '--key', 'original'))
    assert not fixture.api('/api/v1/tasks/' + task['id'] + '/issues')['items']
    print('PASS raw corruption detection, quarantine, copy rejection, failed recheck and recovery', flush=True)
    before = set(fixture.keys())
    config = tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())
    staging = Path(config['instance']['data_path']) / 'multipart'
    before_parts = set(staging.iterdir())
    upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='parts')['UploadId']
    part = fixture.client.upload_part(Bucket=fixture.name, Key='parts', UploadId=upload, PartNumber=1, Body=data)
    assert set(fixture.keys()) == before, 'UploadPart wrote a final raw file'
    part_path, = set(staging.iterdir()) - before_parts
    part_path.write_bytes(b'!')
    rejected(lambda: fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='parts', UploadId=upload,
                         MultipartUpload={'Parts': [{'ETag': part['ETag'], 'PartNumber': 1}]}))
    assert fixture.client.list_parts(Bucket=fixture.name, Key='parts', UploadId=upload)['Parts'][0]['ETag'] == part['ETag']
    fixture.client.abort_multipart_upload(Bucket=fixture.name, Key='parts', UploadId=upload)
    print('PASS raw multipart reuses fragment staging, rejects damaged parts and retains the upload for abort', flush=True)
    expected = 2 * len(data) + len(old)

    def usage_ready():
        response = fixture.api('/api/v1/usage/summary?bucket=' + fixture.bucket_id)
        usage = response.get('usage')
        return usage if usage and int(usage['logical_bytes']) == expected else None

    usage = wait(usage_ready, 'raw usage summary did not refresh', seconds=90)
    assert Decimal(usage['unique_bytes']) == expected and Decimal(usage['encoded_bytes']) == expected, usage
    assert all(Decimal(usage[name]) == 0 for name in ('local_savings', 'shared_savings', 'encoding_savings')), usage
    inventory = fixture.cli('status')['storage']
    assert inventory['last_error'] is None, inventory
    assert inventory['snapshot']['live']['raw_objects'] >= 3, inventory
    print('PASS raw usage and inventory report physical bytes without deduplication or encoding savings', flush=True)
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(upload_retention='1s', unreferenced_grace='1s', min_storage_duration='0s')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    fixture.faults.mkdir(parents=True, exist_ok=True)
    marker = fixture.arm('read-body')
    with ThreadPoolExecutor(max_workers=1) as executor:
        read = executor.submit(fixture.read, 'original', data)
        try:
            wait(lambda: marker.with_suffix('.hit').exists(), 'raw reader did not reach its body')
            fixture.put('original', b'new version')
            fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
            assert Path(physical).exists(), 'GC removed a pinned reader source'
        finally:
            marker.unlink(missing_ok=True)
        read.result(timeout=30)
    fixture.read('original', b'new version')

    def reclaimed():
        fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        return not Path(physical).exists()

    wait(reclaimed, 'raw source was not collected after its reader finished', seconds=45)
    print('PASS overwrite and GC retain a pinned raw reader and reclaim it after completion', flush=True)


if __name__ == '__main__':
    main()
