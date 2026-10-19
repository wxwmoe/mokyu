"""Raw objects preserve S3 content, multipart, profile changes and copy semantics."""
import argparse
import base64
import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--core-only', action='store_true')
    args = parser.parse_args()
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    credentials = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS_FILE']).read_text())
    bucket = credentials['bucket']
    client = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_S3'], region_name='us-east-1',
                          aws_access_key_id=credentials['access_key'],
                          aws_secret_access_key=credentials['secret_key'],
                          config=Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 0},
                                        request_checksum_calculation='when_required',
                                        response_checksum_validation='when_required'))
    prefix = 'raw-' + uuid.uuid4().hex + '/'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
    storage = os.environ.get('MOKYU_TEST_STORAGE', 'local')

    def cli(*arguments, success=True):
        result = subprocess.run(command + list(arguments), capture_output=True, text=True, timeout=60)
        assert (result.returncode == 0) == success, (arguments[:3], result.stderr)
        return json.loads(result.stdout) if success else None

    def configure(success=True, **changes):
        profile = cli('storage', 'profile', 'show', storage)
        profile.update(changes)
        path = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'raw-test-profile.json'
        path.write_text(json.dumps(profile))
        return cli('storage', 'profile', 'set', storage, str(path), success=success)

    def read(key, expected, **options):
        response = client.get_object(Bucket=bucket, Key=prefix + key, **options)
        with response['Body'] as body:
            assert body.read() == expected, key
        assert response['ContentLength'] == len(expected), key
        return response

    def put(key, data, **options):
        return client.put_object(Bucket=bucket, Key=prefix + key, Body=data, **options)

    profile = cli('storage', 'profile', 'show', storage)
    assert not profile['cdc_enabled'] and not profile['compression_enabled']
    configure(compression_enabled=True, success=False)
    if os.environ.get('MOKYU_TEST_STORAGE_TYPE', 'local') == 'local':
        configure(cache_enabled=True, success=False)
        configure(upload_cache=True, success=False)
    configure(pack_enabled=True, success=False)
    for size in [0, 1, 4096, 1024 * 1024 + 17, 5 * 1024 * 1024 + 31]:
        data = (b'raw-object-payload-' * (size // len(b'raw-object-payload-') + 1))[:size]
        key = str(size)
        reply = put(key, data, ContentType='application/octet-stream', Metadata={'raw-test': key})
        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        assert reply['ETag'] == etag
        response = read(key, data)
        assert response['ETag'] == etag and response['Metadata']['raw-test'] == key
        head = client.head_object(Bucket=bucket, Key=prefix + key)
        assert head['ContentLength'] == len(data) and head['ETag'] == etag
        if data:
            end = min(33, len(data) - 1)
            read(key, data[:end + 1], Range=f'bytes=0-{end}')
            read(key, data[-min(31, len(data)):], Range='bytes=-31')
        try:
            client.get_object(Bucket=bucket, Key=prefix + key, IfNoneMatch=etag)
            raise AssertionError('conditional GET returned a body')
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 304
    put('overwrite', b'old')
    try:
        put('overwrite', b'invalid', ContentMD5=base64.b64encode(bytes(16)).decode())
        raise AssertionError('invalid digest was accepted')
    except ClientError as error:
        assert error.response['Error']['Code'] == 'BadDigest'
    read('overwrite', b'old')
    put('overwrite', b'new')
    read('overwrite', b'new')
    client.delete_object(Bucket=bucket, Key=prefix + 'overwrite')
    try:
        read('overwrite', b'')
        raise AssertionError('deleted object remained visible')
    except ClientError as error:
        assert error.response['Error']['Code'] == 'NoSuchKey'
    print('PASS raw PUT, empty content, HEAD, Range, conditions, failed overwrite and deletion', flush=True)
    if args.core_only:
        return

    parts = [b'first-part-' * 524288, b'last-part-data']
    key = prefix + 'multipart'
    upload = client.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
    try:
        manifest = []
        for number in [2, 1]:
            reply = client.upload_part(Bucket=bucket, Key=key, UploadId=upload,
                                       PartNumber=number, Body=parts[number - 1])
            manifest.append({'PartNumber': number, 'ETag': reply['ETag']})
        result = client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload,
                                                   MultipartUpload={'Parts': sorted(manifest, key=lambda p: p['PartNumber'])})
        assert result['ETag'].endswith('-2"')
    except Exception:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
        raise
    read('multipart', b''.join(parts))
    boundary = len(parts[0])
    read('multipart', b''.join(parts)[boundary - 7:boundary + 9], Range=f'bytes={boundary - 7}-{boundary + 8}')
    client.copy_object(Bucket=bucket, Key=prefix + 'raw-copy', CopySource={'Bucket': bucket, 'Key': key})
    read('raw-copy', b''.join(parts))
    key_name = 'raw-test-' + uuid.uuid4().hex
    cli('key', 'create', key_name, 'block', '-a', 'aes-256-gcm')
    cli('storage', 'key', 'set', storage, key_name, success=False)
    configure(cdc_enabled=True)
    cli('storage', 'key', 'set', storage, key_name)
    client.copy_object(Bucket=bucket, Key=prefix + 'chunk-copy', CopySource={'Bucket': bucket, 'Key': key})
    read('chunk-copy', b''.join(parts))
    put('chunk-original', b'chunk-data' * 8192)
    configure(cdc_enabled=False, success=False)
    cli('storage', 'key', 'clear', storage)
    configure(cdc_enabled=False)
    client.copy_object(Bucket=bucket, Key=prefix + 'converted',
                       CopySource={'Bucket': bucket, 'Key': prefix + 'chunk-original'})
    read('converted', b'chunk-data' * 8192)
    read('chunk-original', b'chunk-data' * 8192)
    read('multipart', b''.join(parts))
    print('PASS multipart boundaries, raw copy, encryption restrictions and raw/encrypted chunks conversion', flush=True)


if __name__ == '__main__':
    main()
