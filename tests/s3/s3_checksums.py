"""Additional S3 semantics; use the isolated fixture environment from integration.py."""
import base64
import hashlib
import os
import zlib
from datetime import datetime, timezone
from tests.integration import bucket, s3, read, error


def encoded(algorithm, data):
    value = (zlib.crc32(data).to_bytes(4, 'big') if algorithm == 'CRC32'
             else hashlib.new(algorithm.lower(), data).digest())
    return base64.b64encode(value).decode()


for algorithm in ['CRC32', 'SHA1', 'SHA256']:
    data = os.urandom(256 * 1024)
    field = 'Checksum' + algorithm
    out = s3.put_object(Bucket=bucket, Key='checksum', Body=data, ChecksumAlgorithm=algorithm)
    assert out[field] == encoded(algorithm, data)
    raw, out = read('checksum', ChecksumMode='ENABLED')
    assert raw == data and out[field] == encoded(algorithm, data)
    error('BadDigest', s3.put_object, Bucket=bucket, Key='checksum', Body=b'wrong',
          **{field: encoded(algorithm, data)})
    assert read('checksum')[0] == data

for algorithm, checksum_type, part_mib in [('CRC32', 'FULL_OBJECT', 5), ('SHA256', 'COMPOSITE', 16)]:
    field = 'Checksum' + algorithm
    key = 'checksum-multipart'
    chunks = [os.urandom(part_mib * 1024 * 1024), os.urandom(271_000)]
    upload = s3.create_multipart_upload(Bucket=bucket, Key=key,
                                        ChecksumAlgorithm=algorithm, ChecksumType=checksum_type)['UploadId']
    manifest = []
    for n, data in enumerate(chunks, 1):
        out = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=n,
                             Body=data, ChecksumAlgorithm=algorithm)
        assert out[field] == encoded(algorithm, data)
        manifest.append({'PartNumber': n, 'ETag': out['ETag'], field: out[field]})
    joined = b''.join(chunks)
    expected = (encoded(algorithm, joined) if checksum_type == 'FULL_OBJECT' else
                encoded(algorithm, b''.join(base64.b64decode(p[field]) for p in manifest)) + '-2')
    out = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload,
                                       MultipartUpload={'Parts': manifest}, ChecksumType=checksum_type,
                                       **{field: expected})
    assert out[field] == expected, out
    raw, out = read(key, ChecksumMode='ENABLED')
    assert raw == joined and out[field] == expected and out['ChecksumType'] == checksum_type

names = ['listing/a/1', 'listing/a/2', 'listing/b/1', 'listing/z', 'listing/z']
uploads = [(key, s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']) for key in names]
try:
    pages = list(s3.get_paginator('list_multipart_uploads').paginate(
        Bucket=bucket, Prefix='listing/', Delimiter='/', PaginationConfig={'PageSize': 1}))
    assert [p['Prefix'] for page in pages for p in page.get('CommonPrefixes', [])] == ['listing/a/', 'listing/b/']
    listed = [p['UploadId'] for page in pages for p in page.get('Uploads', [])]
    assert set(listed) == {uid for key, uid in uploads if key == 'listing/z'}, pages
    pages = list(s3.get_paginator('list_multipart_uploads').paginate(Bucket=bucket, Prefix='listing/',
                                                                   PaginationConfig={'PageSize': 1}))
    assert {p['UploadId'] for page in pages for p in page.get('Uploads', [])} == {uid for _, uid in uploads}
finally:
    for key, uid in uploads:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=uid)

assert [b['Name'] for page in s3.get_paginator('list_buckets').paginate(PaginationConfig={'PageSize': 1})
        for b in page['Buckets']] == [bucket]
error('NotImplemented', s3.get_object, Bucket=bucket, Key='checksum', PartNumber=1)
error('NotImplemented', s3.put_object, Bucket=bucket, Key='checksum', Body=b'append', WriteOffsetBytes=0)
original, _ = read('checksum')
for condition in [{'IfMatchSize': len(original)},
                  {'IfMatchLastModifiedTime': datetime.now(timezone.utc)}]:
    error('NotImplemented', s3.delete_object, Bucket=bucket, Key='checksum', **condition)
    assert read('checksum')[0] == original
key = 'conditional-delete'
first = s3.put_object(Bucket=bucket, Key=key, Body=b'first')['ETag']
second = s3.put_object(Bucket=bucket, Key=key, Body=b'second')['ETag']
for condition in [first, 'W/' + second]:
    error('PreconditionFailed', s3.delete_object, Bucket=bucket, Key=key, IfMatch=condition)
    assert read(key)[0] == b'second'
s3.delete_object(Bucket=bucket, Key=key, IfMatch=second)
error('PreconditionFailed', s3.delete_object, Bucket=bucket, Key=key, IfMatch='*')
s3.put_object(Bucket=bucket, Key=key, Body=b'third')
s3.delete_object(Bucket=bucket, Key=key, IfMatch='*')
error('NoSuchKey', s3.get_object, Bucket=bucket, Key=key)
for operation in [s3.get_object_acl, s3.put_object_acl]:
    extra = {'ACL': 'public-read'} if operation == s3.put_object_acl else {}
    error('NotImplemented', operation, Bucket=bucket, Key='checksum', VersionId='unsupported', **extra)
assert read('checksum')[0] == original
error('NotImplemented', s3.copy_object, Bucket=bucket, Key='checksum-copy',
      CopySource={'Bucket': bucket, 'Key': 'checksum'}, ChecksumAlgorithm='CRC32')
error('NotImplemented', s3.put_object, Bucket=bucket, Key='checksum', Body=b'x', ServerSideEncryption='AES256')
head = s3.head_object(Bucket=bucket, Key='checksum', Range='bytes=0-1')
assert head['ContentLength'] == 2 and head['ResponseMetadata']['HTTPStatusCode'] == 200
print('PASS explicit and composite/full checksums, multipart delimiter/ID pagination, bucket pagination and unsupported semantics', flush=True)
