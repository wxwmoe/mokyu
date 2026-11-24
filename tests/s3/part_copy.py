"""Check multipart copy selection, receipts and visibility in disposable versioned buckets."""
import base64
import hashlib
import json
import os
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def assert_missing(client, target):
    try:
        client.head_object(**target)
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    else:
        raise AssertionError("unfinished or aborted destination is visible")


def check(client, source_bucket, destination_bucket):
    prefix = "part-copy/" + uuid.uuid4().hex
    source = {"Bucket": source_bucket, "Key": prefix + "/original +.bin"}
    target = {"Bucket": destination_bucket, "Key": prefix + "/copy"}
    old = hashlib.shake_256(b"old").digest(6 * 1024 * 1024 + 13)
    first = client.put_object(**source, Body=old)
    assert first.get("VersionId")
    copy = dict(**source, VersionId=first["VersionId"])
    unit = 5 * 1024 * 1024
    upload = client.create_multipart_upload(**target, ChecksumAlgorithm="SHA256")["UploadId"]
    part = dict(**target, UploadId=upload)
    try:
        receipts = []
        for n, start, end in [(1, 0, unit), (2, unit, len(old))]:
            if n == 2:
                client.put_object(**source, Body=b"replacement")
                try:
                    client.upload_part_copy(**part, PartNumber=n, CopySource=source, CopySourceIfMatch=first["ETag"])
                except ClientError as error:
                    assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 412, error.response
                else:
                    raise AssertionError("source precondition ignored")
            out = client.upload_part_copy(**part, PartNumber=n, CopySource=copy,
                                          CopySourceRange=f"bytes={start}-{end - 1}", CopySourceIfMatch=first["ETag"])
            r = out["CopyPartResult"]
            assert out["CopySourceVersionId"] == first["VersionId"]
            assert r["ChecksumSHA256"] == base64.b64encode(hashlib.sha256(old[start:end]).digest()).decode()
            receipts.append(dict(PartNumber=n, ETag=r["ETag"], ChecksumSHA256=r["ChecksumSHA256"]))
        assert_missing(client, target)
        client.complete_multipart_upload(**part, MultipartUpload={"Parts": receipts})
        assert client.get_object(**target)["Body"].read() == old
    except BaseException:
        client.abort_multipart_upload(**part)
        raise
    upload = client.create_multipart_upload(**target)["UploadId"]
    whole = client.upload_part_copy(**target, UploadId=upload, PartNumber=1, CopySource=source)
    assert whole["CopyPartResult"]["ETag"].strip('"') == hashlib.md5(b"replacement").hexdigest()
    marker = client.delete_object(**source)["VersionId"]
    for version, code in [(marker, "InvalidRequest"), ("0" * 32, "NoSuchVersion")]:
        try:
            client.upload_part_copy(**target, UploadId=upload, PartNumber=1, CopySource=dict(**source, VersionId=version))
        except ClientError as error:
            assert error.response["Error"]["Code"] == code, error.response["Error"]
        else:
            raise AssertionError("invalid source replaced a committed part")
    persisted = client.list_parts(**target, UploadId=upload)["Parts"]
    assert len(persisted) == 1 and persisted[0]["ETag"] == whole["CopyPartResult"]["ETag"]
    client.abort_multipart_upload(**target, UploadId=upload)
    assert client.get_object(**target)["Body"].read() == old
    cancelled = {"Bucket": destination_bucket, "Key": prefix + "/cancelled"}
    upload = client.create_multipart_upload(**cancelled)["UploadId"]
    client.upload_part_copy(**cancelled, UploadId=upload, PartNumber=1, CopySource=copy, CopySourceRange="bytes=0-127")
    client.abort_multipart_upload(**cancelled, UploadId=upload)
    assert_missing(client, cancelled)
    print("multipart copy, explicit version, overwrite, conditions, hashes and abort passed", flush=True)


if __name__ == "__main__":
    assert os.environ["MOKYU_TEST_ALLOW_STATE_CHANGES"] == "isolated-only"
    credential = json.loads(Path(os.environ["MOKYU_TEST_CREDENTIALS"]).read_text())
    source = credential["bucket"]
    client = boto3.client("s3", endpoint_url=os.environ["MOKYU_TEST_ENDPOINT"], region_name="us-east-1",
                          aws_access_key_id=credential["access_key"], aws_secret_access_key=credential["secret_key"],
                          config=Config(s3={"addressing_style": "path"}))
    check(client, source, os.environ.get("MOKYU_TEST_COPY_BUCKET", source))
