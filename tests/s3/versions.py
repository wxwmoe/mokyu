"""Check version identity across overwrites and delete markers in a disposable bucket."""
import json
import os
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
credential = json.loads(Path(os.environ["MOKYU_TEST_CREDENTIALS"]).read_text())
client = boto3.client(
    "s3", endpoint_url=os.environ["MOKYU_TEST_ENDPOINT"], region_name="us-east-1",
    aws_access_key_id=credential["access_key"], aws_secret_access_key=credential["secret_key"],
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
bucket = credential["bucket"]
key = "history/" + uuid.uuid4().hex
client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
target = {"Bucket": bucket, "Key": key}
try:
    first = client.put_object(**target, Body=b"first")["VersionId"]
    second = client.put_object(**target, Body=b"second")["VersionId"]
    deleted = client.delete_object(**target)
    assert deleted["DeleteMarker"] and deleted["VersionId"] not in (first, second)
    try:
        client.head_object(**target)
        raise AssertionError("delete marker exposed a body")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert error.response["ResponseMetadata"]["HTTPHeaders"]["x-amz-delete-marker"] == "true"
    with client.get_object(**target, VersionId=first, Range="bytes=1-3")["Body"] as body:
        assert body.read() == b"irs"
    source = dict(target, VersionId=first)
    restored = client.copy_object(**target, CopySource=source)
    assert restored["CopySourceVersionId"] == first and restored["VersionId"] not in (first, second)
    with client.get_object(**target)["Body"] as body:
        assert body.read() == b"first"
finally:
    rows = client.list_object_versions(Bucket=bucket, Prefix=key)
    for row in rows.get("Versions", []) + rows.get("DeleteMarkers", []):
        client.delete_object(**target, VersionId=row["VersionId"])
