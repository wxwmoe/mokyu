"""Run before and after restarting the same isolated testing installation."""
import argparse
import base64
import hashlib
import json
import os
import secrets
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def check_error(operation, statuses):
    try:
        operation()
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] in statuses, error.response
    else:
        raise AssertionError("S3 accepted an invalid request")


def client(endpoint, region="us-east-1", secret="mokyu-testing-secret"):
    return boto3.client("s3", endpoint_url=endpoint, region_name=region,
                        aws_access_key_id="MOKYUTESTING00000001", aws_secret_access_key=secret,
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      retries={"total_max_attempts": 1}))


def run(args):
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    api = client(args.endpoint)
    bucket = "test"
    if args.phase == "write":
        key = "install-" + secrets.token_hex(8)
        data = secrets.token_bytes(65001)
        result = api.put_object(Bucket=bucket, Key=key, Body=data, Metadata={"fixture": "restart"})
        etag = result["ETag"]
        assert api.get_object(Bucket=bucket, Key=key, IfMatch=etag)["Body"].read() == data
        assert api.get_object(Bucket=bucket, Key=key, Range="bytes=13-87")["Body"].read() == data[13:88]
        api.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")
        grants = api.get_object_acl(Bucket=bucket, Key=key)["Grants"]
        assert any(g["Permission"] == "READ" and g["Grantee"].get("URI","").endswith("/AllUsers") for g in grants)
        api.put_object_acl(Bucket=bucket, Key=key, ACL="private")
        live = {"Bucket": bucket, "Key": key+"-live"}
        earlier = secrets.token_bytes(3*1024*1024)
        first = api.put_object(**live, Body=earlier)
        stream = api.get_object(**live)["Body"]
        assert stream.read(37) == earlier[:37]
        api.put_object(**live, Body=data, IfMatch=first["ETag"])
        assert stream.read() == earlier[37:]
        stream.close()
        cancelled = api.get_object(**live)["Body"]
        cancelled.read(1)
        cancelled.close()
        assert api.get_object(**live)["Body"].read() == data
        listing = api.list_objects_v2(Bucket=bucket, Prefix=live["Key"])
        assert [entry["Key"] for entry in listing["Contents"]] == [live["Key"]]
        api.delete_object(**live)
        check_error(lambda: api.get_object(Bucket=bucket, Key=key, IfNoneMatch=etag), {304})
        check_error(lambda: client(args.endpoint, secret="invalid").head_object(Bucket=bucket, Key=key), {400, 403})
        check_error(lambda: client(args.endpoint, region="ap-northeast-1").head_object(Bucket=bucket, Key=key), {400, 403})
        check_error(lambda: api.put_object(Bucket="private-other", Key=key, Body=data), {403, 404})
        upload = api.create_multipart_upload(Bucket=bucket, Key=key+"-parts")["UploadId"]
        part = api.upload_part(Bucket=bucket, Key=key+"-parts", UploadId=upload, PartNumber=1, Body=data)["ETag"]
        args.state.write_text(json.dumps([key, base64.b64encode(data).decode(), etag, upload, part]))
    else:
        key, encoded, etag, upload, part = json.loads(args.state.read_text())
        data = base64.b64decode(encoded)
        assert api.head_object(Bucket=bucket, Key=key)["Metadata"] == {"fixture": "restart"}
        assert api.get_object(Bucket=bucket, Key=key, IfMatch=etag)["Body"].read() == data
        parts = dict(Bucket=bucket, Key=key+"-parts", UploadId=upload)
        assert [item["ETag"] for item in api.list_parts(**parts)["Parts"]] == [part]
        api.complete_multipart_upload(**parts, MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part}]})
        assert api.get_object(Bucket=bucket, Key=key+"-parts")["Body"].read() == data
        api.copy_object(Bucket=bucket, Key=key+"-copy", CopySource={"Bucket":bucket, "Key":key})
        assert api.get_object(Bucket=bucket, Key=key+"-copy")["Body"].read() == data
        check_error(lambda: api.put_object(Bucket=bucket,Key=key,Body=data,ContentMD5=base64.b64encode(hashlib.md5(b"bad").digest()).decode()),{400})
        assert api.get_object(Bucket=bucket, Key=key)["Body"].read() == data
        for name in (key,key+"-parts",key+"-copy"):
            api.delete_object(Bucket=bucket, Key=name)
            check_error(lambda: api.head_object(Bucket=bucket, Key=name),{404})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["write", "read"])
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--state", type=Path, required=True)
    run(parser.parse_args())
