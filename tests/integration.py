"""S3/HTTP contract checks against an isolated gateway; never point at production.

Requires boto3 and requests. MOKYU_TEST_CREDENTIALS points to the JSON returned by
`cli credential create`; that credential must only grant the empty test bucket.
"""
import base64
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote, parse_qsl, urlencode, urlsplit, urlunsplit

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint = os.environ["MOKYU_TEST_ENDPOINT"]
credentials = json.loads(Path(os.environ["MOKYU_TEST_CREDENTIALS"]).read_text())
bucket = credentials["bucket"]
config = Config(signature_version="s3v4", s3={"addressing_style": "path"},
                retries={"max_attempts": 0}, request_checksum_calculation="when_required",
                response_checksum_validation="when_required")
s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                  aws_access_key_id=credentials["access_key"],
                  aws_secret_access_key=credentials["secret_key"], config=config)


def error(code, fn, **kwargs):
    try:
        fn(**kwargs)
    except ClientError as exc:
        assert str(exc.response["Error"]["Code"]) == str(code), exc.response
        return
    raise AssertionError(f"expected {code}")


def read(key, **kwargs):
    response = s3.get_object(Bucket=bucket, Key=key, **kwargs)
    with response["Body"] as body:
        return body.read(), response


def ordinary():
    for key, data in [("empty", b""), ("space +/\u4e2d\u6587%2F.jpg", b"abc"),
                      ("nested/random", os.urandom(9 * 1024 * 1024 + 17))]:
        digest = base64.b64encode(hashlib.md5(data).digest()).decode()
        response = s3.put_object(Bucket=bucket, Key=key, Body=data, ContentMD5=digest,
                                 Metadata={"source": "integration"}, ContentType="image/jpeg")
        assert response["ETag"] == f'"{hashlib.md5(data).hexdigest()}"'
        received, out = read(key)
        assert received == data
        assert out["Metadata"] == {"source": "integration"}
        assert s3.head_object(Bucket=bucket, Key=key)["ContentLength"] == len(data)
        if data:
            assert read(key, Range="bytes=0-1")[0] == data[:2]
            assert read(key, Range="bytes=-2")[0] == data[-2:]
            error("InvalidRange", s3.get_object, Bucket=bucket, Key=key,
                  Range=f"bytes={len(data)}-")
        error("304", s3.get_object, Bucket=bucket, Key=key, IfNoneMatch=response["ETag"])
        error("PreconditionFailed", s3.put_object, Bucket=bucket, Key=key, Body=b"replace",
              IfNoneMatch="*")
        assert read(key)[0] == data
    error("BadDigest", s3.put_object, Bucket=bucket, Key="bad-md5", Body=b"abc",
          ContentMD5=base64.b64encode(bytes(16)).decode())
    error("NoSuchKey", s3.get_object, Bucket=bucket, Key="bad-md5")
    s3.copy_object(Bucket=bucket, Key="copy", CopySource={"Bucket": bucket, "Key": "nested/random"})
    assert read("copy")[0] == read("nested/random")[0]
    pages = list(s3.get_paginator("list_objects_v2").paginate(Bucket=bucket,
                                                           PaginationConfig={"PageSize": 2}))
    keys = [item["Key"] for page in pages for item in page.get("Contents", [])]
    assert keys == sorted(set(keys)), keys
    assert "bad-md5" not in keys
    result = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "copy"}, {"Key": "missing"}]})
    assert not result.get("Errors"), result
    print("PASS ordinary, metadata, checksum, copy, condition, range, pagination, delete", flush=True)


def multipart():
    # The first part is deliberately late. Parts 2/3 must finish independently.
    data = [os.urandom(7 * 1024 * 1024 + 113), os.urandom(8 * 1024 * 1024 + 79),
            os.urandom(768 * 1024 + 21)]
    upload = s3.create_multipart_upload(Bucket=bucket, Key="multipart", ACL="public-read")["UploadId"]
    def put(n):
        out = s3.upload_part(Bucket=bucket, Key="multipart", UploadId=upload,
                             PartNumber=n, Body=data[n-1])
        return {"PartNumber": n, "ETag": out["ETag"]}
    with ThreadPoolExecutor(max_workers=2) as executor:
        parts = list(executor.map(put, [2, 3]))
    listed = s3.list_parts(Bucket=bucket, Key="multipart", UploadId=upload)
    assert [p["PartNumber"] for p in listed["Parts"]] == [2, 3]
    parts.append(put(1))
    error("BadDigest", s3.upload_part, Bucket=bucket, Key="multipart", UploadId=upload,
          PartNumber=2, Body=b"bad replacement", ContentMD5=base64.b64encode(bytes(16)).decode())
    parts.sort(key=lambda p: p["PartNumber"])
    response = s3.complete_multipart_upload(Bucket=bucket, Key="multipart", UploadId=upload,
                                            MultipartUpload={"Parts": parts})
    repeated = s3.complete_multipart_upload(Bucket=bucket, Key="multipart", UploadId=upload,
                                            MultipartUpload={"Parts": parts})
    assert response["ETag"] == repeated["ETag"]
    combined = b"".join(data)
    assert read("multipart")[0] == combined
    s3.put_object(Bucket=bucket, Key="same-put", Body=combined)
    assert read("same-put")[0] == combined
    # Complete may select a non-1 starting part; only the final part can be <5MiB.
    small = s3.create_multipart_upload(Bucket=bucket, Key="small-part")["UploadId"]
    one = s3.upload_part(Bucket=bucket, Key="small-part", UploadId=small, PartNumber=9, Body=b"a")
    two = s3.upload_part(Bucket=bucket, Key="small-part", UploadId=small, PartNumber=10, Body=b"b")
    error("EntityTooSmall", s3.complete_multipart_upload, Bucket=bucket, Key="small-part",
          UploadId=small, MultipartUpload={"Parts": [{"PartNumber": 9, "ETag": one["ETag"]},
                                                    {"PartNumber": 10, "ETag": two["ETag"]}]})
    s3.complete_multipart_upload(Bucket=bucket, Key="small-part", UploadId=small,
                                MultipartUpload={"Parts": [{"PartNumber": 10, "ETag": two["ETag"]}]})
    assert read("small-part")[0] == b"b"
    aborted = s3.create_multipart_upload(Bucket=bucket, Key="aborted")["UploadId"]
    s3.upload_part(Bucket=bucket, Key="aborted", UploadId=aborted, PartNumber=1, Body=b"pending")
    s3.abort_multipart_upload(Bucket=bucket, Key="aborted", UploadId=aborted)
    error("NoSuchUpload", s3.list_parts, Bucket=bucket, Key="aborted", UploadId=aborted)
    print("PASS multipart late first part, replacement failure, complete retry, abort, protocol limits", flush=True)


def http_acl():
    public = os.environ["MOKYU_TEST_PUBLIC"]
    key = "space +/\u4e2d\u6587%2F.jpg"
    url = f"{public}/{quote(key, safe='/')}"
    headers = {"Host": "media.test"}
    assert requests.get(url, headers=headers).status_code == 403
    s3.put_object_acl(Bucket=bucket, Key=key, ACL="public-read")
    response = requests.get(url, headers={**headers, "Origin": "https://browser.test"})
    assert response.status_code == 200 and response.content == b"abc", response.text
    assert response.headers["Access-Control-Allow-Origin"] == "https://browser.test"
    signed = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": "same-put"}, ExpiresIn=120)
    assert requests.get(signed).status_code == 200
    parsed = urlsplit(signed)
    values = dict(parse_qsl(parsed.query))
    signature = values["X-Amz-Signature"]
    values["X-Amz-Signature"] = ("0" if signature[0] != "0" else "1") + signature[1:]
    bad = urlunsplit(parsed._replace(query=urlencode(values)))
    assert requests.get(bad).status_code == 403
    web = os.environ["MOKYU_TEST_WEB"]
    assert "<title>Mokyu</title>" in requests.get(web + "/").text
    session = requests.Session()
    assert session.get(web + "/api/buckets").status_code == 403
    assert session.post(web + "/api/login", json={"username": "tester", "password": "irrelevant"}).status_code == 403
    login = session.post(web + "/api/login", headers={"Origin": web},
                         json={"username": "tester", "password": os.environ["MOKYU_TEST_PASSWORD"]})
    assert login.status_code == 200, login.text
    assert len(session.cookies["mokyu_session"]) == 64
    assert session.get(web + "/api/buckets").status_code == 200
    assert session.post(web + "/api/logout", headers={"Origin": web}).status_code == 403
    assert session.post(web + "/api/logout", headers={"Origin": web, "X-CSRF-Token": login.json()["csrf_token"]}).status_code == 204
    assert session.get(web + "/api/buckets").status_code == 403
    print("PASS ACL, anonymous path, presigned signature, CORS, Web session and CSRF", flush=True)


if __name__ == "__main__":
    ordinary()
    multipart()
    http_acl()
