"""Bucket journal capture through revisioned settings and normal S3 publications."""
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import requests
from botocore.config import Config

assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
web = os.environ["MOKYU_TEST_WEB"]
command = [os.environ["MOKYU_TEST_BINARY"], "--config", os.environ["MOKYU_TEST_CONFIG"], "cli"]


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))


session = requests.Session()
response = session.post(web + "/api/v1/auth/login", headers={"Origin": web},
                        json={"username": "tester", "password": os.environ["MOKYU_TEST_PASSWORD"]})
assert response.status_code == 200, response.text
session.headers.update({"Origin": web, "X-CSRF-Token": response.json()["csrf_token"]})
assert session.post(web + "/api/v1/account/reauth",
                    json={"password": os.environ["MOKYU_TEST_PASSWORD"]}).status_code == 204
fixture = json.loads(Path(os.environ["MOKYU_TEST_CREDENTIALS"]).read_text())
storage = next(b["storage_id"] for b in session.get(web + "/api/v1/buckets").json()
               if b["name"] == fixture["bucket"])
prefix = "journal-" + uuid.uuid4().hex[:10]


def create(suffix):
    response = session.post(web + "/api/v1/buckets", json={"name": prefix + suffix, "storage_id": storage})
    assert response.status_code == 201, response.text
    bucket = response.json()
    key = cli("cred", "create", bucket["name"])
    s3 = boto3.client("s3", endpoint_url=os.environ["MOKYU_TEST_ENDPOINT"], region_name="us-east-1",
                      aws_access_key_id=key["access_key"], aws_secret_access_key=key["secret_key"],
                      config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                    retries={"max_attempts": 0}, request_checksum_calculation="when_required"))
    return bucket, s3


def state(bucket):
    response = session.get(web + "/api/v1/buckets/" + bucket["id"] + "/settings")
    assert response.status_code == 200, response.text
    return response.json()


def toggle(bucket, enabled, revision=None, expected=200):
    value = state(bucket)
    body = {k: value["bucket"][k] for k in ["revision", "cors", "website_enabled", "index_document",
                                          "error_document", "public_base_url", "uploads_paused"]}
    body["metadata_journal_enabled"] = enabled
    if revision is not None:
        body["revision"] = revision
    response = session.put(web + "/api/v1/buckets/" + bucket["id"] + "/settings", json=body)
    assert response.status_code == expected, response.text
    return response.json()


def wait_scan(bucket):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        journal = state(bucket)["metadata_journal"]
        if journal and not journal["scanning"]:
            return journal
        time.sleep(0.1)
    raise AssertionError("initial journal capture did not complete")


a, s3a = create("-a")
b, s3b = create("-b")
c, _ = create("-empty")
assert state(a)["metadata_journal_enabled"] is False
assert state(a)["metadata_journal"] is None
for i in range(257):
    s3a.put_object(Bucket=a["name"], Key=f"key-{i:04}", Body=b"baseline")
s3b.put_object(Bucket=b["name"], Key="key-0000", Body=b"private-disabled")
old_revision = state(a)["bucket"]["revision"]
toggle(a, True)
# Race the scanner on both sides of its first page boundary.
s3a.delete_object(Bucket=a["name"], Key="key-0008")
s3a.put_object(Bucket=a["name"], Key="key-0256", Body=b"updated-during-scan")
toggle(a, False, old_revision, 412)
toggle(c, True)
empty = wait_scan(c)
assert empty["captured_sequence"] == "2", empty
before = int(wait_scan(a)["captured_sequence"])


def write(i):
    s3a.put_object(Bucket=a["name"], Key=f"key-{i:04}", Body=str(i).encode(), Metadata={"test": "journal"})


with ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(write, range(64)))
s3a.delete_object(Bucket=a["name"], Key="key-0000")
s3a.copy_object(Bucket=a["name"], Key="copied", CopySource={"Bucket": a["name"], "Key": "key-0001"})
assert int(state(a)["metadata_journal"]["captured_sequence"]) >= before + 66
assert state(b)["metadata_journal"] is None
multipart = s3a.create_multipart_upload(Bucket=a["name"], Key="multipart")
baseline = state(a)["metadata_journal"]["captured_sequence"]
part = s3a.upload_part(Bucket=a["name"], Key="multipart", UploadId=multipart["UploadId"], PartNumber=1, Body=b"completed")
assert state(a)["metadata_journal"]["captured_sequence"] == baseline
s3a.complete_multipart_upload(Bucket=a["name"], Key="multipart", UploadId=multipart["UploadId"],
                              MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]})
assert int(state(a)["metadata_journal"]["captured_sequence"]) > int(baseline)
generation = state(a)["metadata_journal"]["generation"]
toggle(a, False)
toggle(a, True)
assert wait_scan(a)["generation"] == generation
toggle(a, False)
s3a.delete_object(Bucket=a["name"], Key="key-0001")
assert state(a)["metadata_journal"]["dirty"]
toggle(a, True)
assert int(wait_scan(a)["generation"]) == int(generation) + 1
toggle(b, True)
assert int(wait_scan(b)["captured_sequence"]) >= 3
retired, _ = create("-retired")
toggle(retired, True)
wait_scan(retired)
toggle(retired, False)
cli("bucket", "delete", retired["name"])
replacement = cli("bucket", "create", retired["name"], "--storage", storage)
assert replacement["id"] != retired["id"]
assert state(replacement)["metadata_journal_enabled"] is False
print(json.dumps({"dropped_bucket": retired["id"], "buckets": [a["id"], b["id"], c["id"]], "capture_contract": "passed"}))
