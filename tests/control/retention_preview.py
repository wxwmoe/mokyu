"""Read-only retention estimates on an empty, disposable version-enabled bucket."""
import json
import os
from pathlib import Path

import boto3
import requests
from botocore.config import Config


def run():
    assert os.environ["MOKYU_TEST_ALLOW_STATE_CHANGES"] == "isolated-only"
    cred = json.loads(Path(os.environ["MOKYU_TEST_CREDENTIALS"]).read_text())
    bucket = cred["bucket"]
    s3 = boto3.client("s3", endpoint_url=os.environ["MOKYU_TEST_ENDPOINT"], region_name="us-east-1",
                      aws_access_key_id=cred["access_key"], aws_secret_access_key=cred["secret_key"],
                      config=Config(s3={"addressing_style": "path"}, request_checksum_calculation="when_required"))
    listing = s3.list_object_versions(Bucket=bucket)
    assert not listing.get("Versions") and not listing.get("DeleteMarkers"), "use an empty bucket"
    origin = os.environ["MOKYU_TEST_WEB"]
    session = requests.Session()
    login = session.post(origin + "/api/v1/auth/login", headers={"Origin": origin},
                         json={"username": "tester", "password": os.environ["MOKYU_TEST_PASSWORD"]}, timeout=10)
    login.raise_for_status()
    session.headers.update({"Origin": origin, "X-CSRF-Token": login.json()["csrf_token"]})
    buckets = session.get(origin + "/api/v1/buckets", timeout=10)
    buckets.raise_for_status()
    base = origin + "/api/v1/buckets/" + next(b["id"] for b in buckets.json() if b["name"] == bucket)
    path = base + "/versions/retention-preview"
    assert requests.get(path, timeout=10).status_code == 403

    def preview(days, keep):
        response = session.get(path, params={"days": days, "keep": keep}, timeout=15)
        response.raise_for_status()
        return response.json()

    session.put(base + "/versioning", json="enabled", timeout=10).raise_for_status()
    target = dict(Bucket=bucket, Key="history")
    body = bytes(range(251)) * 1024
    oldest = s3.put_object(**target, Body=body)["VersionId"]
    s3.copy_object(Bucket=bucket, Key="retained", CopySource=target)
    for value in [b"second", b"third"]:
        s3.put_object(**target, Body=value)
    s3.delete_object(**target)
    expected = dict(versions=1, logical_bytes=len(body), time_protected_bytes=0, shared_bytes=len(body))
    estimate = preview(0, 2)
    assert estimate == expected, estimate
    assert preview(30, 0)["versions"] == 0
    assert preview(0, 3)["versions"] == 0
    session.put(base + "/versioning", json="suspended", timeout=10).raise_for_status()
    assert preview(0, 2) == expected
    assert s3.get_object(**target, VersionId=oldest)["Body"].read() == body
    print("retention preview kept the current object, markers, shared content and historical bytes", flush=True)


if __name__ == "__main__":
    run()
