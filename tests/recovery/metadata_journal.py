"""Export a shared managed-S3 journal for the Rust recovery-reader contract."""
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config

assert os.environ["MOKYU_TEST_ALLOW_STATE_CHANGES"] == "isolated-only"
command = [os.environ["MOKYU_TEST_BINARY"], "--config", os.environ["MOKYU_TEST_CONFIG"], "cli"]
storage = os.environ["MOKYU_TEST_STORAGE"]
upstream = json.loads(Path(os.environ["MOKYU_TEST_JOURNAL_BACKEND"]).read_text())
directory = Path(os.environ["MOKYU_TEST_JOURNAL_DIR"])
directory.mkdir(parents=True, exist_ok=True)
assert not any(directory.iterdir()), "journal export requires an empty directory"


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))


def client(endpoint, region, access, secret):
    return boto3.client("s3", endpoint_url=endpoint, region_name=region, aws_access_key_id=access,
                        aws_secret_access_key=secret, config=Config(signature_version="s3v4",
                        s3={"addressing_style": "path"}, retries={"total_max_attempts": 1},
                        request_checksum_calculation="when_required"))


remote = client(upstream["endpoint"], upstream["region"], upstream["access_key"], upstream["secret_key"])
view = cli("storage", "show", storage)
base = (view["prefix"].strip("/") + "/") if view["prefix"] else ""
bucket_name = view["bucket"]
tag = "recovery-" + uuid.uuid4().hex[:10]
enabled, disabled, empty = [tag + "-" + suffix for suffix in ("a", "b", "c")]
for name in (enabled, disabled, empty):
    cli("bucket", "create", name, "--storage", view["id"])
    if name != disabled:
        revision = cli("bucket", "journal", name)["revision"]
        cli("bucket", "journal", name, "--enabled", "true", "--revision", revision)
    if name != empty:
        key = cli("cred", "create", name)
        s3 = client(os.environ["MOKYU_TEST_ENDPOINT"], "us-east-1", key["access_key"], key["secret_key"])
        s3.put_object(Bucket=name, Key="same-name", Body=name.encode(), Metadata={"sample": name})
        s3.put_object(Bucket=name, Key="empty", Body=b"")
        if name == enabled:
            s3.put_object(Bucket=name, Key="deleted", Body=b"old")
            s3.delete_object(Bucket=name, Key="deleted")

cli("storage", "journal", storage, "flush")
deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    states = [cli("bucket", "journal", name)["metadata_journal"] for name in (enabled, empty)]
    if all(s["baseline_ready"] and s["pending_records"] == "0" for s in states):
        break
    time.sleep(.2)
else:
    raise AssertionError(cli("storage", "journal", storage, "show"))

marker = remote.get_object(Bucket=bucket_name, Key=base + "mokyu.json")["Body"].read()
(directory / "mokyu.json").write_bytes(marker)
epoch = uuid.UUID(json.loads(marker)["metadata_journal"]["writer_epoch"]).hex
prefix = "metadata/changes/" + epoch + "/"
target = directory / prefix
target.mkdir(parents=True)
for page in remote.get_paginator("list_objects_v2").paginate(Bucket=bucket_name, Prefix=base + prefix):
    for entry in page.get("Contents", []):
        leaf = entry["Key"].removeprefix(base + prefix)
        assert re.fullmatch(r"[0-9]{20}", leaf), leaf
        (target / leaf).write_bytes(remote.get_object(Bucket=bucket_name, Key=entry["Key"])["Body"].read())
assert any(target.iterdir())
(directory / "expected.json").write_text(json.dumps({enabled: ["empty", "same-name"], disabled: None, empty: []}))
