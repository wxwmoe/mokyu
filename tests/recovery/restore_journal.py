"""Restore selected buckets through the normal CLI into a separately initialized deployment."""
import json
import os
import subprocess
import time
from pathlib import Path

import boto3
from botocore.config import Config

assert os.environ["MOKYU_TEST_ALLOW_STATE_CHANGES"] == "isolated-only"
command = [os.environ["MOKYU_TEST_BINARY"], "--config", os.environ["MOKYU_TEST_CONFIG"], "cli"]
source = json.loads(Path(os.environ["MOKYU_TEST_RECOVERY_SOURCE"]).read_text())
expected = json.loads(Path(os.environ["MOKYU_TEST_RECOVERY_EXPECTED"]).read_text())
source_key = os.environ.get("MOKYU_TEST_SOURCE_KEY")
mode = os.environ.get("MOKYU_TEST_RECOVERY_MODE", "copy")


def cli(*args, body=None, check=True):
    result = subprocess.run(command + list(args), input=body, capture_output=True, text=True)
    if not check:
        return result.returncode
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def probe(secret=None):
    if secret is None:
        return cli("storage", "recovery", "probe", "-", body=json.dumps(source))
    connection = Path(os.environ["MOKYU_TEST_RECOVERY_SOURCE"])
    return cli("storage", "recovery", "probe", str(connection), "--key-stdin", body=secret + "\n")


first = probe()
if source_key:
    assert first["state"] == "locked", first["state"]
    assert cli("storage", "recovery", "probe", os.environ["MOKYU_TEST_RECOVERY_SOURCE"],
               "--key-stdin", body="invalid\n", check=False)
    first = probe(source_key)
assert first["state"] == "preview", first
preview = first["recovery"]
buckets = {b["name"]: b for b in preview["progress"]["buckets"]}
assert all((name in buckets) == (rows is not None) for name, rows in expected.items())
selected = {buckets[name]["id"]: name for name, rows in expected.items() if rows is not None}
target = None if mode == "takeover" else cli("storage", "show", os.environ["MOKYU_TEST_STORAGE"])["id"]
selection = {"project": "00000000-0000-0000-0000-000000000001", "buckets": selected,
             "destination": target, "exclusive_takeover": mode == "takeover",
             "complete_journal_upload": os.environ.get("MOKYU_TEST_COMPLETE_JOURNAL") == "1"}
job = cli("storage", "recovery", "start", preview["id"], "-",
          body=json.dumps({"revision": preview["revision"], "selection": selection}))
deadline = time.monotonic() + 240
while time.monotonic() < deadline:
    job = cli("storage", "recovery", "show", job["id"])
    if job["state"] in ("failed", "completed", "cancelled"):
        break
    time.sleep(.25)
assert job["state"] == "completed", job
for name, rows in expected.items():
    if rows is None:
        continue
    credential = cli("cred", "create", name)
    s3 = boto3.client("s3", endpoint_url=os.environ["MOKYU_TEST_ENDPOINT"], region_name="us-east-1",
                      aws_access_key_id=credential["access_key"], aws_secret_access_key=credential["secret_key"],
                      config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    assert sorted(r["Key"] for r in s3.list_objects_v2(Bucket=name).get("Contents", [])) == sorted(rows)
    for key, row in rows.items():
        data = bytes.fromhex(row["hex"])
        got = s3.get_object(Bucket=name, Key=key)
        assert got["Body"].read() == data and got["Metadata"] == row["metadata"]
        assert got["ETag"] == row["etag"]
        if len(data) >= 3:
            assert s3.get_object(Bucket=name, Key=key, Range="bytes=1-2")["Body"].read() == data[1:3]
    s3.put_object(Bucket=name, Key="after-recovery", Body=b"fresh")
    assert s3.get_object(Bucket=name, Key="after-recovery")["Body"].read() == b"fresh"
print("PASS", mode, len(selected), "buckets", sum(len(rows) for rows in expected.values() if rows is not None), "objects")
