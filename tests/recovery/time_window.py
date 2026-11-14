"""Exercise a disposable bucket through S3 and the local Mokyu administrator CLI."""
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config


def check(s3, bucket, cli):
    status = cli("bucket", "recovery-window", bucket)
    assert not status["seconds"], "use a fresh bucket"
    key = "window/" + uuid.uuid4().hex
    obj = {"Bucket": bucket, "Key": key}
    s3.put_object(**obj, Body=b"original")
    cut = cli("bucket", "recovery-window", bucket, "--seconds", "3600")["latest_us"]
    when = datetime.fromtimestamp(cut / 1_000_000, timezone.utc).isoformat()
    s3.put_object(**obj, Body=b"overwrite")
    plan = cli("bucket", "time-preview", bucket, when, "object", "--key", key,
               "--target-prefix", "recovered/")
    assert plan["counts"] == {"add": 1}
    cli("bucket", "time-restore", plan["id"], "--digest", plan["digest"])
    until = time.monotonic() + 60
    while cli("bucket", "time-restore", plan["id"])["phase"] != "done":
        assert time.monotonic() < until, "restore did not complete"
        time.sleep(0.25)
    saved = s3.get_object(Bucket=bucket, Key="recovered/" + key)
    with saved["Body"] as body:
        assert body.read() == b"original"
    with s3.get_object(**obj)["Body"] as body:
        assert body.read() == b"overwrite"
    cli("bucket", "recovery-window", bucket, "--seconds", "0")


if __name__ == "__main__":
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    credential = json.loads(Path(os.environ["MOKYU_TEST_CREDENTIALS"]).read_text())
    s3 = boto3.client("s3", endpoint_url=os.environ["MOKYU_TEST_ENDPOINT"],
                      region_name="us-east-1", aws_access_key_id=credential["access_key"],
                      aws_secret_access_key=credential["secret_key"],
                      config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    def cli(*args):
        return json.loads(subprocess.check_output(
            ["mokyu", "-c", os.environ["MOKYU_TEST_CONFIG"], "cli", *args]))
    check(s3, credential["bucket"], cli)
