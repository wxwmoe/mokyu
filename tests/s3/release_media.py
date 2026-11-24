"""First-run release check. Run in a container with a fresh database and data directory."""
import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError


def run(after_restart=False):
    assert os.environ["MOKYU_TEST_ALLOW_STATE_CHANGES"] == "isolated-only"
    started = time.monotonic()
    config = os.environ["MOKYU_TEST_CONFIG"]
    binary = os.environ.get("MOKYU_BINARY", "mokyu")
    admin = os.environ["MOKYU_TEST_PASSWORD"]
    web = os.environ["MOKYU_TEST_WEB"]
    public = os.environ["MOKYU_TEST_PUBLIC"]
    endpoint = os.environ["MOKYU_TEST_ENDPOINT"]
    directory = Path(os.environ["MOKYU_TEST_MEDIA_STORAGE"])
    credentials_file = Path(os.environ["MOKYU_TEST_CREDENTIALS"])

    def cli(*args, data=None):
        result = subprocess.run([binary, "-c", config, "cli", *map(str, args)],
                                input=data, text=True, capture_output=True, check=True)
        return json.loads(result.stdout)

    if after_restart:
        credential = json.loads(credentials_file.read_text())
        bucket = next(item for item in cli("bucket", "list") if item["name"] == "media")
    else:
        directory.mkdir(exist_ok=True)
        assert not any(directory.iterdir()) and not credentials_file.exists(), "use an empty fixture"
        assert cli("user", "list") == []
        cli("init", "tester", "--stdin", data=admin + "\n")
        storage = cli("storage", "create", "media", "local", "--path", directory, "--init")
        bucket = cli("bucket", "create", "media", "--storage", storage["id"])
        cli("domain", "set", "media.test", "media")
        credential = cli("cred", "create", "media")
        with os.fdopen(os.open(credentials_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
            json.dump(credential, stream)
    client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                          aws_access_key_id=credential["access_key"], aws_secret_access_key=credential["secret_key"],
                          config=Config(s3={"addressing_style": "path"}, request_checksum_calculation="when_required"))
    session = requests.Session()
    login = session.post(web + "/api/v1/auth/login", headers={"Origin": web},
                         json={"username": "tester", "password": admin}, timeout=10)
    login.raise_for_status()
    listed = session.get(web + "/api/v1/buckets", timeout=10)
    listed.raise_for_status()
    assert any(b["id"] == bucket["id"] for b in listed.json())
    data = b"first release media" * 1024
    for key, acl in [("public/first.bin", "public-read"), ("private/first.bin", "private")]:
        if not after_restart:
            client.put_object(Bucket="media", Key=key, Body=data, ACL=acl)
        assert client.get_object(Bucket="media", Key=key)["Body"].read() == data
        response = requests.get(public + "/" + key, headers={"Host": "media.test"}, timeout=10)
        if acl == "public-read":
            response.raise_for_status()
            assert hashlib.sha256(response.content).digest() == hashlib.sha256(data).digest()
        else:
            assert response.status_code == 403, response.status_code
        if after_restart:
            client.delete_object(Bucket="media", Key=key)
            try:
                client.head_object(Bucket="media", Key=key)
            except ClientError as error:
                assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
            else:
                raise AssertionError("deleted object is still visible")
    if after_restart:
        credentials_file.unlink()
    phase = "post-restart authenticated read, anonymous access and deletion" if after_restart else "first-run CLI, Web and S3"
    print(f"{phase} passed in {time.monotonic() - started:.2f}s", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--after-restart", action="store_true")
    run(parser.parse_args().after_restart)
