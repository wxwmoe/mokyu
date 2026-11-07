"""Exercise the three snapshot operations with real foreground writes and an independent recovery fixture."""
import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config

assert os.environ["MOKYU_TEST_ALLOW_STATE_CHANGES"] == "isolated-only"
cmd = [os.environ["MOKYU_TEST_BINARY"], "--config", os.environ["MOKYU_TEST_CONFIG"], "cli"]
storage = os.environ["MOKYU_TEST_STORAGE"]
output = Path(os.environ["MOKYU_TEST_SNAPSHOT_EXPECTED"])


def cli(*args, body=None):
    reply = subprocess.run(cmd + list(args), input=json.dumps(body) if body is not None else None,
                           text=True, capture_output=True)
    assert reply.returncode == 0, reply.stderr
    return json.loads(reply.stdout)


def wait_for(check, label, timeout=180):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        value = check()
        if value:
            return value
        time.sleep(.2)
    raise AssertionError(label + ": " + str(status()))


def status():
    return cli("storage", "journal", storage, "snapshots")


def snapshot(mode):
    previous = int(status()["revision"])
    cli("storage", "journal", storage, "snapshots", mode)
    return wait_for(lambda: revision(previous), mode)


def revision(previous):
    value = status()
    assert value["error"] is None, value
    return value if int(value["revision"]) > previous else None


def policy(trigger):
    value = cli("storage", "journal", storage, "show")
    value["policy"].update(snapshot_trigger_bytes=trigger, snapshot_page_bytes=8192,
                           put_target_bytes=32768, put_max_wait_seconds=2)
    cli("storage", "journal", storage, "set", "-", body={"revision": value["revision"], "policy": value["policy"]})


tag = "snap-" + uuid.uuid4().hex[:8]
primary, paused, empty = [tag + "-" + s for s in ("a", "b", "empty")]
view = cli("storage", "show", storage)
profile = cli("storage", "profile", "show", storage)
profile.update(unreferenced_grace="4s", min_storage_duration="0s")
cli("storage", "profile", "set", storage, "-", body=profile)
policy(512 * 1024 * 1024)
clients, expected = {}, {}


def toggle(name, enabled):
    state = cli("bucket", "journal", name)
    cli("bucket", "journal", name, "--enabled", str(enabled).lower(), "--revision", state["revision"])


for name in (primary, paused, empty):
    cli("bucket", "create", name, "--storage", view["id"])
    toggle(name, True)
    keys = cli("cred", "create", name)
    clients[name] = boto3.client("s3", endpoint_url=os.environ["MOKYU_TEST_ENDPOINT"], region_name="us-east-1",
                                 aws_access_key_id=keys["access_key"], aws_secret_access_key=keys["secret_key"],
                                 config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                               request_checksum_calculation="when_required"))
    expected[name] = {}


def put(name, key, revision=0):
    body = b"shared bytes" if revision == 0 else ("new" + str(revision)).encode()
    meta = {"sample": hashlib.shake_256((key + str(revision)).encode()).hexdigest(275)}
    etag = clients[name].put_object(Bucket=name, Key=key, Body=body, Metadata=meta)["ETag"]
    expected[name][key] = {"hex": body.hex(), "metadata": meta, "etag": etag}


def flush():
    cli("storage", "journal", storage, "flush")
    for name in clients:
        wait_for(lambda: cli("bucket", "journal", name)["metadata_journal"]["pending_records"] == "0", "flush")


for n in range(96):
    put(primary, f"row-{n:03}")
put(paused, "alone")
flush()
first = snapshot("append")
assert int(first["pages"]) > 3, first
wait_for(lambda: status()["source_entries"] == "0", "first source GC")
toggle(paused, False)
paused_view = status()
assert paused_view["current"] == first["current"] and int(paused_view["invalid_bytes"]) > 0
toggle(paused, True)
resumed_view = status()
assert resumed_view["current"] == first["current"] and resumed_view["invalid_bytes"] == first["invalid_bytes"]
for n in range(39):
    k = f"row-{n:03}"
    clients[primary].delete_object(Bucket=primary, Key=k)
    del expected[primary][k]
put(primary, "row-005", 1)
put(primary, "row-095", 1)
toggle(paused, False)
expected[paused] = None
flush()
appended = snapshot("append")
assert int(appended["delete_log_bytes"]) > 0 and int(appended["invalid_bytes"]) > 0, appended
compact = snapshot("compact")
assert int(compact["pages"]) < int(appended["pages"]), (appended, compact)
new = snapshot("new")
assert new["invalid_bytes"] == new["delete_log_bytes"] == "0", new
wait_for(lambda: status()["source_entries"] == "0", "covered entries")
before = int(new["revision"])
policy(512)
put(primary, "auto", 2)
flush()
automatic = wait_for(lambda: revision(before), "automatic append")
policy(512 * 1024 * 1024)
output.write_text(json.dumps(expected))
print("PASS snapshots", json.dumps({"pages": [first["pages"], appended["pages"], compact["pages"], new["pages"]],
      "affected_bytes": appended["invalid_bytes"], "tombstone_bytes": appended["delete_log_bytes"],
      "auto_revision": automatic["revision"]}), flush=True)
