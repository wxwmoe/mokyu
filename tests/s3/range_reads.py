"""Cold member ranges and manual pack maintenance on an isolated S3 backend."""
import json
import os
import subprocess
import time
from pathlib import Path

import psycopg
from tests.integration import bucket, s3

assert os.environ["MOKYU_TEST_ALLOW_STATE_CHANGES"] == "isolated-only"
db = psycopg.connect(Path(os.environ["MOKYU_TEST_DATABASE_FILE"]).read_text().strip(), autocommit=True)
command = [os.environ["MOKYU_TEST_BINARY"], "--config", os.environ["MOKYU_TEST_CONFIG"], "cli"]


def cli(*args):
    return json.loads(subprocess.check_output([*command, *args], text=True, timeout=30))


def pack():
    job = cli("pack", "run")["task_id"]
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        state = cli("task", "show", job)["state"]
        assert state not in ("failed", "paused"), state
        if state == "completed":
            return
        time.sleep(.1)
    raise AssertionError(job)


body = b"".join(bytes([i]) * (4 * 1024 * 1024) for i in range(24, 30))
s3.put_object(Bucket=bucket, Key="ranges", Body=body)
pack()
query = ("SELECT DISTINCT c.pack_id FROM objects o JOIN extents e ON o.stream_id=e.stream_id "
         "JOIN chunks c ON c.id=e.chunk_id WHERE o.object_key='ranges'")
packs = db.execute(query).fetchall()
assert len(packs) == 1 and packs[0][0], packs
for start in [len(body) - 4096, 41, len(body) // 2]:
    before = cli("status")["backend_gets"]
    result = s3.get_object(Bucket=bucket, Key="ranges", Range=f"bytes={start}-{start + 63}")
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert result["Body"].read() == body[start:start + 64]
    assert cli("status")["backend_gets"] - before <= 1
for retired in ["reuse", "range"]:
    result = subprocess.run([*command, "pack", "run", "--mode", retired], capture_output=True, timeout=30)
    assert result.returncode != 0
assert db.execute(query).fetchall() == packs
print("PASS cold member ranges preserve the existing pack")
