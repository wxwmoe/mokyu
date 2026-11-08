"""Crash a local raw testing service at its RAM, file and catalog boundaries."""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from botocore.exceptions import ConnectionClosedError
from testing_install import check_error, client


def wait_for(check):
    end = time.monotonic() + 40
    while time.monotonic() < end:
        if check():
            return
        time.sleep(.05)
    raise AssertionError("fixture did not reach its boundary")


def run(root):
    binary = os.environ["MOKYU_TEST_BINARY"]
    folder = root / "data"
    faults = root / "faults"
    faults.mkdir()
    config = root / "testing.toml"
    config.write_text(f'[instance]\ndata_path={json.dumps(str(folder))}\nlisten_s3="127.0.0.1:19000"\nwrite_memory_size="1MiB"\n')
    args = [binary, "--config", str(config)]
    env = dict(os.environ, MOKYU_TEST_FAULT_DIR=str(faults))
    api = client("http://127.0.0.1:19000")
    process = None
    log = (root / "service.log").open("ab")

    def cli(*command):
        return json.loads(subprocess.check_output([*args, "cli", *command], env=env, stderr=subprocess.PIPE))
    def start():
        nonlocal process
        process = subprocess.Popen([*args, "testing"], env=env, stdout=log, stderr=log)
        def ready():
            assert process.poll() is None, (root/"service.log").read_text()[-2000:]
            try:
                return cli("status", "--ready")["installation"] == "testing"
            except subprocess.CalledProcessError:
                return False
        wait_for(ready)
    def stop(sig=signal.SIGTERM, failure=False):
        process.send_signal(sig)
        rc = process.wait(75)
        assert (rc != 0) == (failure or sig == signal.SIGKILL), rc
    def put(key, data, **kw):
        return api.put_object(Bucket="test", Key=key, Body=data, **kw)
    def get(key, **kw):
        return api.get_object(Bucket="test", Key=key, **kw)["Body"].read()
    def row(key):
        with sqlite3.connect(f"file:{folder}/mokyu.db?mode=ro", uri=True) as db:
            return db.execute("SELECT hex(r.location_id),r.durability,r.quarantine_code FROM objects o JOIN raw_ranges e ON o.stream_id=e.stream_id JOIN raw_sources r ON e.source_id=r.id WHERE o.object_key=?", (key.encode(),)).fetchone()
    def arm(point):
        (faults/point).touch()
    def hit(point):
        wait_for((faults/(point+".hit")).exists)
    def disarm(point):
        (faults/point).unlink()
        (faults/(point+".hit")).unlink(missing_ok=True)

    body = os.urandom(1024 * 700)
    try:
        start()
        with socket.socket() as sock:
            assert sock.connect_ex(("127.0.0.1", 9001)) != 0
        for path in ("/", "/api/v1/session", "/mokyu.node.v1.NodeControl/NodeSession"):
            try:
                urllib.request.urlopen("http://127.0.0.1:9002"+path)
            except urllib.error.HTTPError as error:
                assert error.code == 404, error.code
            else:
                raise AssertionError(path)
        put("old", b"old")
        cli("cache", "flush", "--wait")
        for boundary in ("source-before-publish", "object-published"):
            put("interrupted", b"previous")
            cli("cache", "flush", "--wait")
            arm(boundary)
            arm("local-memory-before-file")
            with ThreadPoolExecutor(max_workers=1) as pool:
                sending = pool.submit(put, "interrupted", body)
                hit(boundary)
                stop(signal.SIGKILL)
                try:
                    sending.result(timeout=15)
                except ConnectionClosedError:
                    pass
                else:
                    raise AssertionError("the request was acknowledged before the fault")
            disarm(boundary)
            disarm("local-memory-before-file")
            start()
            if boundary == "source-before-publish":
                assert get("interrupted") == b"previous"
            else:
                assert row("interrupted")[2] is not None
                check_error(lambda: get("interrupted"), {500, 503})
        arm("local-memory-before-file")
        put("old", body)
        hit("local-memory-before-file")
        assert get("old") == body and get("old", Range="bytes=7-90") == body[7:91]
        assert row("old")[1:] == ("volatile", None)
        assert cli("cache","status")["write_memory"]["used"] >= len(body)
        # Holds the remaining memory below the size of the second PUT.
        put("overflow", body)
        assert row("overflow")[1] in ("buffered","durable")
        assert get("overflow") == body
        check_error(lambda: put("old", b"invalid", ContentMD5=base64.b64encode(hashlib.md5(b"bad").digest()).decode()), {400})
        assert get("old") == body
        stop(signal.SIGKILL)
        disarm("local-memory-before-file")
        start()
        assert row("old")[2] is not None
        check_error(lambda: get("old"), {500,503})
        assert get("overflow") == body
        arm("local-memory-persisted")
        put("linked", body)
        hit("local-memory-persisted")
        assert row("linked")[1] == "volatile"
        stop(signal.SIGKILL)
        disarm("local-memory-persisted")
        start()
        assert row("linked")[1:] == ("durable", None)
        assert get("linked") == body
        arm("local-memory-before-file")
        put("copy-source",body)
        hit("local-memory-before-file")
        with ThreadPoolExecutor(max_workers=1) as pool:
            copied = pool.submit(api.copy_object, Bucket="test", Key="copied",
                                 CopySource={"Bucket":"test","Key":"copy-source"})
            try:
                time.sleep(.15)
                if copied.done():
                    copied.result()
                    name, durability, failure = row("copied")
                    assert (durability, failure) == ("durable", None)
                    target = folder/"storage"/"objects"/name[-3:][::-1].lower()/name.lower()
                    assert target.read_bytes() == body
            finally:
                disarm("local-memory-before-file")
            copied.result(timeout=15)
        assert get("copied") == body and row("copied")[1] == "durable"
        put("clean",body)
        put("large",body*13)
        api.delete_object(Bucket="test",Key="linked")
        stop()
        start()
        assert get("clean") == body and get("large") == body*13
        check_error(lambda: get("linked"),{404})
        assert cli("cache","status")["write_memory"]["used"] == 0
        # A failed filesystem flush must leave the live source readable and be retryable.
        def obstruct(key):
            arm("local-memory-before-file")
            put(key, body)
            hit("local-memory-before-file")
            name = row(key)[0].lower()
            partial = folder/"storage"/"objects"/name[-3:][::-1]/(name+".part")
            partial.mkdir()
            disarm("local-memory-before-file")
            wait_for(lambda: cli("cache","status")["write_backlog"]["last_sync_error"])
            assert get(key) == body
            return partial
        partial = obstruct("retry")
        partial.rmdir()
        cli("cache","flush","--wait")
        assert row("retry")[1] == "durable"
        partial = obstruct("failed-stop")
        stop(failure=True)
        partial.rmdir()
        start()
        assert row("failed-stop")[2] is not None and get("retry") == body
        stop()
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        log.close()


if __name__ == "__main__":
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    with tempfile.TemporaryDirectory(dir=os.environ["MOKYU_TEST_WORKDIR"]) as tmp:
        try:
            run(Path(tmp))
        except Exception:
            path = Path(tmp)/"service.log"
            if path.exists():
                print(path.read_text()[-3200:])
            raise
