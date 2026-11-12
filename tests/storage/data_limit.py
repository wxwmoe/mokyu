"""Exercise directory admission through an isolated local S3 installation."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import secrets
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

from botocore.exceptions import ClientError
import boto3
from botocore.config import Config


def wait(check, seconds=45):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if check():
            return
        time.sleep(.1)
    raise AssertionError("fixture did not reach the expected state")


def run(root):
    binary = os.environ["MOKYU_TEST_BINARY"]
    config = root / "config.toml"
    data = root / "data"
    faults = root / "faults"
    faults.mkdir()
    env = dict(os.environ, MOKYU_TEST_FAULT_DIR=str(faults))
    args = [binary, "--config", str(config)]
    deployment_key = subprocess.check_output([binary, "tool", "keygen"], text=True).strip()
    process = None
    log = (root / "service.log").open("ab")

    def configure(limit, floor="1MiB"):
        config.write_text(
            '[database]\ntype="sqlite"\n'
            f'[instance]\ndata_path={json.dumps(str(data))}\n'
            f'deployment_key={json.dumps(deployment_key)}\n'
            + (f'data_limit="{limit}"\n' if limit else "")
            + f'min_free_space="{floor}"\n'
            'listen_s3="127.0.0.1:19000"\nwrite_memory_size="0B"\n'
            'listen_web="127.0.0.1:19001"\nlisten_control="127.0.0.1:19002"\n'
            'cache_size="4MiB"\nimage_cache_size="4MiB"\n'
        )
        config.chmod(0o600)

    def cli(*command, value=None):
        result = subprocess.run([*args, "cli", *command], input=value, text=True,
                                capture_output=True, env=env, timeout=45)
        assert result.returncode == 0, (command, result.stderr)
        return json.loads(result.stdout)

    def start():
        nonlocal process
        process = subprocess.Popen(args, env=env, stdout=log, stderr=log)

        def ready():
            assert process.poll() is None, (root / "service.log").read_text()[-3000:]
            result = subprocess.run([*args, "cli", "status"],
                                    capture_output=True, env=env, timeout=5)
            return result.returncode == 0

        wait(ready)

    def stop(crash=False):
        if process and process.poll() is None:
            process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
            process.wait(timeout=60)

    def profile(name="local", **changes):
        value = cli("storage", "profile", "show", name)
        value.update(changes)
        return cli("storage", "profile", "set", name, "-", value=json.dumps(value))

    def put(key, body):
        return api.put_object(Bucket="test", Key=key, Body=body)

    def get(key):
        return api.get_object(Bucket="test", Key=key)["Body"].read()

    def rejected(operation):
        try:
            operation()
        except ClientError as error:
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 503, error.response
        else:
            raise AssertionError("write exceeded the directory limit")

    try:
        configure("48MiB")
        start()
        cli("init", "tester", "--stdin", value=secrets.token_urlsafe(24) + "\n")
        store = cli("storage", "create", "local", "local", "--init", "--path", str(data / "storage"))
        cli("bucket", "create", "test", "--storage", store["id"])
        credential = cli("cred", "create", "test")
        api = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", region_name="us-east-1",
                           aws_access_key_id=credential["access_key"],
                           aws_secret_access_key=credential["secret_key"],
                           config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                         retries={"total_max_attempts": 1}))
        profile(write_durability="durable", cache_enabled=False, upload_cache=False,
                cdc_enabled=False, deduplication_enabled=False, pack_enabled=False, compression_enabled=False,
                deleted_chunk_retention="1s", unreferenced_grace="1s", min_storage_duration="0s")
        for name, path in [("inside", data / "second"), ("outside", root / "outside")]:
            store = cli("storage", "create", name, "local", "--init", "--path", str(path))
            cli("bucket", "create", name, "--storage", store["id"])
            cli("cred", "grant", credential["access_key"], name)
            profile(name, write_durability="durable", cdc_enabled=False, pack_enabled=False,
                    compression_enabled=False, deduplication_enabled=False,
                    cache_enabled=False, upload_cache=False)
        old = os.urandom(32768)
        put("preserved", old)
        payload = os.urandom(4 * 1024 * 1024)
        accepted = []
        for n in range(24):
            try:
                put(f"fill-{n}", payload)
                accepted.append(f"fill-{n}")
            except ClientError as error:
                assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 503, error.response
                break
        else:
            raise AssertionError("directory admission did not stop writes")
        assert accepted and get("preserved") == old
        rejected(lambda: put("preserved", os.urandom(4 * 1024 * 1024)))
        assert get("preserved") == old
        full = cli("status")["data_directory"]
        assert full["limit"] == 48 * 1024 * 1024
        rejected(lambda: api.put_object(Bucket="inside", Key="shared-limit", Body=payload))
        api.put_object(Bucket="outside", Key="independent", Body=payload)
        assert api.get_object(Bucket="outside", Key="independent")["Body"].read() == payload
        rejected(lambda: api.copy_object(Bucket="inside", Key="cross-copy",
                                        CopySource={"Bucket": "outside", "Key": "independent"}))

        stop(crash=True)
        configure("2MiB")
        start()
        assert get("preserved") == old
        assert get(accepted[0]) == payload
        rejected(lambda: put("over-lowered-limit", b"x" * 65536))
        stop()

        configure("128MiB")
        start()
        for key in accepted:
            api.delete_object(Bucket="test", Key=key)
        cli("maintenance", "enable")
        cli("gc", "mark-unreferenced")
        cli("maintenance", "disable")
        before_gc = cli("status")["data_directory"]["used_bytes"]

        def reclaimed():
            cli("gc", "run")
            return cli("status")["data_directory"]["used_bytes"] < before_gc - len(payload)

        wait(reclaimed)
        put("after-reclaim", payload)
        assert get("after-reclaim") == payload
        with ThreadPoolExecutor(max_workers=3) as pool:
            writes = [pool.submit(put, "parallel", payload),
                      pool.submit(api.copy_object, Bucket="inside", Key="cross-copy",
                                  CopySource={"Bucket": "outside", "Key": "independent"}),
                      pool.submit(api.copy_object, Bucket="test", Key="local-copy",
                                  CopySource={"Bucket": "test", "Key": "preserved"})]
            for writing in writes:
                writing.result(timeout=30)
        assert get("parallel") == payload and get("local-copy") == old
        assert api.get_object(Bucket="inside", Key="cross-copy")["Body"].read() == payload

        upload = api.create_multipart_upload(Bucket="test", Key="multipart")["UploadId"]
        parts = [os.urandom(5 * 1024 * 1024), os.urandom(5 * 1024 * 1024), os.urandom(12345)]
        entries = []
        for n, body in enumerate(parts, 1):
            part = api.upload_part(Bucket="test", Key="multipart", UploadId=upload, PartNumber=n, Body=body)
            entries.append({"PartNumber": n, "ETag": part["ETag"]})
        api.complete_multipart_upload(Bucket="test", Key="multipart", UploadId=upload,
                                      MultipartUpload={"Parts": entries})
        assert get("multipart") == b"".join(parts)

        marker = faults / "data-directory-write"
        marker.touch()
        url = urlsplit(api.generate_presigned_url("put_object", Params={"Bucket": "test", "Key": "disconnected"}))
        connection = http.client.HTTPConnection(url.hostname, url.port, timeout=15)
        connection.request("PUT", url.path + "?" + url.query, body=old)
        wait(marker.with_suffix(".hit").exists)
        connection.close()
        assert cli("status")["data_directory"]["reserved_bytes"] > 0
        marker.unlink()
        wait(lambda: cli("status")["data_directory"]["reserved_bytes"] == 0)
        marker.with_suffix(".hit").unlink()
        assert get("preserved") == old

        marker.touch()
        with ThreadPoolExecutor(max_workers=1) as pool:
            sending = pool.submit(put, "interrupted", payload)
            wait(marker.with_suffix(".hit").exists)
            during = cli("status")["data_directory"]
            assert during["reserved_bytes"] > 0
            stop(crash=True)
            try:
                sending.result(timeout=15)
            except Exception:
                pass
            else:
                raise AssertionError("interrupted write was acknowledged")
        marker.unlink()
        marker.with_suffix(".hit").unlink(missing_ok=True)
        start()
        assert get("multipart") == b"".join(parts)
        assert cli("status")["data_directory"]["reserved_bytes"] == 0
        put("after-crash", payload)
        assert get("after-crash") == payload

        profile(cdc_enabled=True, deduplication_enabled=False, compression_enabled=False,
                pack_enabled=False)
        chunked = os.urandom(9 * 1024 * 1024)
        put("chunked", chunked)
        assert get("chunked") == chunked
        profile(pack_enabled=True, pack_max_size="32MiB")
        packing = cli("pack", "run")["task_id"]
        wait(lambda: cli("task", "show", packing)["state"] in ("completed", "failed", "paused"))
        assert cli("task", "show", packing)["state"] == "completed"
        packs = [path for path in (data / "storage/packs").rglob("*") if path.is_file()]
        assert packs and all(path.read_bytes()[:8] == b"MOKYU\x00\x00\x05" for path in packs)
        assert get("chunked") == chunked
        bucket = cli("bucket", "journal", "test")
        cli("bucket", "journal", "test", "--enabled", "true", "--revision", str(bucket["revision"]))
        put("journaled", b"journaled-directory-budget")
        cli("storage", "journal", "local", "flush")
        wait(lambda: any((data / "storage/metadata").rglob("*.part")), seconds=60)
        previous_snapshot = cli("storage", "journal", "local", "snapshots")["current"]
        cli("storage", "journal", "local", "snapshots", "new")

        def snapshot_published():
            status = cli("storage", "journal", "local", "snapshots")
            assert not status["error"], status
            return status["current"] and status["current"] != previous_snapshot and status["task"] is None

        wait(snapshot_published)
        snapshot = cli("storage", "journal", "local", "snapshots")
        assert int(snapshot["pages"]) > 0, snapshot
        before_restart = cli("status")["data_directory"]
        stop()
        start()
        assert get("chunked") == chunked
        assert get("journaled") == b"journaled-directory-budget"
        after_restart = cli("status")["data_directory"]
        assert after_restart["used_bytes"] > 0 and after_restart["reserved_bytes"] == 0
        stop()
        configure(None, floor="64TiB")
        start()
        assert get("preserved") == old
        profile(min_free_space="64TiB")
        # Formal local storage keeps its own floor; instance floors protect cache/staging.
        try:
            put("floor-only", payload)
        except ClientError:
            pass
        else:
            raise AssertionError("filesystem floor admitted a payload")
        assert cli("status")["data_directory"]["limit"] is None
        profile(min_free_space="1MiB")
        stop()
        configure("128MiB", floor="1B")
        start()
        put("directory-only", old)
        assert get("directory-only") == old
        stop()
        configure(None)
        start()
        put("original-config", old)
        assert get("original-config") == old
        if space_fault := os.environ.get("MOKYU_TEST_SPACE_FAULT"):
            stop()
            configure("128MiB")
            start()
            fault = Path(space_fault)
            fault.touch()
            try:
                try:
                    put("preserved", payload)
                except ClientError as error:
                    assert error.response["ResponseMetadata"]["HTTPStatusCode"] >= 500
                else:
                    raise AssertionError("filesystem exhaustion admitted a payload")
                assert get("preserved") == old
                assert cli("status")["data_directory"]["reserved_bytes"] == 0
            finally:
                fault.unlink()
            put("space-restored", old)
            assert get("space-restored") == old
            print("PASS zero filesystem availability and recovery", flush=True)
        print(json.dumps({"accepted_raw_files": len(accepted), "full": full,
                          "before_restart": before_restart, "after_restart": after_restart,
                          "chunked_sha256": hashlib.sha256(chunked).hexdigest()}), flush=True)
        print("PASS raw, chunks, packs, Multipart, concurrent PUT/Copy, directory boundaries, independent floors, "
              "overwrite rejection, lower limit, GC, disconnect, crash, local journal/snapshot and original configuration", flush=True)
    finally:
        stop()
        log.close()


if __name__ == "__main__":
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    root = Path(os.environ["MOKYU_TEST_ROOT"]).resolve()
    assert root.is_dir() and not any(root.iterdir()), "fixture must start empty"
    run(root)
