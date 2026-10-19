"""Verify creation permissions with restrictive/permissive inherited umasks.

Requires root in an isolated Linux test container, a stopped initialized SQLite
service and the source_quarantine fixture environment. Reads as UID 65534.
"""
import os
import stat
import subprocess
import sys
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.storage.source_quarantine import Fixture, wait


def mode(path, expected):
    actual = stat.S_IMODE(path.stat().st_mode)
    assert actual == expected, (str(path), oct(actual), oct(expected))


def other_user(path, readable):
    result = subprocess.run(
        [sys.executable, "-c", "import sys; open(sys.argv[1], 'rb').read(1)", str(path)],
        user=65534, group=65534, extra_groups=[], capture_output=True,
    )
    assert (result.returncode == 0) == readable, (str(path), result.stderr)
    if not readable:
        assert b"PermissionError" in result.stderr, result.stderr


def main():
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    assert os.geteuid() == 0, "requires isolated root to test a different UID"
    assert not os.environ.get("MOKYU_TEST_BACKEND"), "local storage required"
    config_path = Path(os.environ["MOKYU_TEST_CONFIG"])
    config = tomllib.loads(config_path.read_text())
    assert config["database"]["type"] == "sqlite"
    data = Path(config["instance"]["data_path"])
    database = Path(config["database"].get("path", data / "mokyu.db"))
    command = [os.environ["MOKYU_TEST_BINARY"], "--config", str(config_path)]
    log = (data.parent / "file-permissions.log").open("a")
    process = None
    retained = []

    def stop():
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=40)

    try:
        for mask in (0o077, 0o000):
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, umask=mask)

            def ready():
                assert process.poll() is None, "service exited; inspect file-permissions.log"
                return subprocess.run(command + ["cli", "status", "--ready"],
                                      capture_output=True, timeout=5).returncode == 0

            wait(ready, "service did not become ready", seconds=60)
            fixture = Fixture()
            fixture.put("chunk", b"chunk permissions" * 1024)
            assert fixture.keys()
            for name in fixture.keys():
                mode(Path(name), 0o644)
                other_user(Path(name), True)

            profile = fixture.cli("storage", "profile", "show", fixture.name)
            profile["cdc_enabled"] = False
            fixture.cli("storage", "profile", "set", fixture.name, "-", value=profile)
            marker = fixture.arm("raw-file-durable")
            body = b"raw permissions" * 1024
            with ThreadPoolExecutor(max_workers=1) as executor:
                uploading = executor.submit(fixture.put, "raw", body)
                try:
                    wait(marker.with_suffix(".hit").exists, "raw creation boundary not reached")
                    temporary, = fixture.root.rglob("*.partial")
                    mode(temporary, 0o644)
                    other_user(temporary, True)
                    inode = temporary.stat().st_ino
                    published = temporary.with_suffix("")
                finally:
                    marker.unlink(missing_ok=True)
                uploading.result(timeout=60)
            assert published.stat().st_ino == inode
            mode(published, 0o644)
            fixture.read("raw", body)
            retained.append((fixture, published, body))

            upload = fixture.client.create_multipart_upload(Bucket=fixture.name, Key="multipart")["UploadId"]
            before = set((data / "multipart").rglob("*"))
            part = fixture.client.upload_part(Bucket=fixture.name, Key="multipart",
                                             UploadId=upload, PartNumber=1, Body=body)
            candidates = [path for path in set((data / "multipart").rglob("*")) - before if path.is_file()]
            assert candidates, "Multipart candidate was not created"
            for candidate in candidates:
                mode(candidate, 0o644)
                other_user(candidate, True)
            fixture.client.complete_multipart_upload(
                Bucket=fixture.name, Key="multipart", UploadId=upload,
                MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
            )
            fixture.read("multipart", body)
            for path in [fixture.root, *fixture.root.rglob("*")]:
                mode(path, 0o755 if path.is_dir() else 0o644)
            mode(data, 0o755)
            for path in data.rglob("*"):
                if path.is_dir():
                    mode(path, 0o755)
            for suffix in ("", "-wal", "-shm", ".lock"):
                sensitive = Path(str(database) + suffix)
                assert sensitive.exists(), str(sensitive)
                mode(sensitive, 0o600)
                other_user(sensitive, False)
            other_user(config_path, False)
            for previous, path, expected in retained:
                mode(path, 0o644)
                previous.read("raw", expected)
            stop()
            print(f"PASS umask {mask:04o}, private SQLite, public payload/staging and restart", flush=True)
    finally:
        stop()
        log.close()


if __name__ == "__main__":
    main()
