"""Persistent file and cache identities across pack, unpack and process restart.

Requires a stopped disposable service and S3 backend. Owns the service process.
"""
import os
import subprocess
import tomllib
import uuid
from pathlib import Path
from urllib.parse import urlencode

from tests.recovery.source_repair import Database, location
from tests.storage.source_quarantine import Fixture, wait


def identity(value):
    return uuid.UUID(bytes=bytes(value)) if isinstance(value, (bytes, bytearray)) else uuid.UUID(str(value))


def main():
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    command = [os.environ["MOKYU_TEST_BINARY"], "--config", os.environ["MOKYU_TEST_CONFIG"]]
    config = tomllib.loads(Path(os.environ["MOKYU_TEST_CONFIG"]).read_text())
    data = Path(config["instance"]["data_path"])
    database = Database()
    process = None
    log = (data.parent / "file-identities.log").open("a")

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, "service exited; inspect file-identities.log"
            return subprocess.run(command + ["cli", "status", "--ready"], capture_output=True, timeout=5).returncode == 0
        wait(ready, "service did not become ready", seconds=60)

    def stop():
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=40)

    try:
        start()
        fixture = Fixture()
        assert fixture.backend, "requires an isolated S3 backend"
        storage = uuid.UUID(fixture.store["id"])
        raw = os.urandom(6 * 1024 * 1024 + 17)
        fixture.put("original", raw)
        logical = "id" if database.type == "postgres" else "uuid"

        def chunks():
            return database.query("SELECT id," + logical + " AS uuid FROM chunks WHERE storage_id=%s ORDER BY id", storage)

        original = chunks()
        assert len(original) >= 2
        assert all(identity(row["uuid"]).version == 7 for row in original)
        encodings = database.query("SELECT encoding_id,location_id FROM chunk_encodings WHERE storage_id=%s AND state='ready'", storage)
        assert len(encodings) == len(original)
        for row in encodings:
            assert identity(row["location_id"]).version == 7
            assert location(fixture, row["location_id"]) in fixture.keys()
            if database.type == "postgres":
                assert row["encoding_id"] == row["location_id"]
        fixture.read("original", raw)

        profile = fixture.cli("storage", "profile", "show", fixture.name)
        profile.update(pack_enabled=True, pack_range_enabled=False, pack_max_size="16MiB",
                       cache_enabled=True, min_storage_duration="0s")
        fixture.cli("storage", "profile", "set", fixture.name, "-", value=profile)
        fixture.finished(fixture.cli("pack", "run"))
        packs = database.query("SELECT id,location_id FROM packs WHERE storage_id=%s AND state='ready' ORDER BY id", storage)
        assert packs
        for row in packs:
            assert identity(row["location_id"]).version == 7
            assert location(fixture, row["location_id"], packed=True) in fixture.keys()
            if database.type == "postgres":
                assert row["id"] == row["location_id"]
        fixture.read("original", raw)
        wait(lambda: fixture.cli("status")["cache_jobs_active"] == 0, "pack cache fill did not finish")
        assert chunks() == original, "packing changed logical chunk identity"

        # Every cache name must resolve to a durable logical or physical identity.
        known = {identity(row["uuid"]) for row in original}
        known.update(identity(row["location_id"]) for row in encodings)
        cached = {identity(path.stem) for path in (data / "cache" / "chunks").glob("*/*")
                  if path.suffix in (".raw", ".zst")}
        assert known & cached, "pack read produced no persistent cache entry"
        all_known = {identity(row["uuid"]) for row in database.query("SELECT " + logical + " AS uuid FROM chunks")}
        all_known.update(identity(row["location_id"]) for row in database.query("SELECT location_id FROM chunk_encodings"))
        assert cached <= all_known, "cache name has no durable database identity"
        for path in (data / "cache" / "chunks").glob("*/*"):
            if path.suffix in (".raw", ".zst"):
                assert path.parent.name == identity(path.stem).hex[-2:]

        wrong = "42" if database.type == "postgres" else str(uuid.uuid4())
        objects = "/api/v1/buckets/" + fixture.bucket_id
        item, = fixture.api(objects + "/objects")["items"]
        version = item["id"]
        assert isinstance(version, str)
        if database.type == "postgres":
            assert uuid.UUID(version).version == 7
        else:
            assert int(version) > 0
        parameters = urlencode(dict(key="original", version=version))
        fixture.api(objects + "/object/chunks?" + parameters)
        fixture.api(objects + "/object/chunks?" + urlencode(dict(key="original", version=wrong)), expected=400)
        fixture.put("versioned", raw)
        old = next(item["id"] for item in fixture.api(objects + "/objects")["items"] if item["object_key"] == "versioned")
        fixture.put("versioned", raw)
        fixture.api(objects + "/object/chunks?" + urlencode(dict(key="versioned", version=old)), expected=412)
        fixture.api("/api/v1/packs/" + wrong, expected=400)
        fixture.api("/api/v1/storage/packs?after=" + wrong, expected=400)
        fixture.api("/api/v1/audit?after=" + wrong, expected=400)
        for row in packs:
            detail = fixture.api("/api/v1/storage/packs/" + str(row["id"]))
            assert detail["pack"]["id"] == str(row["id"])
            assert all(isinstance(member["chunk_id"], str) for member in detail["items"])

        stop()
        start()
        fixture.read("original", raw)
        assert chunks() == original
        profile = fixture.cli("storage", "profile", "show", fixture.name)
        profile["pack_enabled"] = False
        fixture.cli("storage", "profile", "set", fixture.name, "-", value=profile)
        for row in packs:
            fixture.finished(fixture.cli("pack", "unpack", str(row["id"]), "--execute"))
        fixture.read("original", raw)
        assert chunks() == original, "unpacking changed logical chunk identity"
        print("PASS persistent UUIDs, physical paths, pack cache, API IDs, restart and unpack", flush=True)
    finally:
        stop()
        log.close()


if __name__ == "__main__":
    main()
