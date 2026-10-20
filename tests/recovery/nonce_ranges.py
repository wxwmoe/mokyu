"""Nonce reservation, crash boundaries and encrypted source publication.

Requires a stopped, initialized disposable service, administrator password and S3 backend.
MOKYU_TEST_OLD_BINARY optionally seeds encrypted sources before upgrading the database.
Owns that service process; fault-injection boundaries never run in production.
"""
import os
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    command = [os.environ["MOKYU_TEST_BINARY"], "--config", os.environ["MOKYU_TEST_CONFIG"]]
    faults = Path(os.environ["MOKYU_TEST_FAULT_DIR"])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / "nonce-ranges.log").open("a")
    process = None
    database = Database()

    def stop(crash=False):
        if process and process.poll() is None:
            process.kill() if crash else process.terminate()
            process.wait(timeout=40)

    def start(binary=None):
        nonlocal process
        process = subprocess.Popen([binary or command[0], *command[1:]], stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, "service exited; inspect nonce-ranges.log"
            return subprocess.run(command + ["cli", "status", "--ready"], capture_output=True, timeout=5).returncode == 0
        wait(ready, "service did not become ready", seconds=60)

    try:
        old_binary = os.environ.get("MOKYU_TEST_OLD_BINARY")
        start(old_binary)
        if old_binary:
            legacy = Fixture()
            old_key = legacy.cli("key", "create", legacy.name + "-key", "block")
            legacy.cli("storage", "key", "set", legacy.name, old_key["id"])
            legacy_body = os.urandom(12 * 1024 * 1024)
            legacy.put("legacy", legacy_body)
            legacy.read("legacy", legacy_body)
            profile = legacy.cli("storage", "profile", "show", legacy.name)
            profile.update(pack_enabled=True, pack_max_size="16MiB")
            legacy.cli("storage", "profile", "set", legacy.name, "-", value=profile)
            legacy.finished(legacy.cli("pack", "run"))
            profile = legacy.cli("storage", "profile", "show", legacy.name)
            profile["pack_enabled"] = False
            legacy.cli("storage", "profile", "set", legacy.name, "-", value=profile)
            old_sources = database.query("SELECT nonce FROM chunk_encodings WHERE block_key_id=%s AND nonce IS NOT NULL", uuid.UUID(old_key["id"]))
            old_packs = database.query("SELECT nonce FROM packs WHERE block_key_id=%s AND nonce IS NOT NULL", uuid.UUID(old_key["id"]))
            assert old_sources and old_packs
            stop()
            start()
            legacy.read("legacy", legacy_body)
            for table, previous in [("chunk_encodings", old_sources), ("packs", old_packs)]:
                current = database.query("SELECT nonce,nonce_id FROM " + table + " WHERE block_key_id=%s AND nonce IS NOT NULL", uuid.UUID(old_key["id"]))
                assert {bytes(row["nonce"]) for row in current} == {bytes(row["nonce"]) for row in previous}
                assert all(row["nonce_id"] == int.from_bytes(row["nonce"][4:], "big") for row in current)
            print("legacy encrypted encodings and packs preserved across migration", flush=True)
        fixture = Fixture()
        assert fixture.backend, "select an isolated S3 backend to exercise encrypted packs"
        storage = uuid.UUID(fixture.store["id"])

        def configure(**values):
            profile = fixture.cli("storage", "profile", "show", fixture.name)
            profile.update(values)
            fixture.cli("storage", "profile", "set", fixture.name, "-", value=profile)

        def new_key(label, algorithm="aes-256-gcm"):
            name = fixture.name + "-" + label
            row = fixture.cli("key", "create", name, "block", "-a", algorithm)
            fixture.cli("storage", "key", "set", fixture.name, name)
            return uuid.UUID(row["id"])

        def high(key, tag=0):
            rows = database.query("SELECT reserved_id FROM nonce_counters WHERE block_key_id=%s AND type_tag=%s", key, tag)
            return rows[0]["reserved_id"] if rows else None

        def sources(key, tag=0):
            table = "packs" if tag else "chunk_encodings"
            return database.query("SELECT nonce_id,nonce FROM " + table + " WHERE storage_id=%s AND block_key_id=%s AND nonce IS NOT NULL", storage, key)

        def rejected(name):
            try:
                fixture.put(name, os.urandom(8192))
            except ClientError as error:
                assert error.response["ResponseMetadata"]["HTTPStatusCode"] >= 500
                return
            raise AssertionError("invalid nonce state accepted an encrypted upload")

        key = new_key("first")
        assert high(key) == high(key, 1) == 0, "key creation must not reserve a range"
        small = os.urandom(8192)
        fixture.put("encrypted", small)
        fixture.read("encrypted", small)
        assert high(key) == 64 and high(key, 1) == 0
        assert sources(key)[0]["nonce_id"] == 1
        fixture.put("duplicate", small)
        assert high(key) == 64 and len(sources(key)) == 1, "deduplication allocated a new nonce"

        bodies = [os.urandom(16384) for _ in range(16)]
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda item: fixture.put("parallel-" + str(item[0]), item[1]), enumerate(bodies)))
        rows = sources(key)
        assert len(rows) == 17 and len({row["nonce_id"] for row in rows}) == 17
        assert high(key) == 64, "one reservation should cover the first 64 encryptions"
        for row in rows:
            nonce = bytes(row["nonce"])
            assert len(nonce) == 12 and nonce[0] >> 3 == 0
            assert int.from_bytes(nonce[4:], "big") == row["nonce_id"]
        stop(crash=True)
        start()
        assert high(key) == 64, "startup reserved an unused range"
        fixture.put("restart", os.urandom(8192))
        assert max(row["nonce_id"] for row in sources(key)) == 65
        fixture.read("encrypted", small)

        packed = os.urandom(12 * 1024 * 1024)
        fixture.put("packed", packed)
        configure(pack_enabled=True, pack_max_size="16MiB")
        fixture.finished(fixture.cli("pack", "run"))
        assert sources(key, 1), "encrypted pack was not created"
        for row in sources(key, 1):
            assert bytes(row["nonce"])[0] >> 3 == 1
            assert int.from_bytes(row["nonce"][4:], "big") == row["nonce_id"]
        fixture.read("packed", packed)
        configure(pack_enabled=False)

        for boundary, expected in [("nonce-before-commit", 0), ("nonce-after-commit", 64)]:
            current = new_key(boundary)
            body = os.urandom(8192)
            marker = fixture.arm(boundary)
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(fixture.put, boundary, body)
                wait(lambda: marker.with_suffix(".hit").exists() or pending.done(), "reservation did not reach " + boundary)
                assert marker.with_suffix(".hit").exists(), pending.exception()
                assert high(current) == expected
                stop(crash=True)
                marker.unlink(missing_ok=True)
                try:
                    pending.result(timeout=30)
                    raise AssertionError("crashed upload unexpectedly succeeded")
                except AssertionError:
                    raise
                except Exception:
                    pass
            start()
            assert high(current) == expected
            fixture.put(boundary + "-retry", body)
            fixture.read(boundary + "-retry", body)
            assert min(row["nonce_id"] for row in sources(current)) == expected + 1

        missing = new_key("missing")
        database.query("DELETE FROM nonce_counters WHERE block_key_id=%s AND type_tag=0", missing)
        rejected("missing-counter")
        assert high(missing) is None, "missing counters must never restart at zero"
        database.query("INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) VALUES(%s,0,4096)", missing)
        fixture.put("restored-counter", os.urandom(8192))
        assert sources(missing)[0]["nonce_id"] == 4097

        exhausted = new_key("exhausted", "chacha20-poly1305")
        database.query("UPDATE nonce_counters SET reserved_id=%s WHERE block_key_id=%s AND type_tag=0", (1 << 63) - 64, exhausted)
        rejected("exhausted")
        assert high(exhausted) == (1 << 63) - 64
        rotated = new_key("rotated", "chacha20-poly1305")
        fixture.put("rotated", os.urandom(8192))
        assert sources(rotated)[0]["nonce_id"] == 1
        fixture.read("encrypted", small)
        fixture.read("packed", packed)

        before = database.query("SELECT block_key_id,type_tag,reserved_id FROM nonce_counters ORDER BY block_key_id,type_tag")
        fixture.cli("storage", "key", "clear", fixture.name)
        fixture.put("unencrypted", os.urandom(8192))
        configure(cdc_enabled=False, pack_range_enabled=False)
        fixture.put("raw", os.urandom(8192))
        assert database.query("SELECT block_key_id,type_tag,reserved_id FROM nonce_counters ORDER BY block_key_id,type_tag") == before
        for row in database.query("SELECT nonce,nonce_id FROM chunk_encodings WHERE storage_id=%s AND encryption_algorithm='none'", storage):
            assert row["nonce"] is None and row["nonce_id"] is None
        print("nonce ranges: concurrent writes, dedup, pack, restart, commit faults, missing/exhausted counters and plaintext passed", flush=True)
    finally:
        stop()
        log.close()


if __name__ == "__main__":
    main()
