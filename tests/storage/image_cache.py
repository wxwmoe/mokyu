"""Shared image artifacts, admission, integrity, restart and source authority.

The isolated runner supplies the authenticated fixture and process restart hook.
"""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import random
import re
import struct
import time
import uuid
import zlib

import requests
from tests.storage.source_quarantine import wait


def run(f, request, page, case, checks, restart):
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    root = case / "data" / "cache" / "images"
    config = case / "config" / "config.toml"
    original = config.read_text()
    permissions = root.stat().st_mode
    prefix = "image-cache-" + uuid.uuid4().hex[:12]
    rule = "mokyu/v1/frame,index=0/orient/resize,mode=contain,width=384,height=384,upscale=false/encode,format=png"
    policy = page()["settings"]["policy"]
    policy.update(enabled=True, protect_original=False, mapping=None)
    request("PUT", "", dict(revision=page()["settings"]["revision"], policy=policy, edits=[]))
    imported = request("POST", "/styles/import", dict(revision=page()["settings"]["revision"],
                       text="styleName: Cache.Shared, styleBody: " + rule, apply=True)).json()
    assert imported["applied"], imported

    def quota(value):
        assert re.search(r"(?m)^image_cache_size=", original)
        config.write_text(re.sub(r'(?m)^image_cache_size=.*$', 'image_cache_size="' + value + '"', original))
        restart()

    def files():
        return {p.name: p for p in root.iterdir() if p.is_dir() and re.fullmatch("[a-f0-9]{64}", p.name)}

    def accounting():
        used = f.cli("status")["local_bytes"][2]
        actual = sum(p.stat().st_size for entry in files().values() for p in entry.iterdir() if p.is_file())
        assert used == actual and used <= 98304, (used, actual)

    def upload(index, seed=None):
        def part(kind, data):
            return struct.pack("!I", len(data)) + kind + data + struct.pack("!I", zlib.crc32(kind + data))
        rng = random.Random(index if seed is None else seed)
        raw = b"".join(b"\0" + rng.randbytes(72 * 3) for _ in range(64))
        png = b"\x89PNG\r\n\x1a\n" + part(b"IHDR", struct.pack("!IIBBBBB", 72, 64, 8, 2, 0, 0, 0))
        png += part(b"IDAT", zlib.compress(raw)) + part(b"IEND", b"")
        key = prefix + "-" + str(index) + ".png"
        f.client.put_object(Bucket=f.name, Key=key, Body=png, ContentType="image/png", ACL="public-read")
        return key

    def get(key, status=200):
        response = requests.get("http://127.0.0.1:39100/" + f.name + "/" + key + "!Cache.Shared", timeout=65)
        assert response.status_code == status, (response.status_code, status, response.text[:200] if response.status_code != 200 else "")
        return response

    def idle():
        until = time.monotonic() + 8
        while list(root.glob(".work-*")):
            assert time.monotonic() < until, "abandoned image workspace"
            time.sleep(.05)

    try:
        quota("96KiB")
        keys = [upload(i) for i in range(8)]
        first = get(keys[0])
        before = files()
        before_stats = {key: (path / "output").stat() for key, path in before.items()}
        version = f.session.get(f.web + "/api/v1/buckets/" + f.bucket_id + "/objects",
                                params={"q": keys[0], "mode": "exact"}, timeout=20).json()["items"][0]["id"]
        preview = f.session.get(f.web + "/api/v1/buckets/" + f.bucket_id + "/object/thumbnail",
                                params={"key": keys[0], "version": version}, timeout=65)
        assert preview.status_code == 200, preview.text[:200]
        assert preview.content == first.content and preview.headers["Cache-Control"] == "private, no-store"
        assert files().keys() == before.keys()
        assert all((files()[key] / "output").stat().st_ino == stat.st_ino for key, stat in before_stats.items())
        checks.append("management thumbnail and equivalent public rule share the same immutable output inode")
        for key in keys[1:7]:
            get(key)
            accounting()
        assert len(files()) < 7
        with ThreadPoolExecutor(4) as pool:
            replies = list(pool.map(get, [keys[7]] * 4))
        assert all(r.content == replies[0].content for r in replies)
        accounting()
        target = next(path for path in files().values() if (path / "output").read_bytes() == replies[0].content)
        with (target / "output").open("r+b") as output:
            output.write(b"damaged!")
        assert get(keys[7]).content == replies[0].content
        with (target / "output").open("r+b") as output:
            output.write(b"damaged!")
        root.chmod(0o500)
        get(keys[7], 503)
        root.chmod(permissions)
        assert get(keys[7]).content == replies[0].content
        accounting()
        with (target / "output").open("r+b") as output:
            output.write(b"damaged!")
        root.chmod(0o500)
        barrier = f.arm("image-before-capture")
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(get, keys[7])
            try:
                wait(lambda: barrier.with_suffix(".hit").exists(), "cleanup before replacement", 10)
                root.chmod(permissions)
            finally:
                barrier.unlink(missing_ok=True)
            assert pending.result(timeout=30).content == replies[0].content
        assert not (target / "output").exists(), "replacement published into a pending deletion"
        assert get(keys[7]).content == replies[0].content
        accounting()
        checks.append("quota eviction, exact disk accounting, single-flight concurrency and same-size corruption recovery")
        stale = root / (".work-" + str(uuid.uuid4()))
        stale.mkdir()
        (stale / "input").write_bytes(b"interrupted input")
        os.chown(stale, 10001, 10001)
        sentinel = root / "keep-unrecognized"
        sentinel.write_bytes(b"not an image cache entry")
        retained = {key: (path / "output").stat().st_ino for key, path in files().items()}
        restart()
        assert not stale.exists() and sentinel.exists()
        assert all((files()[key] / "output").stat().st_ino == inode for key, inode in retained.items())
        assert get(keys[7]).content == replies[0].content
        accounting()
        header = target / "metadata"
        with header.open("r+b") as metadata:
            metadata.write(b"bad record")
        restart()
        assert not target.exists()
        assert get(keys[7]).content == replies[0].content
        assert get(upload(7, seed=99)).headers["ETag"] != replies[0].headers["ETag"]
        f.client.put_object_acl(Bucket=f.name, Key=keys[7], ACL="private")
        get(keys[7], 403)
        f.client.delete_object(Bucket=f.name, Key=keys[7])
        get(keys[7], 404)
        checks.append("restart reuses verified artifacts, removes only recognized partials, rejects corrupt headers and honors overwrite, ACL and deletion")
        get(keys[6])
        uncached = upload(99)
        root.chmod(0o500)
        restart()
        assert get(keys[6]).status_code == 200
        get(uncached, 503)
        root.chmod(permissions)
        assert get(uncached).status_code == 200
        quota("0B")
        with ThreadPoolExecutor(4) as pool:
            replies = list(pool.map(get, [keys[0]] * 4))
        assert all(r.content == replies[0].content for r in replies)
        idle()
        assert not files()
        f.client.put_object(Bucket=f.name, Key=prefix + "-broken", Body=b"not an image", ACL="public-read")
        get(prefix + "-broken", 415)
        assert get(keys[0]).status_code == 200
        idle()
        assert not files()
        checks.append("read-only retained hits survive; new work fails boundedly and recovers; zero quota and failed native work leave no retained artifacts")

        profile = f.cli("storage", "profile", "show", f.name)
        f.cli("storage", "profile", "set", f.name, "-", value={**profile,
              "cdc_enabled": False, "deduplication_enabled": False, "pack_enabled": False,
              "pack_range_enabled": False, "cache_enabled": False, "read_integrity": "off"})
        try:
            before = set(f.keys())
            damaged_key = upload(123)
            source, = set(f.keys()) - before
            good = f.bytes(source)
            # A valid replacement PNG must still fail the physical source digest.
            other_key = upload(124)
            replacement_source, = set(f.keys()) - before - {source}
            replacement = f.bytes(replacement_source)
            assert good != replacement and len(good) == len(replacement)
            f.bytes(source, replacement)
            try:
                f.read(damaged_key, replacement)
                rejected = requests.get("http://127.0.0.1:39100/" + f.name + "/" + damaged_key + "!Cache.Shared", timeout=65)
                assert rejected.status_code in (400, 415, 500, 503), rejected.status_code
                idle()
                assert not files(), "unchecked source produced a retained artifact"
            finally:
                f.bytes(source, good)
            assert get(other_key).status_code == 200
            checks.append("derived images verify the original physical source even when ordinary reads disable integrity checks")
        finally:
            current = f.cli("storage", "profile", "show", f.name)
            f.cli("storage", "profile", "set", f.name, "-", value={**profile, "revision": current["revision"]})
    finally:
        root.chmod(permissions)
        config.write_text(original)
        restart()
