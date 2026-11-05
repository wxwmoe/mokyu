"""Verify source admission before cached bytes, redirects and conditional responses."""
from io import BytesIO
import os
import uuid

import requests
from PIL import Image


def run(f, request, page, case, checks):
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    initial = page()["settings"]["policy"]
    assert initial["max_pixels"] == 150_000_000
    prefix = "pixel-limit-" + uuid.uuid4().hex[:12]

    def upload(label, width, height, animated=False):
        with BytesIO() as body:
            first = Image.new("RGB", (width, height), "red")
            if animated:
                first.save(body, "GIF", save_all=True, append_images=[Image.new("RGB", first.size, "blue")], duration=40)
            else:
                first.save(body, "PNG")
            key = prefix + "-" + label
            f.client.put_object(Bucket=f.name, Key=key, Body=body.getvalue(), ACL="public-read",
                                ContentType="image/gif" if animated else "image/png")
            return key

    def settings(policy, status=200):
        revision = page()["settings"]["revision"]
        return request("PUT", "", dict(revision=revision, policy=policy, edits=[]), status)

    def limit(n, status=200):
        return settings(dict(initial, enabled=True, protect_original=False, mapping=None, max_pixels=n), status)

    def url(key, fmt="png", method="GET", status=200, headers=None, frame=False, size=8):
        query = "" if not fmt else "?mokyu/v1/" + ("frame,index=0/" if frame else "") + (
            f"resize,mode=contain,width={size},height={size},upscale=false/encode,format={fmt}")
        r = requests.request(method, f"{os.environ['MOKYU_TEST_ENDPOINT']}/{f.name}/{key}{query}",
                             headers=headers or {}, allow_redirects=False, timeout=30)
        assert r.status_code == status, (method, key, fmt, r.status_code, status)
        return r

    land = upload("land", 64, 32)
    port = upload("port", 32, 64)
    over = upload("over", 64, 33)
    gif = upload("two-frames", 64, 32, True)
    try:
        limit(2048)
        saved = page()["settings"]
        for invalid in (0, 150_000_001):
            limit(invalid, 400)
            assert page()["settings"] == saved
        warm = url(land)
        assert Image.open(BytesIO(warm.content)).size == (8, 4)
        assert Image.open(BytesIO(url(port).content)).size == (4, 8)
        url(over, status=415)
        url(gif, status=415, frame=True)
        assert url(land, "auto", status=302).headers["Cache-Control"] == "no-store"
        limit(2047)
        for method, headers in [("GET", {}), ("HEAD", {}), ("GET", {"Range": "bytes=0-1"}),
                                ("GET", {"If-None-Match": warm.headers["ETag"]})]:
            url(land, method=method, headers=headers, status=415)
        url(land, "auto", status=415)
        url(land, fmt="")
        rule = {"stages": [{"operations": [], "format": "png", "quality_mode": "exact", "progressive": False}]}
        request("POST", "/preview", dict(key=land, revision=page()["settings"]["revision"], rule=rule), 415)
        limit(2048)
        assert url(land).headers["ETag"] == warm.headers["ETag"]
        limit(4096)
        url(gif, frame=True)
        limit(4095)
        url(gif, frame=True, status=415)
        settings(dict(initial, enabled=True, protect_original=True))
        opaque = prefix + "-opaque"
        content = b"GIF89a" + bytes(64)
        f.client.put_object(Bucket=f.name, Key=opaque, Body=content, ACL="public-read",
                            ContentType="application/octet-stream")
        endpoint = f.client.meta.endpoint_url.rstrip("/") + "/" + f.name + "/" + opaque
        for query in ("", "?x-amz-foo=1", "?signature=0", "?expires=1"):
            assert requests.get(endpoint + query, timeout=10).status_code == 403
            assert requests.head(endpoint + query, timeout=10).status_code == 403
        assert f.client.get_object(Bucket=f.name, Key=opaque)["Body"].read() == content
    finally:
        settings(initial)
    checks.append("Source pixels across orientations, frames, cached and conditional reads, redirects and previews")
