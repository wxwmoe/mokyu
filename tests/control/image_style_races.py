"""Concurrent image settings use the same bucket lock order as other settings."""
from concurrent.futures import ThreadPoolExecutor
import os
from threading import Barrier
import uuid

import requests


def run(f, request, page, url, checks):
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    prefix = "race." + uuid.uuid4().hex + "."
    endpoint = url + "/api/v1/buckets/" + f.bucket_id + "/image-processing"
    style = {**page()["styles"][0], "enabled": False}
    for index in range(16):
        revision = page()["settings"]["revision"]
        barrier = Barrier(2)
        names = [prefix + str(index) + suffix for suffix in (".a", ".b")]

        def save(name):
            with requests.Session() as session:
                session.cookies.update(f.session.cookies)
                barrier.wait(timeout=10)
                response = session.put(endpoint, headers=f.headers, timeout=30,
                    json={"revision": revision, "edits": [
                        {"action": "put", "style": {**style, "name": name}}]})
                return name, response.status_code, response.text[:500]

        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(save, names))
        assert sorted(status for _, status, _ in results) == [200, 409], results
        assert page()["settings"]["revision"] == revision + 1
        stored = request("GET", "/styles?prefix=" + prefix + str(index) + ".").json()["styles"]
        assert [row["name"] for row in stored] == [name for name, status, _ in results if status == 200]
    rows = request("GET", "/styles?prefix=" + prefix).json()["styles"]
    assert len(rows) == 16
    request("PUT", "", {"revision": page()["settings"]["revision"], "edits": [
        {"action": "remove", "name": row["name"]} for row in rows]})
    checks.append("16 concurrent style saves each committed one winner and rejected the stale revision without deadlock")
