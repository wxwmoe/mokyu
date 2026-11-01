"""Combine cached public images and synchronous replicas across control failures.

The isolated harness supplies independent source/target fixtures, a warm image,
control-network fault hooks, a real database pause hook and a recovery check.
Network hooks must affect only the disposable control RPC path and roll back
automatically if the runner dies. Database pause must have a bounded watchdog.
"""
import os
import socket
import statistics
import time

import requests

from tests.storage.source_quarantine import wait


def run(source, target, public_url, expected, control_address, network, network_state,
        database, writer_ready, checks):
    assert os.environ.get("MOKYU_TEST_ALLOW_STATE_CHANGES") == "isolated-only"
    evidence = {}

    def cached():
        response = requests.get(public_url, timeout=30)
        assert response.status_code == 200 and response.content == expected

    def write(key, payload):
        source.client.put_object(Bucket=source.name, Key=key, Body=payload, ACL="public-read")
        with target.client.get_object(Bucket=target.name, Key=key)["Body"] as body:
            assert body.read() == payload

    def readable():
        try:
            return requests.get(public_url, timeout=15).status_code == 200
        except requests.RequestException:
            return False

    def fenced():
        try:
            return requests.get(public_url, timeout=8).status_code >= 400
        except requests.RequestException:
            return False

    try:
        network("delay", 300)
        timings = []
        for _ in range(3):
            started = time.monotonic()
            with socket.create_connection(control_address, timeout=5):
                pass
            timings.append(time.monotonic() - started)
        assert .20 <= statistics.median(timings) <= 1.5, timings
        for index in range(12):
            write("combined-" + str(index), os.urandom(4096 + index))
            cached()
        for _ in range(41):
            state = network_state()
            assert state["mode"] == "delay", "network fault expired before verification"
            queue = next(row for row in state["qdiscs"] if row["kind"] == "netem")
            if queue["drops"] > 0:
                break
            source.client.head_bucket(Bucket=source.name)
        assert queue["packets"] > 100 and queue["drops"] > 0, queue
        evidence["latency"] = dict(connect_seconds=timings, queue=queue)
        checks.append("cached images and required replica writes survive measured control latency and packet loss")
    finally:
        network("off", 1)
    for mode in ("oneway", "blackhole"):
        try:
            network(mode, 150)
            started = time.monotonic()
            wait(fenced, "cached image outlived disconnected run authority", 110)
            evidence[mode] = time.monotonic() - started
        finally:
            network("off", 1)
        wait(readable, "cached reads did not recover after " + mode, 100)
        cached()
        wait(writer_ready, "writer did not recover after " + mode, 100)
        write("recovered-" + mode, mode.encode())
    checks.append("one-way and complete control blackholes fence cached images and recover required replica writes")
    try:
        database("pause")
        started = time.monotonic()
        wait(fenced, "cached image outlived the unavailable database", 100)
        evidence["database"] = time.monotonic() - started
    finally:
        database("resume")
    wait(readable, "database recovery did not restore cached reads", 100)
    cached()
    wait(writer_ready, "writer did not recover with database", 100)
    write("database-recovered", b"durable after database pause")
    checks.append("database pause fences cached images and recovers required replica durability")
    return evidence
