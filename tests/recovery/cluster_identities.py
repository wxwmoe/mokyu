"""Two real data nodes share UUID identities, encrypted staging and pack recovery."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture, database = Fixture(), Database()
    assert fixture.backend and database.type == "postgres"
    peer = boto3.client("s3", endpoint_url=os.environ["MOKYU_TEST_PEER_ENDPOINT"], region_name="us-east-1",
                        aws_access_key_id=fixture.credentials["access_key"],
                        aws_secret_access_key=fixture.credentials["secret_key"], config=fixture.client.meta.config)
    key = fixture.cli("key", "create", fixture.name + "-key", "block")
    fixture.cli("storage", "key", "set", fixture.name, key["id"])
    profile = fixture.cli("storage", "profile", "show", fixture.name)
    profile.update(upload_cache=True, cache_enabled=True, pack_range_enabled=False, pack_max_size="16MiB")
    fixture.cli("storage", "profile", "set", fixture.name, "-", value=profile)
    clients = [fixture.client, peer]
    bodies = [os.urandom(8192) for _ in range(16)]

    def put(item):
        index, body = item
        clients[index % 2].put_object(Bucket=fixture.name, Key="parallel-" + str(index), Body=body)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(put, enumerate(bodies)))
    for index, body in enumerate(bodies):
        with clients[1 - index % 2].get_object(Bucket=fixture.name, Key="parallel-" + str(index))["Body"] as result:
            assert result.read() == body
    storage = uuid.UUID(fixture.store["id"])
    wait(lambda: not database.query("SELECT 1 FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE c.storage_id=%s", storage),
         "shared staging did not drain")
    rows = database.query("SELECT encoding_id,nonce_id,nonce FROM chunk_encodings WHERE storage_id=%s AND state='ready'", storage)
    assert len(rows) == len(bodies)
    assert len({row["nonce_id"] for row in rows}) == len(rows)
    assert len({bytes(row["nonce"]) for row in rows}) == len(rows)
    assert all(row["encoding_id"].version == 7 for row in rows)
    print("PASS two writers reserve distinct nonces and both readers consume shared staging", flush=True)

    raw = os.urandom(9 * 1024 * 1024 + 17)
    fixture.put("packed", raw)
    wait(lambda: not database.query("SELECT 1 FROM pending_uploads p JOIN chunks c ON c.id=p.chunk_id WHERE c.storage_id=%s", storage),
         "pack inputs did not drain")
    before = database.query("SELECT id,hash FROM chunks WHERE storage_id=%s ORDER BY id", storage)
    profile = fixture.cli("storage", "profile", "show", fixture.name)
    profile["pack_enabled"] = True
    fixture.cli("storage", "profile", "set", fixture.name, "-", value=profile)
    fixture.finished(fixture.cli("pack", "run"))
    packs = database.query("SELECT id FROM packs WHERE storage_id=%s AND state='ready'", storage)
    assert packs and all(row["id"].version == 7 for row in packs)
    for client in clients:
        with client.get_object(Bucket=fixture.name, Key="packed")["Body"] as result:
            assert result.read() == raw
        with client.get_object(Bucket=fixture.name, Key="packed", Range="bytes=4194200-4194400")["Body"] as result:
            assert result.read() == raw[4194200:4194401]
    profile = fixture.cli("storage", "profile", "show", fixture.name)
    profile["pack_enabled"] = False
    fixture.cli("storage", "profile", "set", fixture.name, "-", value=profile)
    for pack in packs:
        fixture.finished(fixture.cli("pack", "unpack", str(pack["id"]), "--execute"))
    assert database.query("SELECT id,hash FROM chunks WHERE storage_id=%s ORDER BY id", storage) == before
    for client in clients:
        with client.get_object(Bucket=fixture.name, Key="packed")["Body"] as result:
            assert result.read() == raw
    print("PASS cluster pack/unpack retains UUIDs and both nodes return full and Range bytes", flush=True)


if __name__ == "__main__":
    main()
