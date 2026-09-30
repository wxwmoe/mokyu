"""Runner restarts, then kills/restarts the isolated app at durable catalog checkpoints."""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
marker = Path(os.environ['MOKYU_TEST_RESTART_MARKER'])
assert not marker.exists(), 'use a fresh marker'
fault = Path(os.environ['MOKYU_TEST_FAULT_DIR']) / 'catalog-after-batch'
url = os.environ['MOKYU_TEST_WEB']
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = 'catalog-restart-' + uuid.uuid4().hex[:10]
bucket = json.loads(subprocess.check_output(command + ['bucket', 'create', name], text=True))
client = requests.Session()
reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})


def wait(predicate):
    deadline = time.monotonic() + 180
    while not predicate():
        assert time.monotonic() < deadline, 'runner did not complete the requested restart'
        time.sleep(.1)


try:
    # Empty streams are valid complete objects and need no backend extents.
    for key in ('empty-a', 'empty-b'):
        stream = uuid.uuid4()
        with db.transaction():
            db.execute("INSERT INTO streams(id,bucket_id,object_key,kind,state,size,etag) VALUES(%s,%s,%s,'object','ready',0,'d41d8cd98f00b204e9800998ecf8427e')", (stream, bucket['id'], key))
            db.execute('INSERT INTO objects(bucket_id,key,stream_id,write_epoch) VALUES(%s,%s,%s,%s)', (bucket['id'], key, stream, stream))
    ledger = db.execute("SELECT used_bytes,reserved_bytes,inflight_bytes,object_count FROM quota_accounts WHERE kind='bucket' AND id=%s", (bucket['id'],)).fetchone()
    db.execute('UPDATE objects SET catalog_size=NULL,catalog_modified=NULL,catalog_type=NULL,catalog_kind=NULL,catalog_public=NULL WHERE bucket_id=%s', (bucket['id'],))
    db.execute('DROP INDEX CONCURRENTLY objects_catalog_search')
    try:
        db.execute('CREATE UNIQUE INDEX CONCURRENTLY objects_catalog_search ON objects(catalog_public)')
        raise AssertionError('duplicate public flags should leave an invalid index')
    except psycopg.errors.UniqueViolation:
        pass
    assert db.execute("SELECT indisvalid FROM pg_index WHERE indexrelid='objects_catalog_search'::regclass").fetchone() == (False,)
    db.execute("UPDATE catalog_build SET phase='indexes',cursor_bucket=NULL,cursor_key=NULL,scanned=0")
    fault.with_suffix('.hit').unlink(missing_ok=True)
    fault.touch()
    marker.write_text('rebuild')
    print('READY restart to rebuild an interrupted index and backfill old objects', flush=True)
    wait(fault.with_suffix('.hit').exists)
    checkpoint = db.execute('SELECT phase,scanned,cursor_bucket,cursor_key FROM catalog_build').fetchone()
    assert checkpoint[0] == 'backfill' and checkpoint[1] > 0 and checkpoint[2], checkpoint
    index = db.execute("SELECT i.indisvalid,a.amname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam WHERE i.indexrelid='objects_catalog_search'::regclass").fetchone()
    assert index == (True, 'gin'), index
    path = url + '/api/buckets/' + bucket['id'] + '/objects'
    assert client.get(path).status_code == 200
    reply = client.get(path, params={'q': 'empty-a', 'mode': 'exact'})
    assert reply.status_code == 200 and reply.json()['objects'][0]['size'] == '0', reply.text
    reply = client.get(path, params={'q': 'empty'})
    assert reply.status_code == 503 and reply.json()['code'] == 'CatalogBuilding', reply.text
    assert db.execute("SELECT used_bytes,reserved_bytes,inflight_bytes,object_count FROM quota_accounts WHERE kind='bucket' AND id=%s", (bucket['id'],)).fetchone() == ledger
    marker.write_text('interrupt')
    print('READY kill after committed backfill batch; cursor and quota are consistent', flush=True)
    wait(marker.with_suffix('.resumed').exists)
    wait(lambda: db.execute('SELECT phase FROM catalog_build').fetchone() == ('ready',))
    final = db.execute('SELECT scanned FROM catalog_build').fetchone()[0]
    assert final >= checkpoint[1]
    assert db.execute('SELECT count(*) FROM objects WHERE stream_id IS NOT NULL AND catalog_size IS NULL').fetchone() == (0,)
    reply = client.get(path, params={'q': 'empty'})
    assert reply.status_code == 200 and len(reply.json()['objects']) == 2, reply.text
    assert db.execute("SELECT used_bytes,reserved_bytes,inflight_bytes,object_count FROM quota_accounts WHERE kind='bucket' AND id=%s", (bucket['id'],)).fetchone() == ledger
    print('PASS invalid concurrent index recovery, checkpoint restart, fallback browsing and quota-neutral backfill', flush=True)
finally:
    fault.unlink(missing_ok=True)
    db.close()
