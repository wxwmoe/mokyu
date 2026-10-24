"""Migration checks against an empty, disposable database with no running gateway.

Uses the MOKYU_TEST_BINARY, MOKYU_TEST_CONFIG and MOKYU_TEST_DATABASE_FILE settings.
Requires psycopg and MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
conninfo = Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip()
data_path = Path(os.environ.get('MOKYU_TEST_DATA', '/data'))
db = psycopg.connect(conninfo, autocommit=True)
assert db.execute("SELECT to_regclass('gateway_meta'),to_regclass('mokyu_meta'),to_regclass('_sqlx_migrations')").fetchone() == (None, None, None)
process = None
migrations = Path(__file__).resolve().parents[1] / 'migrations'
checksums = {int(path.stem.split('_')[0]): hashlib.sha384(path.read_bytes()).digest()
             for path in migrations.glob('*.sql')}
latest = max(checksums)


def cli(*args):
    result = subprocess.run(command + ['cli', *args], capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def start(binary=None):
    global process
    process = subprocess.Popen([binary or command[0], *command[1:]], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)


def stopped_with(message):
    _, stderr = process.communicate(timeout=40)
    assert process.returncode != 0 and message in stderr, stderr


def ready():
    for _ in range(120):
        if process.poll() is not None:
            raise AssertionError(process.stderr.read())
        try:
            cli('status')
            assert db.execute("SELECT to_regclass('catalog_build')").fetchone() == (None,)
            assert db.execute("SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname IN ('objects_catalog_size','objects_catalog_modified','objects_catalog_category','objects_catalog_search') AND i.indisvalid").fetchone() == (4,)
            assert db.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND ((table_name='tasks' AND column_name='batch_cursor') OR (table_name='nodes' AND column_name IN ('credential_version','load_score','load_sampled_at')))").fetchone() == (0,)
            return
        except subprocess.CalledProcessError:
            time.sleep(.25)
    raise AssertionError('gateway readiness timed out')


def stop():
    process.terminate()
    process.communicate(timeout=40)


def reset_local_identity():
    (data_path / 'node.json').unlink(missing_ok=True)


def history():
    return db.execute('SELECT version,installed_on,success,checksum,execution_time FROM _sqlx_migrations ORDER BY version').fetchall()


try:
    if db.info.server_version < 180000:
        start()
        stopped_with('PostgreSQL 18 or newer is required')
        assert db.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public'").fetchone() == (0,)
        print('PASS unsupported PostgreSQL leaves the empty database untouched', flush=True)
        sys.exit(0)

    # A nonempty database without metadata must remain untouched.
    db.execute('CREATE TABLE buckets (sentinel integer)')
    start()
    stopped_with('nonempty database requires valid mokyu_meta.schema_version')
    assert db.execute("SELECT to_regclass('gateway_meta'),to_regclass('key_fingerprints')").fetchone() == (None, None)
    assert db.execute("SELECT to_regclass('_sqlx_migrations')").fetchone() == (None,)
    db.execute('DROP TABLE buckets')
    print('PASS unknown database refuses initialization without writing schema or history', flush=True)

    # A populated old schema is rejected before any migration changes.
    for version_limit in (33, 70):
        db.execute("CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        with db.transaction():
            db.execute("SELECT set_config('media_gateway.backend_identity','storage-registry',false)")
            for path in sorted(migrations.glob('*.sql')):
                version = int(path.stem.split('_')[0])
                if version > version_limit:
                    continue
                db.execute(path.read_text())
                db.execute('INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(%s,%s,true,%s,0)',
                           (version, path.stem.split('_', 1)[1].replace('_', ' '), checksums[version]))
        prior_history = history()
        tables = db.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename").fetchall()
        legacy_storage = '00000000-0000-0000-0000-000000000042'
        db.execute("INSERT INTO storages(id,name,type,path) VALUES(%s,'legacy-local','local','/data/storage')", (legacy_storage,))
        db.execute("INSERT INTO buckets(id,name,storage_id) VALUES(gen_random_uuid(),'legacy-preserved',%s)", (legacy_storage,))
        buckets = db.execute('SELECT * FROM buckets').fetchall()
        start()
        stopped_with('empty store')
        assert db.execute('SELECT * FROM buckets').fetchall() == buckets
        assert history() == prior_history
        assert db.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename").fetchall() == tables
        assert db.execute('SELECT schema_version FROM mokyu_meta').fetchone() == (version_limit,)
        db.execute('DELETE FROM buckets')
        db.execute('DELETE FROM storages')
        start()
        ready()
        assert db.execute('SELECT schema_version FROM mokyu_meta').fetchone() == (latest,)
        assert history()[:len(prior_history)] == prior_history
        stop()
        reset_local_identity()
        with db.transaction():
            db.execute('DROP SCHEMA public CASCADE')
            db.execute('CREATE SCHEMA public')
    print('PASS empty schemas upgrade and populated schemas remain unchanged', flush=True)

    start()
    ready()
    assert db.execute('SELECT schema_version FROM mokyu_meta').fetchone()[0] == latest
    assert db.execute("SELECT to_regclass('gateway_meta')").fetchone()[0] is None
    constraints = {row[0] for row in db.execute("SELECT conname FROM pg_constraint WHERE conrelid='mokyu_meta'::regclass AND contype IN ('p','c')").fetchall()}
    assert {'mokyu_meta_pkey', 'mokyu_meta_singleton_check'} <= constraints
    assert db.execute("SELECT count(*) FROM pg_constraint WHERE conrelid='mokyu_meta'::regclass AND starts_with(conname,'gateway_meta_')").fetchone()[0] == 0
    assert db.execute("SELECT to_regclass('mokyu_meta_pkey'),to_regclass('gateway_meta_pkey')").fetchone() == ('mokyu_meta_pkey', None)
    before = history()
    expected = checksums[1]
    assert [row[0] for row in before] == sorted(checksums)
    assert all(row[2] and row[3] == checksums[row[0]] for row in before)
    preserved_storage = '00000000-0000-0000-0000-000000000043'
    db.execute("INSERT INTO storages(id,name,type,path) VALUES(%s,'migration-local','local','/data/storage')", (preserved_storage,))
    db.execute("INSERT INTO buckets(id,name,storage_id) VALUES(gen_random_uuid(),'migration-preserved',%s)", (preserved_storage,))
    bucket = db.execute("SELECT id,created_at FROM buckets WHERE name='migration-preserved'").fetchone()
    deployment = db.execute('SELECT deployment_id FROM mokyu_meta').fetchone()
    stop()
    start()
    ready()
    assert history() == before
    assert db.execute("SELECT id,created_at FROM buckets WHERE name='migration-preserved'").fetchone() == bucket
    assert db.execute('SELECT deployment_id FROM mokyu_meta').fetchone() == deployment
    stop()
    print('PASS migrations initialize once; restart preserves data and migration history', flush=True)

    db.execute("UPDATE _sqlx_migrations SET checksum=decode(repeat('00',48),'hex') WHERE version=1")
    start()
    stopped_with('PostgreSQL migration checksum changed')
    db.execute('UPDATE _sqlx_migrations SET checksum=%s WHERE version=1', (expected,))
    db.execute("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(999,'future',true,%s,0)", (expected,))
    start()
    stopped_with('schema version does not match the migration history')
    db.execute('DELETE FROM _sqlx_migrations WHERE version=999')
    db.execute('UPDATE mokyu_meta SET schema_version=999')
    start()
    stopped_with('database schema is newer')
    db.execute('UPDATE mokyu_meta SET schema_version=%s', (latest,))
    start()
    ready()
    assert history() == before
    stop()
    print('PASS changed checksum, missing migration and newer schema refuse startup', flush=True)
finally:
    if process is not None and process.poll() is None:
        stop()
    db.close()
