"""Migration checks against an empty, disposable database with no running gateway.

Uses the MGW_TEST_BINARY, MGW_TEST_CONFIG and MGW_TEST_DATABASE_FILE settings.
Requires psycopg and MGW_TEST_ALLOW_STATE_CHANGES=isolated-only.
Set MGW_TEST_BASELINE_BINARY to a schema-1 binary to also check upgrade rollback.
"""
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

import psycopg

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
command = [os.environ['MGW_TEST_BINARY'], '--config', os.environ['MGW_TEST_CONFIG']]
conninfo = Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text().strip()
db = psycopg.connect(conninfo, autocommit=True)
assert db.execute("SELECT to_regclass('gateway_meta'),to_regclass('_sqlx_migrations')").fetchone() == (None, None)
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
    process = subprocess.Popen([binary or command[0], *command[1:], 'serve'], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)


def stopped_with(message):
    _, stderr = process.communicate(timeout=40)
    assert process.returncode != 0 and message in stderr, stderr


def ready():
    for _ in range(120):
        if process.poll() is not None:
            raise AssertionError(process.stderr.read())
        try:
            cli('status')
            return
        except subprocess.CalledProcessError:
            time.sleep(.25)
    raise AssertionError('gateway readiness timed out')


def stop():
    process.terminate()
    process.communicate(timeout=40)


def history():
    return db.execute('SELECT version,installed_on,success,checksum,execution_time FROM _sqlx_migrations ORDER BY version').fetchall()


try:
    # A failure midway through the baseline must undo earlier DDL and its history.
    db.execute('CREATE TABLE buckets (sentinel integer)')
    start()
    stopped_with('apply database migrations')
    assert db.execute("SELECT to_regclass('gateway_meta'),to_regclass('key_fingerprints')").fetchone() == (None, None)
    assert history() == []
    db.execute('DROP TABLE buckets')
    print('PASS failed baseline rolls back DDL and migration record', flush=True)

    # Block the history INSERT after the schema SQL, then disconnect its owner.
    with psycopg.connect(conninfo) as blocker:
        blocker.execute('LOCK TABLE _sqlx_migrations IN SHARE MODE')
        start()
        for _ in range(100):
            waiting = db.execute("""SELECT pid FROM pg_stat_activity WHERE datname=current_database()
                AND wait_event_type='Lock' AND query ILIKE '%INSERT INTO _sqlx_migrations%'""").fetchone()
            if waiting:
                break
            assert process.poll() is None, process.stderr.read()
            time.sleep(.1)
        else:
            raise AssertionError('migration did not reach history insertion')
        assert db.execute('SELECT pg_terminate_backend(%s)', waiting).fetchone()[0]
        stopped_with('apply database migrations')
    assert db.execute("SELECT to_regclass('gateway_meta')").fetchone()[0] is None
    assert history() == []
    print('PASS interrupted migration rolls back schema and can be retried', flush=True)

    if baseline := os.environ.get('MGW_TEST_BASELINE_BINARY'):
        start(baseline)
        ready()
        cli('bucket', 'create', 'upgrade-preserved')
        old_bucket = db.execute("SELECT id,created_at FROM buckets WHERE name='upgrade-preserved'").fetchone()
        stop()
        old_history = history()
        assert [row[0] for row in old_history] == [1]
        db.execute('CREATE INDEX tasks_completed ON tasks(id)')
        start()
        stopped_with('apply database migrations')
        assert db.execute("SELECT to_regclass('chunks_deleted'),to_regclass('uploads_finished')").fetchone() == (None, None)
        assert db.execute('SELECT schema_version FROM gateway_meta').fetchone()[0] == 1
        assert history() == old_history
        db.execute('DROP INDEX tasks_completed')
        with psycopg.connect(conninfo) as blocker:
            blocker.execute('LOCK TABLE _sqlx_migrations IN SHARE MODE')
            start()
            for _ in range(100):
                waiting = db.execute("""SELECT pid FROM pg_stat_activity WHERE datname=current_database()
                    AND wait_event_type='Lock' AND query ILIKE '%INSERT INTO _sqlx_migrations%'""").fetchone()
                if waiting:
                    break
                assert process.poll() is None, process.stderr.read()
                time.sleep(.1)
            else:
                raise AssertionError('upgrade did not reach history insertion')
            assert db.execute('SELECT pg_terminate_backend(%s)', waiting).fetchone()[0]
            stopped_with('apply database migrations')
        assert db.execute("SELECT to_regclass('chunks_deleted'),to_regclass('uploads_finished')").fetchone() == (None, None)
        assert db.execute('SELECT schema_version FROM gateway_meta').fetchone()[0] == 1
        assert history() == old_history
        assert db.execute("SELECT id,created_at FROM buckets WHERE name='upgrade-preserved'").fetchone() == old_bucket
        print('PASS failed and interrupted schema-1 upgrade preserves data and rolls back new indexes', flush=True)

    start()
    ready()
    assert db.execute('SELECT schema_version FROM gateway_meta').fetchone()[0] == latest
    before = history()
    expected = checksums[1]
    assert [row[0] for row in before] == sorted(checksums)
    assert all(row[2] and row[3] == checksums[row[0]] for row in before)
    cli('bucket', 'create', 'migration-preserved')
    bucket = db.execute("SELECT id,created_at FROM buckets WHERE name='migration-preserved'").fetchone()
    deployment = db.execute('SELECT deployment_id FROM gateway_meta').fetchone()
    stop()
    start()
    ready()
    assert history() == before
    assert db.execute("SELECT id,created_at FROM buckets WHERE name='migration-preserved'").fetchone() == bucket
    assert db.execute('SELECT deployment_id FROM gateway_meta').fetchone() == deployment
    stop()
    print('PASS migrations initialize once; restart preserves data and migration history', flush=True)

    db.execute("UPDATE _sqlx_migrations SET checksum=decode(repeat('00',48),'hex') WHERE version=1")
    start()
    stopped_with('previously applied but has been modified')
    db.execute('UPDATE _sqlx_migrations SET checksum=%s WHERE version=1', (expected,))
    db.execute("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(999,'future',true,%s,0)", (expected,))
    start()
    stopped_with('applied but is missing')
    db.execute('DELETE FROM _sqlx_migrations WHERE version=999')
    db.execute('UPDATE gateway_meta SET schema_version=999')
    start()
    stopped_with('database schema is newer')
    db.execute('UPDATE gateway_meta SET schema_version=%s', (latest,))
    start()
    ready()
    assert history() == before
    stop()
    print('PASS changed checksum, missing migration and newer schema refuse startup', flush=True)
finally:
    if process is not None and process.poll() is None:
        stop()
    db.close()
