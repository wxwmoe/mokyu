"""Online scan boundaries and managed outputs in an opted-in disposable fixture."""
import os
import sqlite3
import time
import uuid

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def run(restart, conversion_boundary='cdc-before-read'):
    fixture, database = Fixture(), Database()
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(upload_retention='1s', unreferenced_grace='1s', min_storage_duration='0s')
    if os.environ.get('MOKYU_TEST_REPLICATION_MODE') == 'raw':
        profile.update(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    checks = []

    def start(scope, **options):
        value = dict(source_bucket=fixture.bucket_id, target_bucket=fixture.bucket_id,
            operation='copy', scope=scope, target_prefix='out/', conflict='report', permissions='inherit')
        value.update(options)
        return fixture.api('/api/v1/replications', value, expected=202)['task_id']

    def state(task):
        return fixture.api('/api/v1/replications/' + task)

    def command(task, action):
        fixture.api('/api/v1/replications/' + task + '/command', action, expected=202)

    def finish(task, expected='completed'):
        result = wait(lambda: row if (row := state(task))['phase'] in ('complete', 'cancelled')
            or row['state'] == 'failed' else None, 'online replication stalled', seconds=180)
        assert result['state'] == expected, result
        assert not database.query('SELECT * FROM transfer_subscriptions WHERE task_id=%s', uuid.UUID(task))
        assert not database.query('SELECT * FROM transfer_changes WHERE task_id=%s', uuid.UUID(task))
        actual = database.query('SELECT COUNT(*) AS n,COALESCE(SUM(s.size),0) AS size FROM objects o '
            'JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=%s', uuid.UUID(fixture.bucket_id))[0]
        quota = database.query("SELECT object_count,used_bytes FROM quota_scopes WHERE scope='bucket' AND scope_id=%s",
            uuid.UUID(fixture.bucket_id))[0]
        assert (quota['object_count'], quota['used_bytes']) == (actual['n'], actual['size']), (quota, actual)
        return result

    def reached(marker):
        wait(lambda: marker.with_suffix('.hit').exists(), 'online boundary was not reached')

    def put(key, body, public=False):
        fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body,
            ACL='public-read' if public else 'private')

    def delete(key):
        fixture.client.delete_object(Bucket=fixture.name, Key=key)

    def exists(key):
        return any(row['Key'] == key for row in fixture.client.list_objects_v2(
            Bucket=fixture.name, Prefix=key).get('Contents', []))

    def public(key):
        return any(grant['Grantee'].get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers'
            and grant['Permission'] == 'READ' for grant in fixture.client.get_object_acl(
                Bucket=fixture.name, Key=key)['Grants'])

    for index in range(70):
        put(f'scan/{index:03}', f'initial-{index}'.encode())
    page, seal = fixture.arm('replication-page'), fixture.arm('replication-sealed')
    try:
        task = start(dict(selection='prefix', prefix='scan/'))
        reached(page)
        put('scan/000', b'captured replacement', True)
        fixture.client.put_object_acl(Bucket=fixture.name, Key='scan/001', ACL='public-read')
        metadata_etag = fixture.client.head_object(Bucket=fixture.name, Key='scan/002')['ETag']
        fixture.client.copy_object(Bucket=fixture.name, Key='scan/002',
            CopySource={'Bucket': fixture.name, 'Key': 'scan/002'}, MetadataDirective='REPLACE',
            ContentType='image/jpeg', Metadata={'capture': 'updated'})
        delete('scan/003')
        delete('scan/065')
        put('scan/065', b'recreated before sealing')
        put('scan/-late', b'behind the cursor')
        put('scan/zz-late', b'beyond the initial upper key')
        page.unlink(missing_ok=True)
        reached(seal)
        assert state(task)['captured'] and state(task)['sealed']
        command(task, 'pause')
        put('scan/000', b'after the boundary')
        fixture.client.put_object_acl(Bucket=fixture.name, Key='scan/001', ACL='private')
        delete('scan/065')
        put('scan/zz-excluded', b'outside this finite task')
        restart(lambda: seal.unlink(missing_ok=True))
        assert state(task)['state'] == 'paused'
        time.sleep(1.1)
        fixture.finished(fixture.cli('maintenance', 'run', 'gc'))
        command(task, 'resume')
        row = finish(task)
        assert row['progress']['discovered'] == 72 and row['progress']['completed'] == 71, row
        assert row['progress']['skipped'] == 1 and row['progress']['conflicts'] == 0, row
        fixture.read('out/000', b'captured replacement')
        fixture.read('out/065', b'recreated before sealing')
        fixture.read('out/-late', b'behind the cursor')
        fixture.read('out/zz-late', b'beyond the initial upper key')
        assert public('out/000') and public('out/001')
        fixture.read('out/002', b'initial-2')
        metadata = fixture.client.head_object(Bucket=fixture.name, Key='out/002')
        assert metadata['ETag'] == metadata_etag and metadata['ContentType'] == 'image/jpeg'
        assert metadata['Metadata'] == {'capture': 'updated'}, metadata
        assert not exists('out/003') and not exists('out/zz-excluded')
        fixture.read('scan/000', b'after the boundary')
    finally:
        page.unlink(missing_ok=True)
        seal.unlink(missing_ok=True)
    checks.append('bounded pages capture behind-cursor and beyond-upper-key writes, same-ETag metadata, ACL and delete/recreate; sealed pins survive restart and GC')

    for propagate in (False, True):
        key = 'deletion/' + str(propagate)
        put(key, b'owned output')
        put(key + '-preexisting', b'keep independent output')
        fixture.client.copy_object(Bucket=fixture.name, Key='out/' + key + '-preexisting',
            CopySource={'Bucket': fixture.name, 'Key': key + '-preexisting'})
        published = fixture.arm('replication-published')
        try:
            task = start(dict(selection='objects', keys=[key, key + '-preexisting']),
                propagate_deletes=propagate)
            reached(published)
            delete(key)
            delete(key + '-preexisting')
            published.unlink(missing_ok=True)
            finish(task)
            assert exists('out/' + key) != propagate
            fixture.read('out/' + key + '-preexisting', b'keep independent output')
        finally:
            published.unlink(missing_ok=True)
    checks.append('deletion propagation is opt-in and never removes a preexisting equivalent target')

    for race in (False, True):
        key = 'managed/' + str(race)
        put(key, b'first')
        published = fixture.arm('replication-published')
        try:
            task = start(dict(selection='objects', keys=[key]), propagate_deletes=race)
            reached(published)
            if race:
                put('out/' + key, b'independent target')
                delete(key)
            else:
                put(key, b'new captured body', True)
            published.unlink(missing_ok=True)
            row = finish(task, 'failed' if race else 'completed')
            fixture.read('out/' + key, b'independent target' if race else b'new captured body')
            assert row['progress']['conflicts'] == int(race)
            if not race:
                assert public('out/' + key)
        finally:
            published.unlink(missing_ok=True)
    checks.append('captured replacements reuse this task ownership; independent target writes fence deletion')

    put('ignored/value', b'unchanged body')
    published = fixture.arm('replication-published')
    try:
        task = start(dict(selection='prefix', prefix='ignored/'), target_prefix='ignored-out/', permissions='ignore')
        reached(published)
        fixture.client.put_object_acl(Bucket=fixture.name, Key='ignored/value', ACL='public-read')
        published.unlink(missing_ok=True)
        finish(task)
        assert not public('ignored-out/value')
    finally:
        published.unlink(missing_ok=True)
    checks.append('ignoring permissions does not replay same-body ACL changes')

    put('move/a', b'initial move')
    page, seal = fixture.arm('replication-page'), fixture.arm('replication-sealed')
    try:
        task = start(dict(selection='prefix', prefix='move/'), target_prefix='move-out/', operation='move')
        reached(page)
        put('move/z', b'captured move')
        page.unlink(missing_ok=True)
        reached(seal)
        put('move/z', b'newer than the boundary')
        seal.unlink(missing_ok=True)
        row = finish(task, 'failed')
        assert row['progress']['completed'] == 1 and row['progress']['conflicts'] == 1, row
        fixture.read('move-out/a', b'initial move')
        fixture.read('move/z', b'newer than the boundary')
        assert not exists('move/a') and not exists('move-out/z')
    finally:
        page.unlink(missing_ok=True)
        seal.unlink(missing_ok=True)
    checks.append('Move preserves a source replaced after sealing and does not replay its own deletes onto the destination')

    settings = database.settings

    def connect():
        if database.type == 'postgres':
            import psycopg
            return psycopg.connect(host=settings['host'], user=settings['user'],
                password=settings['password'], dbname=settings['name'], autocommit=True)
        if database.type == 'mysql':
            import pymysql
            return pymysql.connect(host=settings['host'], user=settings['user'],
                password=settings['password'], database=settings['name'], autocommit=True)
        return sqlite3.connect(settings['path'], timeout=15, isolation_level=None)

    def execute(connection, sql, *parameters):
        cursor = connection.cursor()
        cursor.execute(sql.replace('%s', '?') if database.type == 'sqlite' else sql, parameters)
        cursor.close()

    def version(key):
        return database.query('SELECT stream_id FROM objects WHERE bucket_id=%s AND object_key=%s',
            uuid.UUID(fixture.bucket_id), key if database.type == 'postgres' else key.encode())[0]['stream_id']

    def make_public(connection, key):
        stream = version(key)
        execute(connection, 'UPDATE streams SET public_read=true WHERE id=%s', stream)
        bucket = uuid.UUID(fixture.bucket_id)
        execute(connection, 'UPDATE objects SET catalog_public=true WHERE bucket_id=%s AND object_key=%s',
            bucket if database.type == 'postgres' else bucket.bytes,
            key if database.type == 'postgres' else key.encode())

    put('late/a', b'initial bounded set')
    put('late/b', b'rollback body')
    older = connect()
    if database.type != 'sqlite':
        execute(older, 'BEGIN')
        execute(older, 'SELECT public_read FROM streams WHERE id=%s', version('late/a'))
    page, seal = fixture.arm('replication-page'), fixture.arm('replication-sealed')
    writer = None
    try:
        task = start(dict(selection='prefix', prefix='late/'), target_prefix='late-out/')
        reached(page)
        if database.type == 'sqlite':
            execute(older, 'BEGIN')
        make_public(older, 'late/a')
        execute(older, 'COMMIT')
        execute(older, 'BEGIN')
        make_public(older, 'late/b')
        execute(older, 'ROLLBACK')
        assert not database.query('SELECT * FROM transfer_changes WHERE task_id=%s AND object_key=%s',
            uuid.UUID(task), 'late/b')
        put('late/z', b'late committed value')
        writer = connect()
        execute(writer, 'BEGIN')
        make_public(writer, 'late/z')
        page.unlink(missing_ok=True)
        if database.type != 'sqlite':
            wait(lambda: state(task)['phase'] == 'catchup', 'scan did not release its row locks')
        time.sleep(.15)
        assert not seal.with_suffix('.hit').exists(), 'seal passed an unfinished publication'
        execute(writer, 'COMMIT')
        reached(seal)
        seal.unlink(missing_ok=True)
        finish(task)
        fixture.read('late-out/z', b'late committed value')
        assert public('late-out/a') and public('late-out/z') and not public('late-out/b')
    finally:
        page.unlink(missing_ok=True)
        seal.unlink(missing_ok=True)
        older.close()
        if writer is not None:
            writer.close()
    checks.append('activation covers older transactions; rollback is absent; sealing waits for a publication outside the bounded scan')

    put('cancel/source', b'cancelled scope')
    page = fixture.arm('replication-page')
    try:
        task = start(dict(selection='prefix', prefix='cancel/'), target_prefix='cancel-out/')
        reached(page)
        for index in range(140):
            put(f'cancel/{index:03}', b'pending change')
        command(task, 'cancel')
        page.unlink(missing_ok=True)
        assert finish(task)['phase'] == 'cancelled'
        assert not exists('cancel-out/source')
        count = database.query('SELECT subscribers FROM bucket_change_gates WHERE bucket_id=%s',
            uuid.UUID(fixture.bucket_id))[0]['subscribers']
        assert count == 0, count
    finally:
        page.unlink(missing_ok=True)
    checks.append('cancellation drains captured changes in bounded batches and releases the subscription')

    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value={**profile,
        'cdc_enabled': True, 'deduplication_enabled': True, 'write_processing': 'background',
        'upload_cache': True, 'pack_enabled': False, 'compression_enabled': False})
    conversion, page = fixture.arm(conversion_boundary), fixture.arm('replication-page')
    try:
        body = b'physical conversion keeps logical content\x00' * 65536
        put('conversion/source', body, True)
        reached(conversion)
        original = version('conversion/source')
        assert database.query('SELECT payload_type FROM streams WHERE id=%s', original)[0]['payload_type'] == 'raw'
        task = start(dict(selection='objects', keys=['conversion/source']), target_prefix='converted/')
        reached(page)
        conversion.unlink(missing_ok=True)
        wait(lambda: database.query('SELECT payload_type FROM streams WHERE id=%s', original)[0]['payload_type']
            == 'chunks', 'captured source conversion did not complete', seconds=180)
        page.unlink(missing_ok=True)
        row = finish(task)
        assert row['progress']['completed'] == 1 and row['progress']['conflicts'] == 0, row
        fixture.read('converted/conversion/source', body)
        assert public('converted/conversion/source')
        assert version('conversion/source') == original
    finally:
        conversion.unlink(missing_ok=True)
        page.unlink(missing_ok=True)
        profile['revision'] = fixture.cli('storage', 'profile', 'show', fixture.name)['revision']
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    checks.append('raw-to-chunks background conversion during a captured scan preserves content, ACL and source identity')
    return checks
