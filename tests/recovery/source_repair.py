"""Normal uploads recover quarantined sources without changing logical content.

Uses the source_quarantine fixture and a disposable primary database. PyMySQL is
needed for MySQL/MariaDB, psycopg for PostgreSQL; SQLite uses the standard library.
"""
import base64
import hashlib
import json
import os
import re
import sqlite3
import tomllib
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.storage.source_quarantine import Fixture, rejected, wait


class Database:
    def __init__(self):
        config = tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())
        self.config = config
        self.settings = config['database']
        self.type = self.settings.get('type', 'postgres')
        self.raw_location = 'id' if self.type == 'postgres' else 'location_id'

    def query(self, statement, *parameters):
        settings = self.settings
        if self.type == 'sqlite':
            path = settings.get('path') or str(Path(self.config['instance']['data_path']) / 'mokyu.db')
            connection = sqlite3.connect(path, timeout=10)
            connection.row_factory = sqlite3.Row
        elif self.type == 'mysql':
            import pymysql
            connection = pymysql.connect(host=settings['host'], port=settings.get('port', 3306),
                user=settings['user'], password=settings['password'], database=settings['name'],
                autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        else:
            import psycopg
            from psycopg.rows import dict_row
            connection = psycopg.connect(host=settings['host'], port=settings.get('port', 5432),
                user=settings['user'], password=settings['password'], dbname=settings['name'],
                autocommit=True, row_factory=dict_row)
        if self.type == 'postgres' and 'location_id' in statement:
            column = 'encoding_id' if 'FROM chunk_encodings' in statement else 'id'
            statement = re.sub(r'\blocation_id\b', column + ' AS location_id', statement)
        parameters = tuple(value if self.type == 'postgres' else value.bytes if isinstance(value, uuid.UUID)
                           else value for value in parameters)
        try:
            cursor = connection.cursor()
            cursor.execute(statement.replace('%s', '?') if self.type == 'sqlite' else statement, parameters)
            rows = [dict(row) for row in cursor.fetchall()] if cursor.description else []
            if self.type == 'sqlite':
                connection.commit()
            return rows
        finally:
            connection.close()

    def chunks(self, fixture):
        return self.query('SELECT id,current_encoding_id,pack_id,hash,raw_size FROM chunks WHERE storage_id=%s AND state=\'ready\' ORDER BY id', uuid.UUID(fixture.store['id']))

    def candidates(self, fixture):
        return self.query('SELECT encoding_id,location_id,chunk_id,state,owner_stream,recovery_pending FROM chunk_encodings WHERE storage_id=%s AND recovery_pending ORDER BY encoding_id', uuid.UUID(fixture.store['id']))


def location(fixture, identity, packed=False):
    value = uuid.UUID(bytes=identity) if isinstance(identity, bytes) else uuid.UUID(str(identity))
    path = ('packs/' if packed else '') + value.hex[-2:] + '/' + value.hex
    return fixture.prefix + path if fixture.backend else str(fixture.root / path)


def isolate(fixture, database, raw):
    fixture.put('original', raw)
    rows = database.chunks(fixture)
    assert len(rows) == 1, rows
    def durable():
        row = database.chunks(fixture)[0]
        encodings = database.query("SELECT location_id FROM chunk_encodings WHERE encoding_id=%s AND state='ready'", row['current_encoding_id'])
        return (row, encodings[0]) if encodings else None
    row, encoding = wait(durable, 'upload cache did not commit the durable source')
    path = location(fixture, encoding['location_id'])
    encoded = fixture.bytes(path)
    fixture.bytes(path, bytes([encoded[0] ^ 1]) + encoded[1:])
    scope = dict(storage_id=fixture.store['id'], source='chunk', id=str(row['current_encoding_id']))
    fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
    return row, path, encoded


def blocked(fixture, name, upload, action):
    marker = fixture.arm(name)
    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(upload)
        try:
            wait(lambda: marker.with_suffix('.hit').exists() or future.done(), 'repair did not reach ' + name)
            assert not future.done(), future.exception()
            action()
        finally:
            marker.unlink(missing_ok=True)
        return future


def original_unchanged(fixture, database, original):
    current = database.chunks(fixture)
    row = next(row for row in current if row['id'] == original['id'])
    assert row['current_encoding_id'] == original['current_encoding_id']
    assert fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=chunk')['items']
    assert not database.query("SELECT id FROM audit_events WHERE action='source.recover' AND target=%s", str(original['id']))


def recovery_audit(database, original, source):
    rows = database.query("SELECT source,outcome,detail FROM audit_events WHERE action='source.recover' AND target=%s", str(original['id']))
    assert len(rows) == 1 and rows[0]['source'] == source and rows[0]['outcome'] == 'succeeded', rows
    detail = rows[0]['detail']
    if isinstance(detail, str):
        detail = json.loads(detail)
    assert detail['chunk_id'] == str(original['id'])
    assert detail['encoding_id'] != str(original['current_encoding_id'])
    assert 'access_key_id' in detail if source == 's3' else 'request_audit_id' in detail


def main():
    database = Database()
    for algorithm in ('none', 'aes-256-gcm', 'chacha20-poly1305'):
        fixture = Fixture()
        if algorithm != 'none':
            key = fixture.cli('key', 'create', fixture.name + '-key', 'block', '-a', algorithm)
            fixture.cli('storage', 'key', 'set', fixture.name, key['id'])
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile.update(compression_enabled=True)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        raw = (uuid.uuid4().bytes + bytes(range(256))) * 128
        original, physical, encoded = isolate(fixture, database, raw)
        before = fixture.client.head_object(Bucket=fixture.name, Key='original')
        rejected(lambda: fixture.client.copy_object(Bucket=fixture.name, Key='copy', CopySource={'Bucket':fixture.name,'Key':'original'}))
        old = os.urandom(4096)
        fixture.put('replace', old)

        def corrupt_candidate():
            row = database.candidates(fixture)[-1]
            assert row['state'] == 'uploading' and row['owner_stream'] is not None
            path = location(fixture, row['location_id'])
            data = fixture.bytes(path)
            fixture.bytes(path, bytes([data[0] ^ 1]) + data[1:])
            original_unchanged(fixture, database, original)
        failed = blocked(fixture, 'repair-stored', lambda: fixture.put('replace', raw), corrupt_candidate)
        rejected(lambda: failed.result())
        fixture.read('replace', old)
        original_unchanged(fixture, database, original)
        assert database.candidates(fixture), 'failed write lost its physical journal'

        newer = os.urandom(4096)
        failed = blocked(fixture, 'repair-before-commit', lambda: fixture.put('replace', raw), lambda: fixture.put('replace', newer))
        rejected(lambda: failed.result())
        fixture.read('replace', newer)
        original_unchanged(fixture, database, original)

        def change_profile():
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            profile['compression_level'] = 5 if profile['compression_level'] != 5 else 6
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        failed = blocked(fixture, 'repair-before-commit', lambda: fixture.put('replace', raw), change_profile)
        rejected(lambda: failed.result())
        original_unchanged(fixture, database, original)

        if algorithm == 'none':
            identity = str(original['id'])
            if database.type == 'sqlite':
                database.query("CREATE TRIGGER reject_recovery_audit BEFORE INSERT ON audit_events WHEN NEW.action='source.recover' AND NEW.target='" + identity + "' BEGIN SELECT RAISE(ABORT,'audit unavailable'); END")
                remove = 'DROP TRIGGER reject_recovery_audit'
            else:
                database.query("ALTER TABLE audit_events ADD CONSTRAINT reject_recovery_audit CHECK(action<>'source.recover' OR target<>'" + identity + "')")
                maria = database.type == 'mysql' and 'MariaDB' in database.query('SELECT VERSION() AS version')[0]['version']
                remove = 'ALTER TABLE audit_events DROP ' + ('CONSTRAINT' if database.type == 'postgres' or maria else 'CHECK') + ' reject_recovery_audit'
            try:
                rejected(lambda: fixture.put('replace', raw))
                original_unchanged(fixture, database, original)
            finally:
                database.query(remove)

        fixture.put('repaired', raw)
        restored = next(row for row in database.chunks(fixture) if row['id'] == original['id'])
        assert restored['current_encoding_id'] != original['current_encoding_id']
        assert restored['hash'] == original['hash'] and restored['raw_size'] == original['raw_size']
        fixture.read('original', raw)
        fixture.read('repaired', raw)
        recovery_audit(database, original, 's3')
        assert fixture.client.head_object(Bucket=fixture.name, Key='original')['ETag'] == before['ETag']
        assert fixture.bytes(physical) != encoded, 'repair overwrote the damaged evidence'
        healthy = database.query('SELECT owner_stream,recovery_pending,state FROM chunk_encodings WHERE encoding_id=%s', restored['current_encoding_id'])[0]
        assert healthy['owner_stream'] is None and not healthy['recovery_pending'] and healthy['state'] == 'ready'
        fixture.client.copy_object(Bucket=fixture.name, Key='copy', CopySource={'Bucket':fixture.name,'Key':'original'})
        fixture.read('copy', raw)
        print('PASS', algorithm, 'full backend verification, stale writes/profile rejection, immutable evidence and old-reference recovery', flush=True)

    # A maximum-size repeated block reaches the real UploadPart chunk path before completion.
    fixture = Fixture()
    raw = b'R' * (4 * 1024 * 1024)
    original, _, _ = isolate(fixture, database, raw)
    multipart = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='multipart')['UploadId']
    part = fixture.client.upload_part(Bucket=fixture.name, Key='multipart', UploadId=multipart, PartNumber=1, Body=raw)
    assert database.chunks(fixture)[0]['current_encoding_id'] != original['current_encoding_id']
    fixture.client.complete_multipart_upload(Bucket=fixture.name, Key='multipart', UploadId=multipart,
        MultipartUpload={'Parts':[{'ETag':part['ETag'],'PartNumber':1}]})
    fixture.read('multipart', raw)
    fixture.read('original', raw)
    recovery_audit(database, original, 's3')

    fixture = Fixture()
    original, _, _ = isolate(fixture, database, raw)
    response = fixture.session.post(fixture.web + '/api/v1/buckets/' + fixture.bucket_id + '/uploads', headers=fixture.headers,
        json=dict(client_id=str(uuid.uuid4()),key='web',file_name='web',size=str(len(raw))),timeout=30)
    assert response.status_code == 201, response.text
    upload = response.json()['id']
    checksum = base64.b64encode(hashlib.sha256(raw).digest()).decode()
    part = fixture.session.put(fixture.web + f'/api/v1/uploads/{upload}/parts/1', data=raw,
        headers={**fixture.headers,'X-Content-SHA256':checksum}, timeout=90)
    assert part.status_code == 200, part.text
    fixture.api('/api/v1/uploads/' + upload + '/complete', dict(parts=[part.json()]))
    fixture.read('web', raw)
    fixture.read('original', raw)
    recovery_audit(database, original, 'web')
    print('PASS S3 UploadPart and Web upload recover through the shared chunk boundary', flush=True)

    fixture = Fixture()
    fixture.put('original', raw)
    multipart = fixture.client.create_multipart_upload(Bucket=fixture.name, Key='seeded')['UploadId']
    fixture.client.upload_part(Bucket=fixture.name, Key='seeded', UploadId=multipart,
        PartNumber=1, Body=raw[:1024 * 1024])
    original, _, _ = isolate(fixture, database, raw)
    try:
        rejected(lambda: fixture.client.upload_part(Bucket=fixture.name, Key='seeded', UploadId=multipart,
            PartNumber=2, Body=raw[1024 * 1024:]))
        original_unchanged(fixture, database, original)
    finally:
        fixture.client.abort_multipart_upload(Bucket=fixture.name, Key='seeded', UploadId=multipart)
    fixture.put('full-retry', raw)
    fixture.read('original', raw)
    print('PASS bytes borrowed from an earlier part cannot authorize recovery', flush=True)

    fixture = Fixture()
    original, _, _ = isolate(fixture, database, raw)
    old = b'keep the previous object'
    fixture.put('conditional', old)
    rejected(lambda: fixture.client.put_object(Bucket=fixture.name,Key='conditional',Body=raw,IfNoneMatch='*'))
    fixture.read('conditional', old)
    fixture.read('original', raw)
    assert database.chunks(fixture)[0]['current_encoding_id'] != original['current_encoding_id']
    print('PASS verified chunk recovery does not bypass final object write conditions', flush=True)

    fixture = Fixture()
    original, _, _ = isolate(fixture, database, raw)
    fixture.put('short', raw[:-1])
    original_unchanged(fixture, database, original)
    fixture.put('checksum', old)
    rejected(lambda: fixture.client.put_object(Bucket=fixture.name,Key='checksum',Body=raw,
        ContentMD5=base64.b64encode(bytes(16)).decode()))
    fixture.read('checksum', old)
    if database.type == 'mysql':
        # Check serial recovery after asynchronous failed-upload cleanup releases its locks.
        wait(lambda: not database.query("SELECT id FROM streams WHERE bucket_id=%s AND object_key=%s AND state<>'ready'",
            uuid.UUID(fixture.bucket_id), 'checksum'), 'failed checksum cleanup did not finish')
    fixture.put('retry', raw)
    fixture.read('original', raw)
    print('PASS partial content cannot recover another hash and bad request checksums cannot publish objects', flush=True)


if __name__ == '__main__':
    main()
