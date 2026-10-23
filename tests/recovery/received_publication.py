"""Ordinary PUT preserves metadata, conditions and late authorization changes."""
import base64
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    database = Database()
    for chunked in (False, True):
        fixture = Fixture()
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=chunked, pack_range_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
        previous = b'previous acknowledged value'
        fixture.put('object', previous)
        key = 'object' if database.type == 'postgres' else b'object'
        for accepted in (False, True):
            body = b'verified replacement\n' * 2048
            checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
            marker = fixture.arm('source-before-publish')
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(fixture.client.put_object, Bucket=fixture.name,
                    Key='object', Body=body, ChecksumSHA256=checksum,
                    ContentType='text/plain', Metadata={'verified': 'yes'},
                    **({} if accepted else {'IfMatch': '"never-matched"'}))
                try:
                    wait(lambda: marker.with_suffix('.hit').exists() or future.done(),
                         'PUT did not reach the publication boundary')
                    assert not future.done(), future.exception()
                    fixture.read('object', previous)
                    pending = database.query("SELECT s.id,s.size FROM streams s JOIN buckets b ON b.id=s.bucket_id "
                        "WHERE b.name=%s AND s.object_key=%s AND s.state='writing' ORDER BY s.created_at DESC",
                        fixture.name, key)[0]
                finally:
                    marker.unlink(missing_ok=True)
                if accepted:
                    reply = future.result(timeout=30)
                    assert reply['ChecksumSHA256'] == checksum
                    assert reply['ETag'].strip('"') == hashlib.md5(body).hexdigest()
                    previous = body
                else:
                    try:
                        future.result(timeout=30)
                    except ClientError as error:
                        assert error.response['Error']['Code'] == 'PreconditionFailed'
                    else:
                        raise AssertionError('conditional PUT unexpectedly succeeded')
                    row = database.query('SELECT size,state FROM streams WHERE id=%s', pending['id'])[0]
                    assert row['size'] == pending['size'] and row['state'] != 'ready', row
            fixture.read('object', previous)
        head = fixture.client.head_object(Bucket=fixture.name, Key='object', ChecksumMode='ENABLED')
        assert head['ContentLength'] == len(previous) and head['ChecksumSHA256'] == checksum
        assert head['ContentType'] == 'text/plain' and head['Metadata'] == {'verified': 'yes'}
        fixture.put('empty', b'')
        fixture.read('empty', b'')
        marker = fixture.arm('source-before-publish')
        access = fixture.credentials['access_key']
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fixture.put, 'object', b'revoked replacement')
            try:
                wait(lambda: marker.with_suffix('.hit').exists() or future.done(),
                     'PUT did not reach the authorization boundary')
                assert not future.done(), future.exception()
                fixture.cli('cred', 'grant', access, fixture.name, '--read-only')
                marker.unlink(missing_ok=True)
                try:
                    future.result(timeout=30)
                except ClientError as error:
                    assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403
                else:
                    raise AssertionError('revoked writer published received content')
                fixture.read('object', previous)
            finally:
                marker.unlink(missing_ok=True)
                fixture.cli('cred', 'grant', access, fixture.name)
        print(json.dumps(dict(layout='chunks' if chunked else 'raw',
            result='PASS metadata, failed conditions, checksum, empty PUT, old-version reads and revoked writer')), flush=True)


if __name__ == '__main__':
    main()
