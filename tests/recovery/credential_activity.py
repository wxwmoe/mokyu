"""Authenticated S3 reads remain independent of bounded credential activity writes."""
import os
import subprocess
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log = (faults / 'credential-activity.log').open('a')
    process = None
    marker = None

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=log)
        def ready():
            assert process.poll() is None, 'service exited; inspect credential-activity.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not start', seconds=60)

    def stop():
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=30)

    def client(credentials, secret=None):
        return boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                            aws_access_key_id=credentials['access_key'], aws_secret_access_key=secret or credentials['secret_key'],
                            config=Config(signature_version='s3v4', connect_timeout=1, read_timeout=1, retries={'max_attempts': 0},
                                          request_checksum_calculation='when_required'))

    def last_used(credentials):
        return database.query('SELECT last_used_at FROM s3_credentials WHERE access_key_id=%s', credentials['access_key'])[0]['last_used_at']

    def rejected(call):
        try:
            call()
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403, error.response['Error']['Code']
        else:
            raise AssertionError('invalid credential was accepted')

    try:
        start()
        fixture, database = Fixture(), Database()
        data = b'credential activity is not part of authorization'
        fixture.put('activity', data)
        wait(lambda: last_used(fixture.credentials), 'initial activity was not recorded')
        status = lambda: fixture.cli('status')['credential_activity']
        wait(lambda: status()['pending'] == 0, 'initial activity did not settle')

        credentials = fixture.cli('cred', 'create', fixture.name)
        signed = client(credentials)
        marker = fixture.arm('credential-activity-before-write')
        failed = status()['failed']
        assert signed.head_object(Bucket=fixture.name, Key='activity')['ContentLength'] == len(data)
        wait(marker.with_suffix('.hit').exists, 'activity worker did not pause')
        signed.put_object(Bucket=fixture.name, Key='activity-write', Body=data)
        with signed.get_object(Bucket=fixture.name, Key='activity')['Body'] as body:
            assert body.read() == data
        assert last_used(credentials) is None
        wait(lambda: status()['failed'] > failed, 'background activity write was not bounded', seconds=10)
        marker.unlink()
        def retry():
            signed.head_object(Bucket=fixture.name, Key='activity')
            return last_used(credentials)
        wait(retry, 'activity did not recover after a timeout', seconds=10)
        wait(lambda: status()['pending'] == 0, 'activity did not settle')
        before = status()['completed']
        touched = last_used(credentials)
        for _ in range(20):
            signed.head_object(Bucket=fixture.name, Key='activity')
        assert status()['completed'] == before and last_used(credentials) == touched
        print('PASS signed PUT/GET/HEAD finish while activity is paused; timeout, retry and coalescing', flush=True)

        invalid = fixture.cli('cred', 'create', fixture.name)
        entries = status()['entries']
        rejected(lambda: client(invalid, 'incorrect-secret').head_object(Bucket=fixture.name, Key='activity'))
        assert last_used(invalid) is None and status()['entries'] == entries
        fixture.cli('cred', 'disable', invalid['access_key'])
        rejected(lambda: client(invalid).head_object(Bucket=fixture.name, Key='activity'))
        assert last_used(invalid) is None and status()['entries'] == entries
        print('PASS invalid signatures and disabled credentials do not enter the activity queue', flush=True)

        pending = fixture.cli('cred', 'create', fixture.name)
        marker = fixture.arm('credential-activity-before-write')
        assert client(pending).head_object(Bucket=fixture.name, Key='activity')['ContentLength'] == len(data)
        wait(marker.with_suffix('.hit').exists, 'activity worker did not pause before restart')
        stop()
        marker.unlink()
        assert last_used(pending) is None
        start()
        assert last_used(pending) is None
        with client(pending).get_object(Bucket=fixture.name, Key='activity')['Body'] as body:
            assert body.read() == data
        wait(lambda: last_used(pending), 'activity did not resume after restart')
        print('PASS restart can discard optional activity without changing readable objects', flush=True)
    finally:
        if marker:
            marker.unlink(missing_ok=True)
        stop()
        log.close()


if __name__ == '__main__':
    main()
