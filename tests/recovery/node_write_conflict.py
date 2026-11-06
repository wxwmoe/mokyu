"""A superseded data-node PUT reports an object conflict and preserves the winner."""
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from tests.recovery.source_repair import blocked
from tests.storage.source_quarantine import Fixture


def main():
    for chunks, barrier in [(True, 'node-write-seal'), (False, 'node-raw-uploaded')]:
        home = Fixture()
        home.faults = Path(os.environ['MOKYU_TEST_DATA_FAULT_DIR'])
        peer = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_SECOND_DATA'], region_name='us-east-1',
                            aws_access_key_id=home.credentials['access_key'], aws_secret_access_key=home.credentials['secret_key'],
                            config=home.client.meta.config)
        profile = home.cli('storage', 'profile', 'show', home.name)
        home.cli('storage', 'profile', 'set', home.name, '-', value={**profile, 'cdc_enabled': chunks, 'pack_range_enabled': False})
        losing, winning = os.urandom(65536), os.urandom(32768)
        first = blocked(home, barrier, lambda: home.put('same-key', losing),
                        lambda: peer.put_object(Bucket=home.name, Key='same-key', Body=winning))
        try:
            first.result()
        except ClientError as error:
            assert (error.response['ResponseMetadata']['HTTPStatusCode'], error.response['Error']['Code']) == (409, 'OperationAborted'), error
        else:
            raise AssertionError('superseded PUT reported success')
        home.read('same-key', winning)
        home.put('followup', losing)
        home.read('followup', losing)
    print('PASS superseded chunk and raw PUTs return 409, preserve the winner and keep the node writable', flush=True)


if __name__ == '__main__':
    main()
