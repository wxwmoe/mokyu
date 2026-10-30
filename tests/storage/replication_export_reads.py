"""Verify bounded native raw export reads without a read cache."""
import json
import os
from pathlib import Path
import random
import uuid

from tests.storage.source_quarantine import Fixture, wait


def run():
    fixture = Fixture()
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(cdc_enabled=False, deduplication_enabled=False, cache_enabled=False,
        upload_cache=False, compression_enabled=False, pack_enabled=False, pack_range_enabled=False)
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    backend = json.loads(Path(os.environ['MOKYU_TEST_BACKEND']).read_text())
    resource = fixture.api('/api/v1/external-buckets', dict(
        project_id=fixture.api('/api/v1/projects')[0]['id'], name='reads-' + uuid.uuid4().hex[:12],
        connection=dict(endpoint_url=backend['endpoint'], region=backend['region'],
            bucket=fixture.remote_bucket, prefix=fixture.name + '/export', allow_http=True),
        access_key=backend['access_key'], secret_key=backend['secret_key']), expected=201)
    result = []
    for size in (16, 64):
        body = random.Random(917).randbytes(size * 1024 * 1024)
        key = 'raw-' + str(size)
        fixture.client.put_object(Bucket=fixture.name, Key=key, Body=body)
        before = fixture.cli('status')
        task = fixture.api('/api/v1/replications', dict(source_bucket=fixture.bucket_id,
            target_bucket=resource['id'], source_external=False, target_external=True,
            operation='copy', scope=dict(selection='objects', keys=[key]),
            conflict='report', permissions='ignore'), expected=202)['task_id']
        row = wait(lambda: (row if (row := fixture.api('/api/v1/replications/' + task))['state']
            in ('completed', 'failed') else None), 'export did not settle', seconds=240)
        assert row['state'] == 'completed', row
        after = fixture.cli('status')
        with fixture.backend.get_object(Bucket=fixture.remote_bucket,
                Key=fixture.name + '/export/' + key)['Body'] as stream:
            assert stream.read() == body
        measurement = dict(size=len(body), reads=after['backend_gets'] - before['backend_gets'],
            bytes=after['backend_read_bytes'] - before['backend_read_bytes'])
        assert measurement['bytes'] == 2 * len(body), measurement
        result.append(measurement)
    return result


if __name__ == '__main__':
    print(json.dumps(run()))
