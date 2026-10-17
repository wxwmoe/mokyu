"""Default boto3 streams/multipart and competing publication; isolated fixture only."""
import hashlib
import io
import json
import os
import random
import tempfile
import threading
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import botocore
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credentials = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credentials['bucket']
nodes = json.loads(os.environ.get('MOKYU_TEST_ENDPOINTS', 'null')) or [os.environ['MOKYU_TEST_ENDPOINT']]
assert 1 <= len(nodes) <= 8
clients = [boto3.client('s3', endpoint_url=node, region_name='us-east-1',
                        aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                                      retries={'max_attempts': 0}, connect_timeout=5, read_timeout=30)) for node in nodes]
rounds = int(os.environ.get('MOKYU_TEST_ROUNDS', '20'))
writers = int(os.environ.get('MOKYU_TEST_WRITERS', '8'))
assert 1 <= rounds <= 100 and 2 <= writers <= 16
prefix = 'publication-' + uuid.uuid4().hex + '/'
keys = []

def remember(suffix):
    key = prefix + suffix
    keys.append(key)
    return key

try:
    raw = random.Random(719).randbytes(11 * 1024 * 1024 + 17)
    stream = remember('stream')
    clients[0].put_object(Bucket=bucket, Key=stream, Body=io.BytesIO(raw), Metadata={'source': 'file-stream'})
    multipart = remember('multipart')
    with tempfile.NamedTemporaryFile() as file:
        file.write(raw)
        file.flush()
        clients[0].upload_file(file.name, bucket, multipart, ExtraArgs={'Metadata': {'source': 'transfer-manager'}})
    for client in clients:
        for key, source in [(stream, 'file-stream'), (multipart, 'transfer-manager')]:
            response = client.get_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
            with response['Body'] as body:
                assert body.read() == raw
            assert response['Metadata']['source'] == source and response['ContentLength'] == len(raw)
    print(f'PASS default boto3 {boto3.__version__} / botocore {botocore.__version__}: file stream and transfer-manager multipart', flush=True)
    del raw

    for conditional in [False, True]:
        codes = Counter()
        for round_number in range(rounds):
            key = remember(f'{conditional}-{round_number}')
            values = [random.Random(719 + round_number * writers + i).randbytes(4096 + i * 173) for i in range(writers)]
            barrier = threading.Barrier(writers, timeout=20)
            def write(i):
                barrier.wait()
                try:
                    response = clients[i % len(clients)].put_object(
                        Bucket=bucket, Key=key, Body=values[i], ACL='public-read',
                        Metadata={'writer': str(i)}, ContentType=f'application/x-writer-{i}',
                        **({'IfNoneMatch': '*'} if conditional else {}))
                    assert response['ETag'] == '"' + hashlib.md5(values[i]).hexdigest() + '"'
                    return i, 200, 'OK'
                except ClientError as error:
                    return i, error.response['ResponseMetadata']['HTTPStatusCode'], error.response['Error']['Code']
            with ThreadPoolExecutor(max_workers=writers) as pool:
                replies = list(pool.map(write, range(writers)))
            for _, status, code in replies:
                codes[f'{status}:{code}'] += 1
                assert (status, code) in [(200, 'OK'), (409, 'OperationAborted'), (503, 'SlowDown')] or (
                    conditional and (status, code) == (412, 'PreconditionFailed')), replies
            winners = {i for i, status, _ in replies if status == 200}
            assert winners and (not conditional or len(winners) == 1), replies
            for client, node in zip(clients, nodes):
                response = client.get_object(Bucket=bucket, Key=key)
                with response['Body'] as body:
                    received = body.read()
                writer = int(response['Metadata']['writer'])
                assert writer in winners and received == values[writer]
                assert response['ContentLength'] == len(received)
                assert response['ContentType'] == f'application/x-writer-{writer}'
                assert response['ETag'] == '"' + hashlib.md5(received).hexdigest() + '"'
                head = client.head_object(Bucket=bucket, Key=key)
                assert all(head[field] == response[field] for field in ['ETag', 'ContentLength', 'ContentType', 'Metadata'])
                public = requests.get(f'{node}/{bucket}/{key}', headers={'Accept-Encoding': 'identity'}, timeout=20)
                assert public.status_code == 200 and public.content == received
                assert public.headers['etag'] == response['ETag']
                assert public.headers['content-type'] == response['ContentType']
                assert public.headers['x-amz-meta-writer'] == str(writer)
        print(json.dumps({'test': 'conditional' if conditional else 'ordinary', 'rounds': rounds,
                          'writers': writers, 'nodes': len(nodes), 'responses': codes}), flush=True)
finally:
    for offset in range(0, len(keys), 1000):
        response = clients[0].delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key} for key in keys[offset:offset + 1000]]})
        assert not response.get('Errors'), response.get('Errors')
    for client in clients:
        client.close()
