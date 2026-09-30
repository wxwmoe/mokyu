"""Isolated runner restarts the app at markers; verifies derived cache quota and recovery."""
import json
import os
import struct
import time
import uuid
import zlib
from pathlib import Path

import boto3
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
marker = Path(os.environ['MOKYU_TEST_RESTART_MARKER'])
assert not marker.exists()
config = Path(os.environ['MOKYU_TEST_CONFIG'])
backup = config.with_suffix('.thumbnail-backup')
assert not backup.exists()
original = config.read_text()
assert 'thumbnail_cache_size' not in original
backup.write_text(original)
directory = config.parent.parent / 'data' / 'thumbnails'
permissions = directory.stat().st_mode
base = os.environ['MOKYU_TEST_WEB']
credentials = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}, request_checksum_calculation='when_required'))
client = requests.Session()
reply = client.post(base + '/api/login', headers={'Origin': base}, json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
assert reply.status_code == 200
bucket = next(b for b in client.get(base + '/api/buckets').json() if b['name'] == credentials['bucket'])
path = base + '/api/buckets/' + bucket['id']
prefix = 'thumbnail-recovery-' + uuid.uuid4().hex[:10] + '/'
keys = []


def restart(phase):
    ack = marker.with_suffix('.' + phase)
    marker.write_text(phase)
    print('READY restart: ' + phase, flush=True)
    until = time.monotonic() + 180
    while not ack.exists():
        assert time.monotonic() < until, 'runner restart acknowledgement missing'
        time.sleep(.1)
    while True:
        try:
            if client.get(base + '/api/status').status_code == 200:
                break
        except requests.RequestException:
            pass
        assert time.monotonic() < until
        time.sleep(.1)


def upload(index, width, height):
    def chunk(kind, data):
        return struct.pack('!I', len(data)) + kind + data + struct.pack('!I', zlib.crc32(kind + data))
    png = b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('!IIBBBBB', width, height, 8, 2, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(b''.join(b'\0' + os.urandom(width * 3) for _ in range(height)))) + chunk(b'IEND', b'')
    key = prefix + str(index) + '.png'
    keys.append(key)
    s3.put_object(Bucket=credentials['bucket'], Key=key, Body=png, ContentType='image/png')
    item = client.get(path + '/objects', params={'q': key, 'mode': 'exact'}).json()['objects'][0]
    return {'key': key, 'version': item['id']}


def thumbnail(query):
    reply = client.get(path + '/object/thumbnail', params=query)
    assert reply.status_code == 200, reply.text
    return reply


try:
    queries = [upload(i, 100, 60) for i in range(6)]
    config.write_text(original.replace('[manage]', '[manage]\nthumbnail_cache_size = "64KiB"'))
    restart('bounded')
    for query in queries[:5]:
        thumbnail(query)
        assert (directory / ('v1-' + query['version'])).is_file()
        used = client.get(base + '/api/status').json()['local_bytes'][2]
        actual = sum(f.stat().st_size for f in directory.iterdir() if f.is_file())
        assert used == actual and used <= 65536, (used, actual)
    assert not (directory / ('v1-' + queries[0]['version'])).exists()
    print('PASS thumbnail cache eviction and exact bounded disk accounting', flush=True)
    config.write_text(original.replace('[manage]', '[manage]\nthumbnail_cache_size = "1KiB"'))
    directory.chmod(0o555)
    restart('readonly')
    before = client.get(base + '/api/status').json()['local_bytes'][2]
    assert before > 1024
    thumbnail(queries[-1])
    assert client.get(base + '/api/status').json()['local_bytes'][2] == before
    directory.chmod(permissions)
    small = upload('small', 1, 1)
    thumbnail(small)
    assert (directory / ('v1-' + small['version'])).exists()
    assert client.get(base + '/api/status').json()['local_bytes'][2] <= 1024
    print('PASS startup with undeletable old cache, generated fallback and eviction after permission repair', flush=True)
finally:
    directory.chmod(permissions)
    config.write_text(original)
    restart('restored')
    for key in keys:
        s3.delete_object(Bucket=credentials['bucket'], Key=key)
    backup.unlink()
