"""Version-bound previews, decoder limits, local cache failures and live scoped access."""
import base64
import json
import os
import struct
import subprocess
import uuid
import zlib
from pathlib import Path

import boto3
import requests
from botocore.config import Config

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']
name = 'previews-' + uuid.uuid4().hex[:10]


def cli(*args):
    return json.loads(subprocess.check_output(command + list(args), text=True))


def login(username, password):
    client = requests.Session()
    reply = client.post(url + '/api/login', headers={'Origin': url}, json={'username': username, 'password': password})
    assert reply.status_code == 200, reply.text
    client.headers.update({'Origin': url, 'X-CSRF-Token': reply.json()['csrf_token']})
    return client


def png(width, height, rows=b''):
    def chunk(kind, data):
        return struct.pack('!I', len(data)) + kind + data + struct.pack('!I', zlib.crc32(kind + data))
    return b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('!IIBBBBB', width, height, 8, 2, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(rows)) + chunk(b'IEND', b'')


admin = login('tester', os.environ['MOKYU_TEST_PASSWORD'])
bucket = cli('bucket', 'create', name)
credential = cli('credential', 'create', name)
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}, retries={'max_attempts': 0}, request_checksum_calculation='when_required'))
base = url + '/api/buckets/' + bucket['id']
files = []
user = None
cache_root = Path(os.environ['MOKYU_TEST_CONFIG']).parent.parent / 'data' / 'thumbnails'
mode = cache_root.stat().st_mode if cache_root.exists() else None


def put(key, data, mime, **options):
    s3.put_object(Bucket=name, Key=key, Body=data, ContentType=mime, **options)
    if key not in files:
        files.append(key)
    reply = admin.get(base + '/objects', params={'q': key, 'mode': 'exact'})
    assert reply.status_code == 200, reply.text
    return {'key': key, 'version': reply.json()['objects'][0]['id']}


try:
    source = png(800, 400, (b'\0' + bytes([247, 191, 213]) * 800) * 400)
    q = put('odd/../heart +%#.png', source, 'image/png')
    reply = admin.get(base + '/object', params=q)
    assert reply.status_code == 200 and reply.json()['preview'] == 'image', reply.text
    thumb = admin.get(base + '/object/thumbnail', params=q, timeout=30)
    assert thumb.status_code == 200, thumb.text
    assert thumb.headers['content-type'] == 'image/png' and 'no-store' in thumb.headers['cache-control']
    assert struct.unpack('!II', thumb.content[16:24]) == (384, 192)
    path = cache_root / ('v1-' + q['version'])
    assert path.is_file(), cache_root
    before = admin.get(url + '/api/status').json()['local_bytes'][2]
    assert before >= len(thumb.content) + 32
    assert admin.get(base + '/object/thumbnail', params=q).content == thumb.content
    assert admin.get(url + '/api/status').json()['local_bytes'][2] == before
    path.write_bytes(b'damaged preview')
    cache_root.chmod(0o555)
    fallback = admin.get(base + '/object/thumbnail', params=q)
    assert fallback.status_code == 200 and fallback.content == thumb.content, fallback.text
    assert admin.get(url + '/api/status').json()['local_bytes'][2] == before
    cache_root.chmod(mode or 0o755)
    assert admin.get(base + '/object/thumbnail', params=q).content == thumb.content
    assert len(path.read_bytes()) == len(thumb.content) + 32
    print('PASS bounded thumbnail, version cache, corruption fallback and retained failed-delete charge', flush=True)

    content = admin.get(base + '/object/content', params={**q, 'preview': 'true'}, headers={'Range': 'bytes=1-9'})
    assert content.status_code == 206 and content.content == source[1:10]
    assert content.headers['content-type'] == 'image/png' and content.headers['content-disposition'] == 'inline'
    assert 'no-store' in content.headers['cache-control'] and 'sandbox' in content.headers['content-security-policy']
    same = put(q['key'], source, 'image/png')
    assert same['version'] != q['version']
    for route in ['/object', '/object/thumbnail', '/object/content', '/object/text']:
        assert admin.get(base + route, params=q).status_code == 412, route
    assert requests.get(base + '/object/thumbnail', params=same).status_code == 403
    print('PASS exact-version guards even identical bytes, Range streaming and private response headers', flush=True)

    for key, data, mime in [('bad.png', b'bad', 'image/png'), ('large.png', png(16385, 16385), 'image/png'), ('large-pixels.png', png(8000, 8000), 'image/png'),
                            ('attack.svg', b'<svg xmlns="http://www.w3.org/2000/svg" onload="alert(1)"/>', 'image/svg+xml'),
                            ('fake.png', b'<svg onload="alert(1)"/>', 'image/png')]:
        query = put(key, data, mime)
        reply = admin.get(base + '/object/thumbnail', params=query, timeout=30)
        assert reply.status_code == 415, (key, reply.status_code, reply.text)
        if key.endswith('.svg'):
            assert admin.get(base + '/object', params=query).json()['preview'] == 'none'
            reply = admin.get(base + '/object/content', params={**query, 'preview': 'true'})
            assert reply.headers['content-type'] == 'application/octet-stream' and reply.headers['content-disposition'] == 'attachment'
    encoded = put('encoded.png', source, 'image/png', ContentEncoding='gzip')
    assert admin.get(base + '/object', params=encoded).json()['preview'] == 'none'
    assert admin.get(base + '/object/thumbnail', params=encoded).status_code == 415
    query = put('notes.txt', b'<script>document.body.remove()</script>\n' + b'A' * 70000, 'text/plain')
    reply = admin.get(base + '/object/text', params=query)
    assert reply.status_code == 200 and reply.json()['truncated'] and len(reply.json()['text'].encode()) == 65536
    assert 'no-store' in reply.headers['cache-control']
    print('PASS malformed, dimension/pixel bomb, SVG/spoof and encoded-body rejection; bounded plain text', flush=True)

    password = 'Preview-test-' + uuid.uuid4().hex
    reply = admin.post(url + '/api/users', json={'username': name, 'password': password, 'role': 'member', 'must_change_password': False})
    assert reply.status_code == 201, reply.text
    user = reply.json()['id']
    membership = {'role': 'reader', 'scope': 'selected', 'grants': [{'bucket_id': bucket['id'], 'actions': ['bucket.list']}]}
    member_url = url + '/api/projects/' + next(b['project_id'] for b in admin.get(url + '/api/buckets').json() if b['id'] == bucket['id']) + '/members/' + user
    assert admin.put(member_url, json=membership).status_code == 204
    member = login(name, password)
    assert member.get(base + '/object', params=same).status_code == 200
    assert member.get(base + '/object/thumbnail', params=same).status_code == 403
    membership['grants'][0]['actions'].append('object.read')
    assert admin.put(member_url, json=membership).status_code == 204
    assert member.get(base + '/object/thumbnail', params=same).status_code == 200
    membership['grants'][0]['actions'] = ['bucket.list']
    assert admin.put(member_url, json=membership).status_code == 204
    assert member.get(base + '/object/thumbnail', params=same).status_code == 403
    assert member.get(base + '/object/content', params=same).status_code == 403
    print('PASS listing-only detail, private cached thumbnail authorization and immediate read revocation', flush=True)
finally:
    if mode is not None:
        cache_root.chmod(mode)
    for key in files:
        s3.delete_object(Bucket=name, Key=key)
    if user:
        admin.delete(url + '/api/projects/' + next(b['project_id'] for b in admin.get(url + '/api/buckets').json() if b['id'] == bucket['id']) + '/members/' + user)
        admin.delete(url + '/api/users/' + user)
