"""Check embedded static delivery without making private API responses cacheable."""
import os
import re
import requests
import gzip
import brotli
import zstandard

base = os.environ['MOKYU_TEST_WEB']
home = requests.get(base + '/')
home.raise_for_status()
assert '<title>Mokyu</title>' in home.text
assert 'no-cache' in home.headers['cache-control']
assert "object-src 'none'" in home.headers['content-security-policy']
assert 'unsafe-eval' not in home.headers['content-security-policy']
assert requests.get(base + '/', headers={'If-None-Match': home.headers['etag']}).status_code == 304
assert not requests.head(base + '/').content
assets = re.findall(r'(?:src|href)="(/assets/[^"?]+\.(?:js|css))"', home.text)
assert assets
for path in assets:
    reply = requests.get(base + path)
    reply.raise_for_status()
    assert 'immutable' in reply.headers['cache-control']
    assert requests.get(base + path, headers={'If-None-Match': reply.headers['etag']}).status_code == 304
assert requests.get(base + '/media', headers={'Accept': 'text/html'}).text == home.text
for path in ['/api/unknown', '/api/v1/unknown', '/assets/missing.js', '/missing.css', '/classic/app.js']:
    reply = requests.get(base + path, headers={'Accept': 'text/html'})
    assert reply.status_code == 404 and reply.json()['code'] == 'NotFound'
for path in ['/favicon.ico', '/assets/mokyu-icon.svg', '/site.webmanifest', '/THIRD_PARTY_NOTICES.txt']:
    assert requests.get(base + path).status_code == 200
assert 'private, no-store' == requests.get(base + '/api/v1/info').headers['cache-control']
print('PASS static assets, ETags, HEAD, SPA deep links, missing resources, license notices and private API cache policy')

def wire(path, encoding, **headers):
    reply = requests.get(base + path, headers={'Accept-Encoding': encoding, **headers}, stream=True)
    data = reply.raw.read(decode_content=False)
    reply.close()
    return reply, data

for path in [assets[0], '/api/v1/openapi.json']:
    original, raw = wire(path, 'identity')
    assert original.status_code == 200 and len(raw) >= 1024
    for encoding, decode in [('zstd', lambda value: zstandard.ZstdDecompressor().stream_reader(value).read()),
                             ('br', brotli.decompress), ('gzip', gzip.decompress)]:
        reply, encoded = wire(path, encoding)
        assert reply.status_code == 200 and reply.headers['content-encoding'] == encoding
        assert decode(encoded) == raw and len(encoded) < len(raw)
        assert 'accept-encoding' in reply.headers['vary'].lower()
        if 'content-length' in reply.headers:
            assert int(reply.headers['content-length']) == len(encoded)
        head = requests.head(base + path, headers={'Accept-Encoding': encoding})
        assert head.status_code == 200 and head.headers.get('content-encoding') == encoding and not head.content, (path, encoding, head.status_code, dict(head.headers))
        if path == '/api/v1/openapi.json':
            assert 'content-length' not in head.headers
        if path != '/api/v1/openapi.json':
            assert reply.headers['etag'] == 'W/' + original.headers['etag']
            cached, data = wire(path, encoding, **{'If-None-Match': original.headers['etag']})
            assert cached.status_code == 304 and not data and cached.headers['etag'] == reply.headers['etag']
            cached, data = wire(path, 'identity', **{'If-None-Match': reply.headers['etag']})
            assert cached.status_code == 304 and not data and cached.headers['etag'] == original.headers['etag']
            matched, data = wire(path, encoding, **{'If-Match': original.headers['etag']})
            assert matched.status_code == 200 and 'content-encoding' not in matched.headers and data == raw
            failed, _ = wire(path, encoding + ', identity;q=0', **{'If-Match': original.headers['etag']})
            assert failed.status_code == 412
    preferred, _ = wire(path, 'gzip, deflate, br, zstd')
    assert preferred.headers['content-encoding'] == 'zstd'
    weighted, _ = wire(path, 'zstd;q=0.1, br;q=0.8, gzip;q=1')
    assert weighted.headers['content-encoding'] == 'gzip'
    plain, body = wire(path, 'zstd', **{'Cache-Control': 'no-transform'})
    assert 'content-encoding' not in plain.headers and body == raw
    refused, _ = wire(path, '*;q=0')
    assert refused.status_code == 406
    for suffix in ['.gz', '.br', '.zst']:
        assert requests.get(base + path + suffix).status_code == 404

for path in ['/api/v1/info', '/api/v1/auth/session', '/api/v1/unknown']:
    reply, _ = wire(path, 'gzip, br, zstd')
    assert 'content-encoding' not in reply.headers
    assert reply.headers['cache-control'] == 'private, no-store'
if os.environ.get('MOKYU_TEST_PASSWORD'):
    with requests.Session() as session:
        headers = {'Origin': base, 'Accept-Encoding': 'zstd, br, gzip'}
        reply = session.post(base + '/api/v1/auth/login', headers=headers,
                             json={'username': 'tester', 'password': os.environ['MOKYU_TEST_PASSWORD']})
        reply.raise_for_status()
        assert 'refresh_token' in reply.json() and 'content-encoding' not in reply.headers
        headers['X-CSRF-Token'] = reply.json()['csrf_token']
        try:
            reply = session.get(base + '/api/v1/auth/session', headers=headers)
            reply.raise_for_status()
            assert 'content-encoding' not in reply.headers and reply.headers['cache-control'] == 'private, no-store'
        finally:
            session.post(base + '/api/v1/auth/logout', headers=headers).raise_for_status()
print('PASS static and schema codecs, q weights, validators, HEAD, no-transform, exclusions and sensitive-route policy')
