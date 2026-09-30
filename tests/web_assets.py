"""Check embedded static delivery without making private API responses cacheable."""
import os
import re
import requests

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
for path in ['/api/unknown', '/assets/missing.js', '/missing.css']:
    reply = requests.get(base + path, headers={'Accept': 'text/html'})
    assert reply.status_code == 404 and reply.json()['code'] == 'NotFound'
for path in ['/favicon.ico', '/assets/mokyu-icon.svg', '/site.webmanifest', '/classic/', '/classic/app.js']:
    assert requests.get(base + path).status_code == 200
assert 'private, no-store' == requests.get(base + '/api/info').headers['cache-control']
print('PASS static assets, ETags, HEAD, SPA deep links, missing resources, classic routes and private API cache policy')
