"""Local raw delegation, metadata-only caching and real proxy handoff contracts.

Optional MOKYU_TEST_PROXY_RUN writes isolated proxy configs and waits for the
orchestrator to mount only the selected raw directory into Nginx and Apache.
"""
import json
import os
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote

import boto3
import requests

from tests.storage.source_quarantine import Fixture, wait


class Gate:
    def __init__(self, port):
        self.pause = threading.Event()
        self.selected = threading.Event()
        self.release = threading.Event()
        self.bytes = 0
        self.requests = 0
        self.paths = []
        gate = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_GET(self):
                gate.paths.append(self.path)
                reply = requests.request(self.command, f'http://127.0.0.1:{port}' + self.path,
                    headers={key: value for key, value in self.headers.items() if key.lower() not in ('connection', 'content-length')},
                    stream=True, timeout=30)
                body = reply.raw.read(decode_content=False)
                gate.bytes += len(body)
                gate.requests += 1
                if gate.pause.is_set() and ('X-Accel-Redirect' in reply.headers or 'X-Sendfile' in reply.headers):
                    gate.selected.set()
                    assert gate.release.wait(60), 'test did not release the proxy handoff'
                self.send_response(reply.status_code)
                for key, value in reply.headers.items():
                    if key.lower() not in ('connection', 'transfer-encoding', 'content-length', 'date', 'server'):
                        self.send_header(key, value)
                self.send_header('Content-Length', reply.headers.get('Content-Length', str(len(body))) if self.command == 'HEAD' else str(len(body)))
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(body)
                reply.close()

            do_HEAD = do_GET
            do_OPTIONS = do_GET

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('0.0.0.0', port + 100), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()


def proxy_configs(folder, host):
    root = Path(__file__).resolve().parents[2]
    document = (root / 'docs/local-nginx-deployment.md').read_text()
    main, delivery = re.findall(r'```nginx\n(.*?)\n```', document, re.S)
    nginx = main.replace('include /etc/nginx/mokyu-file-delivery.conf;', delivery)
    nginx = nginx.replace('mokyu:9000', host + ':9100').replace('mokyu:9001', host + ':9101')
    nginx = nginx.replace('/srv/mokyu/raw/', '/srv/raw/').replace('worker_processes auto;', 'worker_processes 1;')
    blocks = re.findall(r'(?m)^ *# BEGIN control cache\n(.*?)^ *# END control cache$',
        (root / 'nginx.example.conf').read_text(), re.S | re.M)
    shared, assets = [re.sub(r'(?m)^ *# ?', '', block) for block in blocks]
    assets = assets.replace('proxy_cache mokyu_control_assets;',
        'proxy_cache mokyu_control_assets; add_header X-Test-Cache $upstream_cache_status always;')
    control = f'upstream mokyu_control {{ server {host}:9002; }}\nserver {{ listen 8082; {assets} location / {{ proxy_pass http://mokyu_control; }} }}\n'
    nginx = nginx.replace('http {', 'http {\n' + shared + control, 1)
    apache = 'Listen 8080\nListen 8081\nServerName localhost\n'
    snippet, = re.findall(r'```apache\n(.*?)\n```', document, re.S)
    for index in range(2):
        apache += snippet.replace('*:8080', '*:' + str(8080 + index)).replace('mokyu:9000', host + ':' + str(9100 + index)) + '\n'
    (folder / 'nginx.conf').write_text(nginx + '\n')
    (folder / 'apache.conf').write_text(apache)


def control_assets(proxy):
    url = f'http://{proxy}:8082'
    index = requests.get(url + '/', timeout=20)
    assert index.status_code == 200 and 'X-Test-Cache' not in index.headers
    assets = re.findall(r'/assets/[^"\s]+\.(?:js|css)', index.text)
    assert assets, 'test binary did not embed the frontend'
    path = assets[0]
    etags = {}
    for encoding in ['identity', 'gzip', 'br', 'zstd']:
        bodies = []
        for expected in ['MISS', 'HIT']:
            reply = requests.get(url + path, headers={'Accept-Encoding': encoding, 'Cookie': 'example=present'}, stream=True, timeout=20)
            assert reply.status_code == 200 and reply.headers['X-Test-Cache'] == expected, dict(reply.headers)
            assert reply.headers.get('content-encoding', 'identity') == encoding
            assert 'accept-encoding' in reply.headers['vary'].lower()
            bodies.append(reply.raw.read(decode_content=False))
            etags[encoding] = reply.headers['etag']
            reply.close()
        assert bodies[0] == bodies[1]
        reply = requests.get(url + path, headers={'Accept-Encoding': encoding, 'If-None-Match': etags[encoding]}, timeout=20)
        assert reply.status_code == 304 and reply.headers['X-Test-Cache'] == 'HIT', dict(reply.headers)
    assert etags['identity'] == etags['gzip'].removeprefix('W/')
    for path in ['/api/v1/account', '/assets/mokyu-icon.svg', '/missing', '/assets/missing-00000000.js']:
        for _ in range(2):
            reply = requests.get(url + path, timeout=20)
            assert reply.headers.get('X-Test-Cache') != 'HIT', (path, dict(reply.headers))
    print('PASS documented control cache partitions four encodings, preserves 304 and excludes API, HTML, fixed and missing assets', flush=True)


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    backend = os.environ.pop('MOKYU_TEST_BACKEND', None)
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG']]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    log_path = faults / 'file-delivery.log'
    gates = []
    with log_path.open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log,
            env=dict(os.environ, RUST_LOG='mokyu=info,mokyu::status::stats=debug,sqlx::query=debug', NO_COLOR='1'))
        try:
            wait(lambda: subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0,
                'service did not start', seconds=60)
            fixture, other = Fixture(), Fixture()
            base = '/api/v1/storages/' + fixture.name + '/profile'

            def policy(**values):
                profile = fixture.cli('storage', 'profile', 'show', fixture.name)
                return fixture.cli('storage', 'profile', 'set', fixture.name, '-', value={**profile, **values})

            def status():
                return fixture.cli('status')

            def read(key='object', **headers):
                return requests.get(os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/' + key,
                    headers={'Accept-Encoding': 'identity', **headers}, timeout=20)

            def queries(reply):
                request_id = reply.headers['x-request-id']
                def trace():
                    return [line for line in log_path.read_text().splitlines() if request_id in line]
                wait(lambda: any('request finished' in line for line in trace()), 'request trace did not finish')
                return sum('rows_returned=' in line for line in trace())

            def put(key='object', body=None, **metadata):
                return fixture.client.put_object(Bucket=fixture.name, Key=key, Body=payload if body is None else body,
                    ACL='public-read', **metadata)

            policy(cdc_enabled=False, pack_range_enabled=False, unreferenced_grace='1s', deleted_chunk_retention='1s')
            payload = os.urandom(65536)
            put()
            for _ in range(3):
                assert read().content == payload
            other.client.put_object(Bucket=other.name, Key='other', Body=payload, ACL='public-read')
            for _ in range(3):
                other.read('other', payload)
            original = fixture.cli('storage', 'profile', 'show', fixture.name)
            assert original['file_delivery'] == 'application' and original['file_delivery_prefix'] is None
            for prefix in ['/', '//host/', '/a/../b/', '/a/%2e/', '/a\\b/', '/a?b/', '/a\n/', 'relative']:
                reply = fixture.session.put(fixture.web + base, headers=fixture.headers,
                    json={**original, 'file_delivery': 'x_accel_redirect', 'file_delivery_prefix': prefix}, timeout=15)
                assert reply.status_code == 400, (prefix, reply.text)
            saved = policy(file_delivery='x_accel_redirect', file_delivery_prefix='/_mokyu_raw')
            assert saved['file_delivery_prefix'] == '/_mokyu_raw/'
            before = status()['response_cache']['hits']
            assert read(**{'X-Mokyu-File-Delivery': 'x_accel_redirect'}).content == payload, 'inline verification was bypassed'
            assert status()['response_cache']['hits'] == before
            other.read('other', payload)
            assert status()['response_cache']['hits'] == before + 1, 'unrelated storage was evicted'
            policy(read_integrity='off')
            before = status()['backend_read_bytes']
            reply = read(**{'X-Mokyu-File-Delivery': 'x_accel_redirect'})
            assert reply.status_code == 200 and not reply.content
            assert re.fullmatch(r'/_mokyu_raw/[0-9a-f]{2}/[0-9a-f]{32}', reply.headers['X-Accel-Redirect'])
            assert status()['backend_read_bytes'] == before
            assert queries(reply) == 1
            assert read().content == payload, 'direct clients without a proxy capability lost the body'
            policy(public_cache_validation='background', public_cache_refresh_interval='3s', public_cache_max_age='3s',
                read_source_validation='background', source_validation_max_age='3s')
            read(**{'X-Mokyu-File-Delivery': 'x_accel_redirect'})
            assert queries(read(**{'X-Mokyu-File-Delivery': 'x_accel_redirect'})) == 0
            print('PASS prefix validation, inline fallback, one-query cold/zero-query hot metadata and storage-local cache retirement', flush=True)

            if backend:
                os.environ['MOKYU_TEST_BACKEND'] = backend
                remote = Fixture()
                os.environ.pop('MOKYU_TEST_BACKEND')
                profile = remote.cli('storage', 'profile', 'show', remote.name)
                reply = remote.session.put(remote.web + '/api/v1/storages/' + remote.name + '/profile', headers=remote.headers,
                    json={**profile, 'file_delivery': 'x_sendfile', 'file_delivery_prefix': '/srv/raw/'}, timeout=15)
                assert reply.status_code == 400, reply.text

            if run := os.environ.get('MOKYU_TEST_PROXY_RUN'):
                folder = Path(os.environ['MOKYU_TEST_CONFIG']).parent / 'proxy'
                folder.mkdir()
                proxy_configs(folder, run)
                (folder / 'raw-path').write_text(str(fixture.root / 'raw'))
                gates = [Gate(9000), Gate(9001)]
                (folder / 'configured').touch()
                wait(lambda: (folder / 'ready').exists(), 'proxy orchestrator did not start', seconds=120)
                control_assets(run + '-nginx')
                domain = fixture.name + '.delivery.test'
                fixture.cli('domain', 'set', domain, fixture.name)
                fixture.cli('bucket', 'cors', fixture.name, '-', value=[{'origins': ['https://reader.test'],
                    'methods': ['GET', 'HEAD'], 'headers': ['*'], 'expose': ['ETag', 'X-Request-Id'], 'max_age': 60}])
                policy(public_cache_validation='strict')
                put(ContentType='application/octet-stream', ContentDisposition='inline; filename="object.bin"',
                    ContentLanguage='en', CacheControl='public, max-age=120')
                physical = fixture.keys()[0]
                for mode, proxy, prefix in [('x_accel_redirect', run + '-nginx', '/_mokyu_raw/'), ('x_sendfile', run + '-apache', '/srv/raw/')]:
                    policy(file_delivery=mode, file_delivery_prefix=prefix)
                    for port, path, host in [(8080, '/' + fixture.name + '/object', None), (8081, '/object', domain)]:
                        headers = {'Accept-Encoding': 'identity', 'Origin': 'https://reader.test'}
                        if host:
                            headers['Host'] = host
                        url = f'http://{proxy}:{port}' + path
                        expected = requests.get((os.environ['MOKYU_TEST_PUBLIC_WEB'] if host else os.environ['MOKYU_TEST_ENDPOINT']) + path, headers=headers, timeout=20)
                        before = gates[port - 8080].bytes
                        actual = requests.get(url, headers=headers, timeout=20)
                        assert actual.status_code == expected.status_code == 200 and actual.content == expected.content == payload, (mode, actual.status_code, actual.text[:200])
                        assert gates[port - 8080].bytes == before, 'proxy received the object body from Mokyu'
                        assert {value.strip().lower() for value in actual.headers['vary'].split(',')} == {value.strip().lower() for value in expected.headers['vary'].split(',')}
                        for name in ['etag', 'last-modified', 'content-type', 'content-length', 'content-language', 'content-disposition', 'cache-control', 'accept-ranges', 'x-content-type-options', 'access-control-allow-origin', 'access-control-expose-headers']:
                            assert actual.headers.get(name) == expected.headers.get(name), (mode, name, dict(actual.headers), dict(expected.headers))
                        assert actual.headers.get('x-request-id') and 'x-accel-redirect' not in actual.headers and 'x-sendfile' not in actual.headers
                        head = requests.head(url, headers=headers, timeout=20)
                        assert not head.content and head.headers['etag'] == expected.headers['etag']
                        ranged = requests.get(url, headers={**headers, 'Range': 'bytes=2-127'}, timeout=20)
                        assert ranged.status_code == 206 and ranged.content == payload[2:128]
                        assert requests.get(url, headers={**headers, 'If-None-Match': expected.headers['etag']}, timeout=20).status_code == 304
                        assert requests.get(url, headers={**headers, 'If-Match': '"wrong"'}, timeout=20).status_code == 412
                        wrong = requests.get(url, headers={**headers, 'Origin': 'https://wrong.test'}, timeout=20)
                        assert 'access-control-allow-origin' not in wrong.headers
                        options = requests.options(url, headers={**headers, 'Access-Control-Request-Method': 'GET'}, timeout=20)
                        assert options.status_code in (200, 204) and options.headers['access-control-allow-origin'] == 'https://reader.test'
                    relative = Path(physical).relative_to(fixture.root / 'raw').as_posix()
                    assert requests.get(f'http://{proxy}:8080/_mokyu_raw/' + relative, timeout=20).status_code >= 400
                    client = boto3.client('s3', endpoint_url=f'http://{proxy}:8080', region_name='us-east-1',
                        aws_access_key_id=fixture.credentials['access_key'], aws_secret_access_key=fixture.credentials['secret_key'], config=fixture.client.meta.config)
                    assert client.get_object(Bucket=fixture.name, Key='object')['Body'].read() == payload
                    put('metadata', Metadata={'custom': 'preserved'})
                    actual = requests.get(f'http://{proxy}:8080/{fixture.name}/metadata', headers={'Accept-Encoding': 'identity'}, timeout=20)
                    assert actual.content == payload and actual.headers['x-amz-meta-custom'] == 'preserved'
                    text = b'compressible response\n' * 8192
                    put('text', body=text, ContentType='text/plain')
                    actual = requests.get(f'http://{proxy}:8080/{fixture.name}/text', headers={'Accept-Encoding': 'gzip'}, timeout=20)
                    assert actual.content == text and actual.headers['content-encoding'] == 'gzip'
                    fixture.client.put_object_acl(Bucket=fixture.name, Key='text', ACL='private')
                    assert requests.get(f'http://{proxy}:8080/{fixture.name}/text', timeout=20).status_code == 403
                    fixture.client.delete_object(Bucket=fixture.name, Key='text')
                    assert requests.get(f'http://{proxy}:8080/{fixture.name}/text', timeout=20).status_code == 404
                    print('PASS ' + mode + ' real public S3/Web bytes, metadata, CORS, HEAD, Range, conditions and signed fallback', flush=True)

                    paths = set(fixture.keys())
                    race = 'race a+%/child'
                    put(race, body=b'old body', CacheControl='public, max-age=1', ContentDisposition='inline; filename="old.bin"')
                    old_file, = set(fixture.keys()) - paths
                    gate = gates[0]
                    gate.selected.clear()
                    gate.release.clear()
                    gate.pause.set()
                    with ThreadPoolExecutor() as executor:
                        pending = executor.submit(requests.get, f'http://{proxy}:8080/{fixture.name}/' + quote(race, safe='/'),
                            headers={'Accept-Encoding': 'identity', 'Origin': 'https://reader.test'}, timeout=60)
                        assert gate.selected.wait(20), 'proxy handoff was not selected'
                        put(race, body=b'new body', CacheControl='public, max-age=300', ContentDisposition='attachment; filename="new.bin"')
                        def collected():
                            fixture.cli('gc', 'run')
                            return not Path(old_file).exists()
                        wait(collected, 'GC could not retire the unopened old file', seconds=30)
                        gate.pause.clear()
                        gate.release.set()
                        result = pending.result(timeout=30)
                    assert result.status_code == 200 and result.content == b'new body', (mode, result.status_code, result.text, gate.paths[-5:])
                    assert result.headers['cache-control'] == 'public, max-age=300', (mode, dict(result.headers))
                    assert result.headers['content-disposition'] == 'attachment; filename="new.bin"', (mode, dict(result.headers))
                    assert result.headers['access-control-allow-origin'] == 'https://reader.test'
                    print('PASS ' + mode + ' GC before proxy open retries the original request without stale headers or a loop', flush=True)
            policy(public_cache_validation='strict', file_delivery='x_accel_redirect', file_delivery_prefix='/_mokyu_raw/')
            paths = set(fixture.keys())
            put('invalid', body=b'file type and size')
            path, = set(fixture.keys()) - paths
            path = Path(path)
            original_bytes = path.read_bytes()
            path.write_bytes(b'short')
            assert read('invalid', **{'X-Mokyu-File-Delivery': 'x_accel_redirect'}).status_code >= 500
            path.write_bytes(original_bytes)
            saved_path = path.with_suffix('.test-original')
            path.rename(saved_path)
            try:
                path.symlink_to(saved_path)
                assert read('invalid', **{'X-Mokyu-File-Delivery': 'x_accel_redirect'}).status_code >= 500
            finally:
                path.unlink()
                saved_path.rename(path)
            print('PASS file size and symlink rejection before proxy handoff', flush=True)
            policy(file_delivery='application', file_delivery_prefix=None, read_integrity='inline')
            for _ in range(3):
                assert read().content == payload
            before = status()['response_cache']['hits']
            assert read().content == payload and status()['response_cache']['hits'] == before + 1
            print('PASS application restoration admits verified body caching again', flush=True)
        finally:
            for gate in gates:
                gate.close()
            process.terminate()
            process.wait(timeout=60)


if __name__ == '__main__':
    main()
