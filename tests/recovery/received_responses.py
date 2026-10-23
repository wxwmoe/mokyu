"""Hot-object PUT reuse, failed publication and independent read/write memory ownership."""
import base64
import os
import re
import socket
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import requests
from botocore.exceptions import ClientError

from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    config = Path(os.environ['MOKYU_TEST_CONFIG'])
    text = re.sub(r'^write_memory_size = .*\n', '', config.read_text(), flags=re.M)
    config.write_text(text.replace('[instance]\n', '[instance]\nwrite_memory_size = "8MiB"\n'))
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
    faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
    faults.mkdir(parents=True, exist_ok=True)
    markers = []
    with (faults / 'received-responses.log').open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log,
            env=dict(os.environ, RUST_LOG='mokyu=debug', NO_COLOR='1'))
        try:
            def ready():
                assert process.poll() is None, 'service exited; inspect received-responses.log'
                return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
            wait(ready, 'service did not start', seconds=60)

            def policy(fixture, **values):
                profile = fixture.cli('storage', 'profile', 'show', fixture.name)
                fixture.cli('storage', 'profile', 'set', fixture.name, '-', value={**profile, **values})

            def cache(fixture):
                return fixture.cli('status')['response_cache']

            def read(fixture, expected, **headers):
                reply = requests.get(os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/hot',
                    headers={'Accept-Encoding': 'identity', **headers}, timeout=15)
                assert reply.status_code == 200 and reply.content == expected, reply.status_code

            def put(fixture, body, **values):
                return fixture.client.put_object(Bucket=fixture.name, Key='hot', Body=body, ACL='public-read', **values)

            def rejected(action, status):
                try:
                    action()
                except ClientError as error:
                    assert error.response['ResponseMetadata']['HTTPStatusCode'] == status
                else:
                    raise AssertionError('invalid write succeeded')

            def arm(fixture, name):
                marker = fixture.arm(name)
                markers.append(marker)
                return marker

            for chunked in (False, True):
                fixture = Fixture()
                policy(fixture, cdc_enabled=chunked, pack_range_enabled=False)
                first, second = os.urandom(32768), os.urandom(32768)
                before = cache(fixture)['entries']
                put(fixture, first)
                assert cache(fixture)['entries'] == before, 'write-only content polluted read cache'
                read(fixture, first)
                read(fixture, first)
                assert cache(fixture)['entries'] == before + 1
                put(fixture, second)
                wait(lambda: cache(fixture)['entries'] == before + 2, 'accepted PUT was not reused')
                hits = cache(fixture)['hits']
                read(fixture, second)
                fixture.read('hot', second)
                assert cache(fixture)['hits'] >= hits + 2
                reply = requests.get(os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/hot',
                    headers={'Accept-Encoding': 'identity', 'Range': 'bytes=19-1024'}, timeout=15)
                assert reply.status_code == 206 and reply.content == second[19:1025]
                domain = fixture.name + '.example.test'
                fixture.cli('domain', 'set', domain, fixture.name)
                reply = requests.get(os.environ['MOKYU_TEST_PUBLIC_WEB'] + '/hot',
                    headers={'Host': domain, 'Accept-Encoding': 'identity'}, timeout=15)
                assert reply.status_code == 200 and reply.content == second
                print('PASS received allocation reuse, signed/anonymous/Web and Range: ' + ('chunks' if chunked else 'raw'), flush=True)

                baseline = cache(fixture)
                rejected(lambda: put(fixture, first, ContentMD5=base64.b64encode(bytes(16)).decode()), 400)
                rejected(lambda: put(fixture, first, IfNoneMatch='*'), 412)
                wait(lambda: cache(fixture)['candidate_bytes'] == 0, 'failed writes retained candidates')
                assert cache(fixture)['entries'] == baseline['entries']
                assert cache(fixture)['live_bytes'] == baseline['live_bytes']
                read(fixture, second)

                url = urlsplit(fixture.client.generate_presigned_url('put_object',
                    Params={'Bucket': fixture.name, 'Key': 'hot'}, ExpiresIn=60))
                with socket.create_connection((url.hostname, url.port or 80), timeout=10) as connection:
                    connection.sendall((f'PUT {url.path}?{url.query} HTTP/1.1\r\nHost: {url.netloc}\r\n'
                        'Content-Length: 65536\r\nConnection: close\r\n\r\nx').encode())
                    wait(lambda: cache(fixture)['candidate_bytes'] > 0, 'partial write did not reserve candidate')
                wait(lambda: cache(fixture)['candidate_bytes'] == 0, 'disconnected writer leaked response memory')
                read(fixture, second)

                marker = arm(fixture, 'response-upload-before-admit')
                put(fixture, first)
                wait(marker.with_suffix('.hit').exists, 'optional warming did not pause')
                # Accepted PUT has returned; an uncompleted warm is discarded after its bounded deadline.
                wait(lambda: cache(fixture)['candidate_bytes'] == 0, 'optional warming exceeded its deadline', seconds=8)
                marker.unlink(missing_ok=True)
                assert cache(fixture)['entries'] == baseline['entries']
                read(fixture, first)
                put(fixture, second, CacheControl='no-store')
                wait(lambda: cache(fixture)['candidate_bytes'] == 0, 'no-store candidate did not drain')
                hits = cache(fixture)['hits']
                read(fixture, second)
                assert cache(fixture)['hits'] == hits
                fixture.client.put_object_acl(Bucket=fixture.name, Key='hot', ACL='private')
                reply = requests.get(os.environ['MOKYU_TEST_ENDPOINT'] + '/' + fixture.name + '/hot', timeout=15)
                assert reply.status_code == 403, 'received bytes bypassed current permission'
                fixture.read('hot', second)
                print('PASS checksum/conditional/cancel/deadline/no-store and permission: ' + ('chunks' if chunked else 'raw'), flush=True)

            if os.environ.get('MOKYU_TEST_BACKEND'):
                fixture = Fixture()
                policy(fixture, cdc_enabled=False, pack_range_enabled=False)
                first, second = os.urandom(65536), os.urandom(65536)
                put(fixture, first)
                read(fixture, first)
                read(fixture, first)
                policy(fixture, upload_cache=True, write_durability='volatile')
                marker = arm(fixture, 'raw-upload-before-process')
                before = cache(fixture)['entries']
                put(fixture, second)
                wait(marker.with_suffix('.hit').exists, 'RAM uploader did not pause')
                wait(lambda: cache(fixture)['entries'] == before + 1, 'RAM write was not shared with response cache')
                assert fixture.cli('cache', 'status')['write_memory']['used'] == len(second)
                hits = cache(fixture)['hits']
                read(fixture, second)
                assert cache(fixture)['hits'] > hits
                marker.unlink(missing_ok=True)
                wait(lambda: fixture.cli('cache', 'status')['write_memory']['used'] == 0,
                    'response cache retained write quota after drain')
                assert cache(fixture)['live_bytes'] >= len(second)
                # Drain changes the source revision; the ordinary mapping fence may rebuild this entry.
                read(fixture, second)
                read(fixture, second)
                hits = cache(fixture)['hits']
                read(fixture, second)
                assert cache(fixture)['hits'] > hits
                print('PASS RAM shares received allocation, drains write quota and retains charged read bytes', flush=True)
        finally:
            for marker in markers:
                marker.unlink(missing_ok=True)
            process.terminate()
            try:
                process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


if __name__ == '__main__':
    main()
