"""Exercise both management protocols on an isolated instance with four connection slots."""
import http.client
import os
import socket
import time
from contextlib import ExitStack, closing
from urllib.parse import urlsplit

from tests.http2 import Client


def get(endpoint, path):
    target = urlsplit(endpoint)
    with closing(http.client.HTTPConnection(target.hostname, target.port, timeout=3)) as connection:
        connection.request('GET', path, headers={'Connection': 'close'})
        response = connection.getresponse()
        response.read()
        return response.status


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    web, s3 = os.environ['MOKYU_TEST_WEB'], os.environ['MOKYU_TEST_ENDPOINT']
    target = urlsplit(web)
    assert target.scheme == 'http' and get(web, '/api/v1/info') == 200
    with closing(http.client.HTTPConnection(target.hostname, target.port, timeout=3)) as connection:
        connection.request('POST', '/api/v1/auth/login', b' ' * (16 * 1024) + b'{}',
                           {'Content-Type': 'application/json'})
        response = connection.getresponse()
        response.read()
        assert response.status == 413
    baseline = get(s3, '/')
    with ExitStack() as held:
        sockets = []
        for _ in range(2):
            sock = held.enter_context(socket.create_connection((target.hostname, target.port), timeout=25))
            sock.sendall(b'POST /api/v1/auth/login HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n{')
            sockets.append(sock)
            client = Client(web)
            held.callback(client.socket.close)
            stream = client.start('POST', '/api/v1/auth/login', [('content-type', 'application/json'), ('content-length', '64')], end=False)
            client.connection.send_data(stream, b'{')
            client.flush()
        for sock in sockets:
            response = http.client.HTTPResponse(sock)
            response.begin()
            assert response.status == 408, response.status
            response.read()
        # Prove all four permits return even if an h2c peer ignores GOAWAY.
        deadline = time.monotonic() + 7
        while True:
            try:
                with ExitStack() as probes:
                    for _ in range(3):
                        probe = probes.enter_context(socket.create_connection((target.hostname, target.port), timeout=3))
                        probe.sendall(b'GET /api/v1/info HTTP/1.1\r\nHost: test\r\n\r\n')
                        response = http.client.HTTPResponse(probe)
                        response.begin()
                        assert response.status == 200
                        response.read()
                    assert get(web, '/api/v1/info') == 200 and get(s3, '/') == baseline
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.1)
    print('PASS stalled management bodies release connection slots')


if __name__ == '__main__':
    main()
