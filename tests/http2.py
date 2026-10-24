"""Small h2c wire client for isolated listener and proxy regressions."""
import socket
from urllib.parse import urlsplit

import h2.config
import h2.connection
import h2.events


class Client:
    def __init__(self, endpoint, authority=None):
        target = urlsplit(endpoint)
        assert target.scheme == 'http' and target.path in ('', '/')
        self.authority = authority or target.netloc
        self.socket = socket.create_connection((target.hostname, target.port or 80), timeout=20)
        self.connection = h2.connection.H2Connection(config=h2.config.H2Configuration(
            client_side=True, header_encoding='utf-8', validate_outbound_headers=False))
        self.responses = {}
        self.connection.initiate_connection()
        self.flush()

    def flush(self):
        data = self.connection.data_to_send()
        if data:
            self.socket.sendall(data)

    def receive(self):
        data = self.socket.recv(65536)
        assert data, 'HTTP/2 connection closed with responses outstanding'
        for event in self.connection.receive_data(data):
            if isinstance(event, h2.events.ResponseReceived):
                self.responses[event.stream_id]['headers'] = dict(event.headers)
            elif isinstance(event, h2.events.DataReceived):
                self.responses[event.stream_id]['body'].extend(event.data)
                self.connection.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
            elif isinstance(event, h2.events.TrailersReceived):
                self.responses[event.stream_id]['trailers'] = dict(event.headers)
            elif isinstance(event, h2.events.StreamEnded):
                self.responses[event.stream_id]['ended'] = True
            elif isinstance(event, h2.events.StreamReset):
                # Servers can stop an unread request after finishing its response.
                if event.error_code != 0 or not self.responses[event.stream_id]['ended']:
                    self.responses[event.stream_id]['reset'] = event.error_code
                self.responses[event.stream_id]['ended'] = True
            elif isinstance(event, h2.events.ConnectionTerminated):
                raise AssertionError(('unexpected GOAWAY', event.error_code))
        self.flush()

    def start(self, method, path, headers=(), end=True):
        stream = self.connection.get_next_available_stream_id()
        self.responses[stream] = dict(headers={}, body=bytearray(), trailers={}, ended=False)
        values = [(':method', method), (':scheme', 'http'), (':authority', self.authority), (':path', path)]
        values.extend((name.lower(), str(value)) for name, value in headers)
        self.connection.send_headers(stream, values, end_stream=end)
        self.flush()
        return stream

    def upload(self, stream, body):
        offset = 0
        while offset < len(body):
            size = min(self.connection.local_flow_control_window(stream),
                       self.connection.max_outbound_frame_size, len(body) - offset)
            if size == 0:
                self.receive()
                continue
            self.connection.send_data(stream, body[offset:offset + size], end_stream=offset + size == len(body))
            offset += size
            self.flush()

    def collect(self, streams):
        while not all(self.responses[stream]['ended'] for stream in streams):
            self.receive()
        return [self.responses[stream] for stream in streams]

    def request(self, method, path, headers=(), body=b''):
        stream = self.start(method, path, headers, end=not body)
        if body:
            self.upload(stream, body)
        return self.collect([stream])[0]

    def close(self):
        self.socket.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
