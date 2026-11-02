"""Compare the primary's CLI and HTTP deployment views on a running fixture."""
import argparse
import json
import os
import subprocess

import requests


def check(binary, config, origin, password, mode, counts=None):
    def cli(*args):
        result = subprocess.run([binary, '--config', str(config), 'cli', *args],
                                capture_output=True, text=True, check=True, timeout=20)
        return json.loads(result.stdout)

    with requests.Session() as session:
        session.post(origin + '/api/v1/auth/login', headers={'Origin': origin},
                     json={'username': 'tester', 'password': password}, timeout=10).raise_for_status()

        def get(path):
            response = session.get(origin + '/api/v1/' + path, timeout=10)
            response.raise_for_status()
            return response.json()

        service, local = get('system/status'), cli('status')
        assert service['deployment_mode'] == local['deployment_mode'] == mode
        assert service['deployment_mode_revision'] == local['deployment_mode_revision']
        assert service['started_mode'] == mode and not service['mode_mismatch']
        if mode == 'single':
            assert service['current_node'] is None
            return
        overview, command = get('cluster/status'), cli('cluster', 'status')
        fields = ('mode', 'mode_revision', 'remote_nodes', 'ready_data', 'ready_workers', 'remote_work')
        assert [overview[key] for key in fields] == [command[key] for key in fields]
        assert overview['mode'] == mode and overview['mode_revision'] == service['deployment_mode_revision']
        if counts is not None:
            assert [overview[key] for key in fields[2:]] == list(counts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('single', 'single_control'))
    parser.add_argument('--counts', type=int, nargs=4, metavar=('PEERS', 'DATA', 'WORKERS', 'REMOTE_WORK'))
    args = parser.parse_args()
    check(os.environ['MOKYU_TEST_BINARY'], os.environ['MOKYU_TEST_CONFIG'],
          os.environ['MOKYU_TEST_WEB'].rstrip('/'), os.environ['MOKYU_TEST_PASSWORD'],
          args.mode, args.counts)
