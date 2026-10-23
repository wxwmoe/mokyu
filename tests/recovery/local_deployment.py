"""Reproduce the documented SQLite/raw CLI setup in an isolated empty directory."""
import json
import os
import re
import secrets
import sqlite3
import stat
import subprocess
from pathlib import Path

import boto3
import requests
from botocore.config import Config

from tests.storage.source_quarantine import wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    root = Path(os.environ['MOKYU_TEST_ROOT']).resolve()
    assert root.is_dir() and not any(root.iterdir()), 'fixture must start empty'
    binary = os.environ['MOKYU_TEST_BINARY']
    document = (Path(__file__).resolve().parents[2] / 'docs/local-nginx-deployment.md').read_text()
    config, = re.findall(r'```toml\n(.*?)\n```', document, re.S)
    key = subprocess.check_output([binary, 'tool', 'keygen'], text=True).strip()
    config = config.replace('REPLACE_WITH_MOKYU_TOOL_KEYGEN_OUTPUT', key).replace('"/data"', json.dumps(str(root / 'data')))
    path = root / 'config.toml'
    path.write_text(config + '\n')
    path.chmod(0o600)
    command = [binary, '--config', str(path)]

    def cli(*args, value=None):
        result = subprocess.run(command + ['cli', *args], input=value, text=True, capture_output=True, timeout=60)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    with (root / 'app.log').open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=log)
        try:
            wait(lambda: subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True).returncode == 0,
                'documented configuration did not start', seconds=60)
            cli('init', 'admin', '--stdin', value=secrets.token_urlsafe(24) + '\n')
            store = cli('storage', 'create', 'primary', 'local', '--init', '--path', str(root / 'data/storage'))
            profile = cli('storage', 'profile', 'show', 'primary')
            expression, = re.findall(r"jq '(.*?)' > config/raw-profile.json", document, re.S)
            for field, value in re.findall(r'\.(\w+)\s*=\s*("[^"]*"|false|true)', expression):
                profile[field] = json.loads(value)
            draft = root / 'raw-profile.json'
            draft.write_text(json.dumps(profile))
            cli('storage', 'profile', 'preview', 'primary', str(draft))
            saved = cli('storage', 'profile', 'set', 'primary', str(draft))
            assert saved['file_delivery'] == 'x_accel_redirect' and not saved['cdc_enabled']
            cli('bucket', 'create', 'media')
            credentials = cli('cred', 'create', 'media')
            cli('domain', 'set', 'media.example.com:8081', 'media')
            client = boto3.client('s3', endpoint_url='http://127.0.0.1:9000', region_name='us-east-1',
                aws_access_key_id=credentials['access_key'], aws_secret_access_key=credentials['secret_key'],
                config=Config(s3={'addressing_style': 'path'}, request_checksum_calculation='when_required'))
            payload = b'documented local raw setup\n' * 1024
            client.put_object(Bucket='media', Key='sample', Body=payload, ACL='public-read')
            for port, route, headers in [(9000, '/media/sample', {}), (9001, '/sample', {'Host': 'media.example.com:8081'})]:
                url = f'http://127.0.0.1:{port}' + route
                assert requests.get(url, headers=headers, timeout=10).content == payload
                reply = requests.get(url, headers={**headers, 'X-Mokyu-File-Delivery': 'x_accel_redirect'}, timeout=10)
                assert reply.status_code == 200 and not reply.content and reply.headers['x-accel-redirect'].startswith('/_mokyu_raw/')
            raw = root / 'data/storage/raw'
            files = [path for path in raw.rglob('*') if path.is_file()]
            assert len(files) == 1 and files[0].read_bytes() == payload
            assert stat.S_IMODE(files[0].stat().st_mode) == 0o644
            assert stat.S_IMODE(raw.stat().st_mode) == 0o755
            assert stat.S_IMODE((root / 'data/mokyu.db').stat().st_mode) == 0o600
            with sqlite3.connect(root / 'data/mokyu.db') as connection:
                assert connection.execute('SELECT COUNT(*) FROM chunks').fetchone()[0] == 0
            assert cli('storage', 'show', 'primary')['id'] == store['id']
            print('PASS documented CLI initialization, profile preview/save, SQLite local raw, public reads and file permissions', flush=True)
        finally:
            process.terminate()
            process.wait(timeout=45)


if __name__ == '__main__':
    main()
