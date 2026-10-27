"""Local backends retain authoritative files without duplicate node read caches."""
import os
import subprocess
import tomllib
from pathlib import Path

from tests.storage.source_quarantine import Fixture, wait


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    assert not os.environ.get('MOKYU_TEST_BACKEND'), 'requires a local backend'
    config = Path(os.environ['MOKYU_TEST_CONFIG'])
    root = Path(tomllib.loads(config.read_text())['instance']['data_path'])
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
    process = None
    log = (root.parent / 'local-cache.log').open('a')

    def start():
        nonlocal process
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

        def ready():
            assert process.poll() is None, 'service exited; inspect local-cache.log'
            return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0
        wait(ready, 'service did not start')

    def stop():
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=40)

    try:
        start()
        fixture = Fixture()
        payload = os.urandom(6 * 1024 * 1024 + 11)
        for chunks in [False, True]:
            name = 'chunks' if chunks else 'raw'
            profile = fixture.cli('storage', 'profile', 'show', fixture.name)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value={**profile, 'cache_enabled': True}, succeeds=False)
            profile.update(cdc_enabled=chunks, compression_enabled=chunks, pack_enabled=False, pack_range_enabled=False)
            fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
            fixture.put(name, payload)
            fixture.finished(fixture.cli('integrity', 'check', '--mode', 'full', '--bucket', fixture.name, '--key', name))
            fixture.read(name, payload)
            with fixture.client.get_object(Bucket=fixture.name, Key=name, Range='bytes=13-543')['Body'] as response:
                assert response.read() == payload[13:544]
            wait(lambda: fixture.cli('status')['cache_jobs_active'] == 0, 'optional work did not drain')
            assert not [path for path in (root / 'cache').rglob('*') if path.is_file()]
            assert fixture.cli('status')['local_bytes'][1] == 0
            stop()
            start()
            fixture.read(name, payload)
            assert not [path for path in (root / 'cache').rglob('*') if path.is_file()]
            assert not any((root / area).exists() for area in ['chunks', 'raw', 'multipart', 'read-cache'])
            print('PASS local', name, 'read, maintenance and restart without duplicate L2', flush=True)
    finally:
        stop()
        log.close()


if __name__ == '__main__':
    main()
