"""An 8 MiB read cache retains foreground hot data through maintenance scans.

Owns a stopped single service or MOKYU_TEST_NODE_CONFIG node; uses the existing
source_quarantine S3 fixture and keeps required pending uploads progressing.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import os
import subprocess
import time
import tomllib
import uuid

from tests.storage.source_quarantine import Fixture, wait
from tests.recovery.source_repair import Database


def main():
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    node = os.environ.get('MOKYU_TEST_NODE_CONFIG')
    config = node or os.environ['MOKYU_TEST_CONFIG']
    instance = tomllib.loads(Path(config).read_text())['instance']
    assert instance['cache_size'] == '8MiB', 'requires an 8MiB cache'
    root = Path(instance['data_path'])
    command = [os.environ['MOKYU_TEST_BINARY'], '--config', config]
    log = (root.parent/'cache-pressure.log').open('a')
    process = subprocess.Popen(command + (['node'] if node else []), stdout=log, stderr=subprocess.STDOUT)

    def ready():
        assert process.poll() is None, 'service exited; inspect cache-pressure.log'
        return subprocess.run(command + ['cli', 'status', '--ready'], capture_output=True, timeout=5).returncode == 0

    try:
        wait(ready, 'service did not start', seconds=180)
        fixture = Fixture()
        database = Database()
        def configure(**values):
            profile = fixture.cli('storage','profile','show',fixture.name)
            fixture.cli('storage','profile','set',fixture.name,'-',value={**profile, **values})
        def status():
            value = json.loads(subprocess.check_output(command + ['cli', 'status'], text=True)) if node else fixture.cli('status')
            return dict(jobs=value['cache']['fills'], gets=value['io']['backend']['read']['completed'],
                size=value['cache']['bytes'], cpu=value['resources']['cpu_jobs']) if node else dict(
                jobs=value['cache_jobs_active'], gets=value['backend_gets'], size=value['local_bytes'][1],
                cpu=value['resources']['cpu_jobs'])
        def idle():
            wait(lambda: status()['jobs'] == 0, 'fills did not drain')
        def raw_path(key):
            row, = database.query(f"SELECT r.{database.raw_location} AS value FROM raw_sources r JOIN raw_ranges x ON x.source_id=r.id JOIN objects o ON o.stream_id=x.stream_id WHERE o.bucket_id=%s AND o.object_key=%s",
                uuid.UUID(fixture.bucket_id), key if database.type=='postgres' else key.encode())
            value = row['value']
            value = uuid.UUID(bytes=bytes(value)) if isinstance(value,(bytes,bytearray,memoryview)) else uuid.UUID(str(value))
            return root/'cache/raw'/value.hex[-2:]/value.hex
        configure(cdc_enabled=False, deduplication_enabled=False, pack_range_enabled=False,
            write_processing='inline', read_integrity='off')
        hot = os.urandom(6*1024*1024)
        fixture.put('hot', hot)
        for i in range(48):
            fixture.put('cold-'+str(i), os.urandom(256*1024))
        configure(cache_enabled=True)
        fixture.finished(fixture.cli('integrity','check','--mode','full','--bucket',fixture.name,'--key','hot'))
        idle()
        path = raw_path('hot')
        assert path.read_bytes() == hot
        original = path.stat().st_ino
        before = status()['gets']
        times = []
        for i in range(12):
            started = time.monotonic()
            with fixture.client.get_object(Bucket=fixture.name,Key='hot',Range='bytes=0-4095')['Body'] as response:
                assert response.read() == hot[:4096]
            times.append(time.monotonic()-started)
        assert status()['gets'] == before
        job = fixture.cli('integrity','check','--mode','full','--bucket',fixture.name)
        active_times = []
        with ThreadPoolExecutor(max_workers=1) as pool:
            completion = pool.submit(fixture.finished, job)
            while not completion.done() or len(active_times) < 12:
                started = time.monotonic()
                with fixture.client.get_object(Bucket=fixture.name,Key='hot',Range='bytes=0-4095')['Body'] as response:
                    assert response.read() == hot[:4096]
                active_times.append(time.monotonic()-started)
                assert path.exists() and path.stat().st_ino == original
                current=status()
                assert current['jobs'] <= current['cpu'], current
                assert current['size'] <= 8*1024*1024, current
            completion.result()
        idle()
        assert path.read_bytes() == hot and path.stat().st_ino == original
        before=status()['gets']
        fixture.read('hot', hot)
        assert status()['gets']==before
        print('PASS scan retains hot inode and foreground cache hit',json.dumps(dict(
            base_max_ms=round(max(times)*1000,2),scan_max_ms=round(max(active_times)*1000,2),
            reads=len(active_times),cache_bytes=status()['size'])),flush=True)
        configure(upload_cache=True, cdc_enabled=not node, compression_enabled=not node)
        marker=fixture.arm('node-cache-before-upload' if node else 'upload-before-work')
        pending=os.urandom(256*1024+31)
        fixture.put('necessary',pending)
        wait(marker.with_suffix('.hit').exists,'pending work not reached')
        area = root/('pending/raw' if node else 'pending/chunks')
        assert any(area.glob('*/*'))
        job=fixture.cli('integrity','check','--mode','full','--bucket',fixture.name,'--key','cold-0')
        marker.unlink()
        wait(lambda:not any(area.glob('*/*')),'required pending upload starved',seconds=150)
        fixture.finished(job)
        fixture.read('necessary',pending)
        idle()
        assert status()['size'] <= 8*1024*1024
        print('PASS pending drain and result confirmation progress with full cache',flush=True)
    finally:
        for marker in Path(os.environ['MOKYU_TEST_FAULT_DIR']).glob('*'):
            if marker.is_file() and marker.suffix != '.log':
                marker.unlink()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=40)
        log.close()


if __name__ == '__main__':
    main()
