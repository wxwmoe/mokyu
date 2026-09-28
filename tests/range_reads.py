"""Cold Range evidence, shared-download accounting and repack cooldowns; isolated only."""
import fcntl
import json
import os
import signal
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from integration import s3, bucket

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
config = Path(os.environ['MOKYU_TEST_CONFIG'])
original = config.read_text()
command = [os.environ['MOKYU_TEST_BINARY'], '--config', str(config)]
data = Path(os.environ['MOKYU_TEST_DATA'])
faults = Path(os.environ['MOKYU_TEST_FAULT_DIR'])
pid = int(os.environ['MOKYU_TEST_GATEWAY_PID'])
process = None


def cli(*args):
    return json.loads(subprocess.check_output(command + ['cli', *args], stderr=subprocess.DEVNULL, text=True))


def wait(predicate, message='Range test timed out'):
    for _ in range(600):
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError(message)


def task(*args):
    job = cli(*args)['task_id']
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'))
    state = cli('task', 'show', job)
    assert state['state'] == 'completed', state
    return state


def get(span=None):
    return s3.get_object(Bucket=bucket, Key='ranges', **({'Range': span} if span else {}))['Body'].read()


def count(pack):
    return db.execute('SELECT COALESCE(sum(downloads),0)::bigint,COALESCE(sum(partial_downloads),0)::bigint FROM pack_access_windows WHERE pack_id=%s', (pack,)).fetchone()


def evict():
    for path in (data / 'chunks').glob('*/*'):
        if path.is_file():
            path.unlink()


def restart(text, maintenance=False):
    global pid, process
    os.kill(pid, signal.SIGKILL)
    if process:
        process.wait(timeout=10)
    def released():
        with (data / 'gateway.lock').open('rb') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False
    wait(released)
    config.write_text(text)
    process = subprocess.Popen(command + ['serve'] + (['--maintenance'] if maintenance else []), stdout=open(Path(os.environ['MOKYU_TEST_RESULTS']) / 'range-restarts.log', 'a'), stderr=subprocess.STDOUT)
    pid = process.pid
    def ready():
        assert process.poll() is None, 'Range restart exited'
        try:
            return cli('status')['maintenance'] == maintenance
        except subprocess.CalledProcessError:
            return False
    wait(ready)


try:
    raw = os.urandom(40 * 1024 * 1024)
    s3.put_object(Bucket=bucket, Key='ranges', Body=raw)
    task('pack', 'run')
    stream = db.execute("SELECT stream_id FROM objects WHERE key='ranges'").fetchone()[0]
    rows = db.execute('SELECT e.offset_bytes,c.id,c.raw_size,c.pack_id FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE e.stream_id=%s ORDER BY e.offset_bytes', (stream,)).fetchall()
    pack = rows[0][3]
    members = [r for r in rows if r[3] == pack]
    assert len(members) > 8
    target = members[len(members) // 2]
    start = target[0] + 128
    span = f'bytes={start}-{start + 65535}'
    expected = raw[start:start + 65536]
    ids = [r[1] for r in members]
    old_bytes = db.execute('SELECT stored_size FROM packs WHERE id=%s', (pack,)).fetchone()[0]
    before = cli('status')['backend_gets']
    for _ in range(10):
        assert get(span) == expected
    s3.head_object(Bucket=bucket, Key='ranges')
    assert cli('status')['backend_gets'] == before
    time.sleep(1.3)
    assert count(pack) == (0, 0)
    assert task('pack', 'run', '--kind', 'range')['processed'] == 0
    for full_span in (None, f'bytes=0-{len(raw) - 1}'):
        evict()
        previous = count(pack)[0]
        assert get(full_span) == raw
        wait(lambda: count(pack)[0] == previous + 1)
        assert count(pack)[1] == 0
    print('PASS warm Range, HEAD, full GET and full-range noise', flush=True)

    evict()
    marker = faults / 'pack-download'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    previous = count(pack)[0]
    with ThreadPoolExecutor(max_workers=2) as pool:
        whole = pool.submit(get)
        wait(marker.with_suffix('.hit').exists)
        small = pool.submit(get, span)
        time.sleep(.3)
        marker.unlink()
        assert whole.result(timeout=30) == raw and small.result(timeout=30) == expected
    wait(lambda: count(pack)[0] == previous + 1)
    assert count(pack)[1] == 0
    print('PASS shared full/Range download counted once without false partial pressure', flush=True)

    evict()
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    previous = count(pack)
    with ThreadPoolExecutor(max_workers=3) as pool:
        one = pool.submit(get, span)
        wait(marker.with_suffix('.hit').exists)
        others = [pool.submit(get, span) for _ in range(2)]
        time.sleep(.3)
        marker.unlink()
        assert one.result(timeout=30) == expected
        assert all(f.result(timeout=30) == expected for f in others)
    wait(lambda: count(pack)[0] == previous[0] + 1)
    assert count(pack)[1] == previous[1] + 1
    assert task('pack', 'run', '--kind', 'range')['processed'] == 0
    print('PASS three Range waiters count as one download; one cold download does not split', flush=True)

    for _ in range(7):
        evict()
        previous = count(pack)[1]
        assert get(span) == expected
        wait(lambda: count(pack)[1] == previous + 1)
    publish = faults / 'pack-before-publish'
    publish.with_suffix('.hit').unlink(missing_ok=True)
    publish.touch()
    job = cli('pack', 'run', '--kind', 'range')['task_id']
    wait(publish.with_suffix('.hit').exists)
    cli('maintenance', 'enable')
    restart(original, maintenance=True)
    publish.unlink()
    assert cli('task', 'show', job)['state'] == 'paused'
    assert db.execute('SELECT bool_and(pack_id=%s AND range_split_at IS NULL) FROM chunks WHERE id=ANY(%s)', (pack, ids)).fetchone()[0]
    assert db.execute('SELECT count(*) FROM pack_inputs').fetchone()[0] == 0
    assert get(span) == expected
    cli('maintenance', 'disable')
    cli('task', 'resume', job)
    wait(lambda: cli('task', 'show', job)['state'] in ('completed', 'failed'))
    result = cli('task', 'show', job)
    assert result['state'] == 'completed', result
    print('PASS Range publication crash, maintenance pause and resumed layout switch', flush=True)
    assert result['processed'] == len(members), result
    report = result['detail']['last_range']
    assert report['applied'] and report['benefit']['net_savings_bytes'] >= 64 * 1024 * 1024, report
    sources = db.execute('SELECT c.id,c.pack_id,c.range_split_at IS NOT NULL,c.repack_after>now()+interval \'29 days\',l.stored_size FROM chunks c LEFT JOIN chunk_locations l ON l.chunk_id=c.id AND l.state=\'ready\' WHERE c.id=ANY(%s)', (ids,)).fetchall()
    hot = next(r for r in sources if r[0] == target[1])
    assert hot[1] is None and all(r[2] and r[3] for r in sources), sources
    assert hot[4] < old_bytes // 4
    evict()
    before = cli('status')['backend_gets']
    assert get(span) == expected
    assert cli('status')['backend_gets'] == before + 1
    assert get() == raw
    print('PASS repeated cold 64 KiB reads split only a beneficial layout:', json.dumps(report), flush=True)

    layout = [(r[0], r[1]) for r in sources]
    task('pack', 'run', '--kind', 'repack')
    assert sorted(db.execute('SELECT id,pack_id FROM chunks WHERE id=ANY(%s)', (ids,)).fetchall()) == sorted(layout)
    # Old physical metadata and completed task history do not own the cooldown.
    db.execute('DELETE FROM packs WHERE id=%s', (pack,))
    db.execute("DELETE FROM tasks WHERE state='completed' AND kind='pack'")

    def expire():
        db.execute("UPDATE chunks SET range_split_at=now()-interval '31 days',repack_after=now()-interval '1 second',reference_changed_at=now()-interval '2 hours' WHERE id=ANY(%s)", (ids,))

    expire()
    db.execute("UPDATE mokyu_meta SET access_coverage_since=now()-interval '8 days',access_flushed_at=now()")
    task('pack', 'run', '--kind', 'repack')
    assert db.execute("SELECT repack_after>now()+interval '6 days' FROM chunks WHERE id=%s", (target[1],)).fetchone()[0]
    db.execute('DELETE FROM chunk_access_windows WHERE chunk_id=ANY(%s)', (ids,))
    expire()
    db.execute('UPDATE mokyu_meta SET access_coverage_since=now()')
    task('pack', 'run', '--kind', 'repack')
    assert db.execute("SELECT bool_and(repack_after>now()+interval '6 days') FROM chunks WHERE id=ANY(%s)", (ids,)).fetchone()[0]
    expire()
    db.execute("UPDATE mokyu_meta SET access_coverage_since=now()-interval '8 days',access_flushed_at=now()")
    task('pack', 'run', '--kind', 'repack')
    packed = db.execute('SELECT DISTINCT pack_id,range_split_at FROM chunks WHERE id=ANY(%s)', (ids,)).fetchall()
    assert len(packed) == 1 and packed[0][0] is not None and packed[0][1] is None, packed
    assert get() == raw
    print('PASS 30-day cooldown, 7-day activity/coverage checks, 7-day retry and repack after history deletion', flush=True)

    pattern = os.urandom(64 * 1024)
    compressible = b''.join(pattern * 63 + os.urandom(64 * 1024) for _ in range(10))
    s3.put_object(Bucket=bucket, Key='compressed-ranges', Body=compressible)
    task('pack', 'run')
    compressed_pack = db.execute("SELECT c.pack_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.key='compressed-ranges' ORDER BY e.offset_bytes LIMIT 1").fetchone()[0]
    assert compressed_pack is not None
    assert db.execute('SELECT compressed FROM packs WHERE id=%s', (compressed_pack,)).fetchone()[0]
    for _ in range(8):
        evict()
        previous = count(compressed_pack)[1]
        assert s3.get_object(Bucket=bucket, Key='compressed-ranges', Range='bytes=0-65535')['Body'].read() == compressible[:65536]
        wait(lambda: count(compressed_pack)[1] == previous + 1)
    before_puts = cli('status')['backend_puts']
    result = task('pack', 'run', '--kind', 'range')
    assert result['processed'] == 0 and result['detail']['last_range']['reason'] == 'insufficient_net_savings', result
    assert cli('status')['backend_puts'] == before_puts
    del compressible
    print('PASS highly compressed pack stays intact when encoded-byte savings cannot pay rewrite costs', flush=True)

    restart(original.replace('max_size="128MiB"', 'max_size="0B"') + '\n[pack]\nenabled=false\nrange_optimization=true\n')
    pack = packed[0][0]
    initial_packs = db.execute('SELECT count(*) FROM packs').fetchone()[0]
    for _ in range(8):
        previous = count(pack)[1]
        assert get(span) == expected
        wait(lambda: count(pack)[1] == previous + 1)
    result = task('pack', 'run', '--kind', 'range')
    assert result['processed'] == len(members), result
    assert db.execute('SELECT bool_and(pack_id IS NULL) FROM chunks WHERE id=ANY(%s)', (ids,)).fetchone()[0]
    assert db.execute('SELECT count(*) FROM packs').fetchone()[0] == initial_packs
    assert get() == raw
    print('PASS pack disabled / Range enabled outputs only independent chunks, including zero-cache reads', flush=True)

    from playwright.sync_api import sync_playwright
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(locale='en-US')
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto(os.environ['MOKYU_TEST_WEB'])
        page.get_by_label('Username', exact=True).fill('tester')
        page.get_by_label('Password', exact=True).fill(os.environ['MOKYU_TEST_PASSWORD'])
        page.get_by_role('button', name='Sign in', exact=True).click()
        page.locator('#browser:not([hidden])').wait_for()
        page.locator('#packs-tab').click()
        page.get_by_role('button', name='Optimize cold Range reads', exact=True).wait_for()
        assert page.get_by_role('button', name='Pack eligible chunks', exact=True).count() == 0
        page.locator('#language').select_option('zh-CN')
        page.get_by_role('button', name='优化 Range 回源', exact=True).wait_for()
        page.screenshot(path=str(Path(os.environ['MOKYU_TEST_RESULTS']) / 'range-pack-off-zh.png'), full_page=True)
        assert not errors, errors
        browser.close()
    print('PASS Range management in both locales while pack creation is disabled', flush=True)

    restart(original + '\n[pack]\nrange_optimization=false\n')
    assert not cli('pack', 'status')['range_optimization']
    assert subprocess.run(command + ['cli', 'pack', 'run', '--kind', 'range'], capture_output=True).returncode != 0
    print('PASS disabled Range optimization rejects manual scheduling', flush=True)
finally:
    (faults / 'pack-download').unlink(missing_ok=True)
    (faults / 'pack-before-publish').unlink(missing_ok=True)
    if process and process.poll() is None:
        process.terminate()
        process.wait(timeout=40)
