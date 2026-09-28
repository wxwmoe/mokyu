"""Streaming, reuse and multipart behavior with async upload and a 512 MiB cache."""
import hashlib
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from integration import s3, bucket

assert os.environ.get('MGW_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MGW_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
command = [os.environ['MGW_TEST_BINARY'], '--config', os.environ['MGW_TEST_CONFIG']]
faults = Path(os.environ['MGW_TEST_FAULT_DIR'])


def cli(*args):
    return json.loads(subprocess.check_output(command + ['cli', *args], text=True))


def wait(predicate, message):
    for _ in range(900):
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError(message)


def scalar(sql, args=()):
    return db.execute(sql, args).fetchone()[0]


def drain():
    wait(lambda: scalar('SELECT count(*) FROM pending_uploads') == 0, 'pending uploads did not drain')


def read_hash(key):
    digest = hashlib.sha256()
    with s3.get_object(Bucket=bucket, Key=key)['Body'] as body:
        while data := body.read(1024 * 1024):
            digest.update(data)
    return digest.digest()


path = Path(os.environ['MGW_TEST_RESULTS']) / 'streaming-input.bin'
try:
    digest = hashlib.sha256()
    with path.open('wb') as out:
        for _ in range(96):
            data = os.urandom(1024 * 1024)
            out.write(data)
            digest.update(data)
    # Pause at a later frontend chunk allocation, after an earlier CDC segment can drain.
    def upload():
        with path.open('rb') as body:
            return s3.put_object(Bucket=bucket, Key='streaming', Body=body)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(upload)
        wait(lambda: scalar('SELECT COALESCE(sum(size),0)>36*1024*1024 FROM streams WHERE object_key=\'streaming\' AND state=\'writing\'') or future.done(), 'streaming upload did not advance')
        marker = faults / 'chunk-allocated'
        marker.with_suffix('.hit').unlink(missing_ok=True)
        marker.touch()
        wait(lambda: marker.with_suffix('.hit').exists() or future.done(), 'receiver barrier not reached')
        assert not future.done(), 'input finished before streaming checkpoint'
        wait(lambda: scalar("SELECT count(*) FROM packs WHERE state='ready'") > 0, 'no pack uploaded while frontend was still receiving')
        assert scalar("SELECT count(*) FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.key='streaming' AND s.state='ready'") == 0
        assert scalar("SELECT max(raw_size) FROM packs WHERE state='ready'") <= 32 * 1024 * 1024
        marker.unlink()
        future.result(timeout=120)
    drain()
    assert read_hash('streaming') == digest.digest()
    assert cli('cache', 'status')['fallbacks'] == 0
    independent = db.execute("SELECT e.offset_bytes,c.raw_size FROM chunk_locations l JOIN chunks c ON c.id=l.chunk_id JOIN extents e ON e.chunk_id=c.id WHERE l.state='ready'").fetchall()
    assert not independent or (len(independent) == 1 and sum(independent[0]) == 96 * 1024 * 1024), independent
    print('PASS 96 MiB bounded stream uploads CDC-aligned packs before frontend EOF', flush=True)

    raw = os.urandom(15 * 1024 * 1024 + 17)
    s3.put_object(Bucket=bucket, Key='reuse-source', Body=raw)
    drain()
    source_pack = scalar("SELECT c.pack_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.key='reuse-source' LIMIT 1")
    before = cli('status')['backend_puts']
    s3.put_object(Bucket=bucket, Key='reuse-whole', Body=raw)
    drain()
    assert cli('status')['backend_puts'] == before, 'whole pack reuse wrote a temporary copy'
    assert scalar('SELECT state FROM packs WHERE id=%s', (source_pack,)) == 'ready'
    members = db.execute('SELECT m.chunk_id,m.offset_bytes,c.raw_size FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=%s ORDER BY m.ordinal', (source_pack,)).fetchall()
    selected = members[2:4]
    assert len(selected) == 2
    subset = raw[selected[0][1]:selected[-1][1] + selected[-1][2]]
    independent = scalar("SELECT count(*) FROM chunk_locations WHERE state='ready'")
    marker = faults / 'upload-before-work'
    marker.with_suffix('.hit').unlink(missing_ok=True)
    marker.touch()
    s3.put_object(Bucket=bucket, Key='reuse-subset', Body=subset)
    wait(marker.with_suffix('.hit').exists, 'reuse did not queue async work')
    assert scalar('SELECT count(*) FROM cache_pins WHERE pin_type=\'pack\'') >= 2
    assert cli('status')['backend_puts'] == before
    s3.delete_object(Bucket=bucket, Key='reuse-whole')
    s3.delete_object(Bucket=bucket, Key='reuse-source')
    cli('gc', 'run')
    assert s3.get_object(Bucket=bucket, Key='reuse-subset')['Body'].read() == subset
    marker.unlink()
    drain()
    assert scalar("SELECT count(*) FROM chunk_locations WHERE state='ready'") == independent, 'shared multi-chunk segment became temporary independent sources'
    assert s3.get_object(Bucket=bucket, Key='reuse-subset')['Body'].read() == subset
    print('PASS whole-pack reuse, pinned shared segment and old-object deletion before split', flush=True)

    pieces = [os.urandom(15 * 1024 * 1024) for _ in range(3)]
    upload_id = s3.create_multipart_upload(Bucket=bucket, Key='unordered')['UploadId']
    etags = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {i: pool.submit(s3.upload_part, Bucket=bucket, Key='unordered', UploadId=upload_id, PartNumber=i, Body=pieces[i-1]) for i in (3, 2)}
        for i, result in futures.items():
            etags[i] = result.result()['ETag']
    etags[1] = s3.upload_part(Bucket=bucket, Key='unordered', UploadId=upload_id, PartNumber=1, Body=pieces[0])['ETag']
    replacement = os.urandom(len(pieces[1]))
    etags[2] = s3.upload_part(Bucket=bucket, Key='unordered', UploadId=upload_id, PartNumber=2, Body=replacement)['ETag']
    pieces[1] = replacement
    s3.complete_multipart_upload(Bucket=bucket, Key='unordered', UploadId=upload_id, MultipartUpload={'Parts': [{'PartNumber': i, 'ETag': etags[i]} for i in (1, 2, 3)]})
    expected = hashlib.sha256()
    for piece in pieces:
        expected.update(piece)
    assert read_hash('unordered') == expected.digest()
    flush = cli('cache', 'flush')['task_id']
    wait(lambda: cli('task', 'show', flush)['state'] in ('failed', 'completed'), 'multipart flush did not finish')
    assert cli('task', 'show', flush)['state'] == 'completed', cli('task', 'show', flush)
    assert scalar('SELECT count(*) FROM pending_uploads') == 0
    print('PASS parallel out-of-order multipart, part replacement and completion with local pending sources', flush=True)
finally:
    (faults / 'chunk-allocated').unlink(missing_ok=True)
    (faults / 'upload-before-work').unlink(missing_ok=True)
    path.unlink(missing_ok=True)
