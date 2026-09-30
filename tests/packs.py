"""Pack layout and physical lifetime checks against an isolated gateway (cache disabled)."""
import json
import hashlib
import os
import subprocess
import time
import tomllib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import psycopg
from integration import s3, bucket
import boto3
import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text().strip(), autocommit=True)
command = [os.environ['MOKYU_TEST_BINARY'], '--config', os.environ['MOKYU_TEST_CONFIG'], 'cli']


def cli(*args):
    result = subprocess.run(command + list(args), capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def task(*args):
    job = cli(*args)['task_id']
    for _ in range(600):
        state = cli('task', 'show', job)
        assert state['state'] not in ('failed', 'paused'), state
        if state['state'] == 'completed':
            return state
        time.sleep(.1)
    raise AssertionError(f'task did not finish: {job}')


def get(key, **kwargs):
    return s3.get_object(Bucket=bucket, Key=key, **kwargs)['Body'].read()

def wait(predicate):
    for _ in range(200):
        value=predicate()
        if value: return value
        time.sleep(.1)
    raise AssertionError('condition timed out')


raw = os.urandom(15 * 1024 * 1024 + 17)
s3.put_object(Bucket=bucket, Key='pack-source', Body=raw)
task('pack', 'run')
pack = db.execute("SELECT DISTINCT c.pack_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.key='pack-source'").fetchall()
assert len(pack) == 1 and pack[0][0], pack
original_pack = pack[0][0]
assert get('pack-source') == raw
assert get('pack-source', Range='bytes=500000-600001') == raw[500000:600002]
# Full ordered reuse retains the existing physical pack.
s3.copy_object(Bucket=bucket,Key='pack-whole-copy',CopySource={'Bucket':bucket,'Key':'pack-source'})
task('pack','run','--kind','reuse')
assert db.execute("SELECT state FROM packs WHERE id=%s",(original_pack,)).fetchone()[0]=='ready'
s3.delete_object(Bucket=bucket,Key='pack-whole-copy')
wait(lambda: (cli('gc','run'),db.execute("SELECT count(*)=0 FROM streams WHERE object_key='pack-whole-copy'").fetchone()[0])[1])

# Full verification downloads the physical object once even across inspection batches.
before=cli('status')['backend_gets']
checked=task('integrity','check','--mode','full','--bucket',bucket,'--key','pack-source')
assert checked['detail']['issues']==0,checked
assert cli('status')['backend_gets']-before==1
wait(lambda: db.execute('SELECT sum(downloads)>0 FROM pack_access_windows WHERE pack_id=%s',(original_pack,)).fetchone()[0])
assert db.execute('SELECT sum(range_reads)>0 FROM chunk_access_stats').fetchone()[0]

# The body reuses its decoded pack even when the persistent cache cannot fill.
before = cli('status')['backend_gets'] if 'backend_gets' in cli('status') else None
assert get('pack-source') == raw
if before is not None:
    assert cli('status')['backend_gets'] - before == 1

marker=Path(os.environ['MOKYU_TEST_FAULT_DIR'])/'pack-download'
marker.with_suffix('.hit').unlink(missing_ok=True)
marker.touch()
before=cli('status')['backend_gets']
try:
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures=[pool.submit(get,'pack-source') for _ in range(3)]
        try:
            wait(marker.with_suffix('.hit').exists)
            wait(lambda: cli('status')['runtime']['http']['s3']['active']>=3)
        finally:
            marker.unlink(missing_ok=True)
        assert all(f.result(timeout=30)==raw for f in futures)
finally:
    marker.unlink(missing_ok=True)
assert cli('status')['backend_gets']-before==1

members = db.execute("SELECT e.offset_bytes,e.length,c.id FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.key='pack-source' ORDER BY e.offset_bytes").fetchall()
assert len(members) >= 4
start = members[1][0]
end = members[2][0] + members[2][1]
shared = raw[start:end]
s3.put_object(Bucket=bucket, Key='pack-shared', Body=shared)
task('pack', 'run', '--kind', 'reuse')
assert get('pack-source') == raw
assert get('pack-shared') == shared
shared_packs = db.execute('SELECT DISTINCT pack_id FROM chunks WHERE id=ANY(%s)', ([members[1][2], members[2][2]],)).fetchall()
assert len(shared_packs) == 1 and shared_packs[0][0] != original_pack, shared_packs
assert db.execute('SELECT state FROM packs WHERE id=%s', (original_pack,)).fetchone()[0] == 'retired'

# Source GC must retain the replacement and clean retired physical files.
db.execute("UPDATE chunk_locations SET unreferenced_at=now()-interval '3 days' WHERE state='retired'")
db.execute("UPDATE packs SET unreferenced_at=now()-interval '3 days' WHERE state='retired'")
cli('gc', 'run')
assert get('pack-source') == raw and get('pack-shared') == shared
assert db.execute('SELECT state FROM packs WHERE id=%s', (original_pack,)).fetchone()[0] == 'deleted'

target = shared_packs[0][0]
if target is not None:
    preview = cli('pack', 'unpack', str(target))
    assert preview['preview'] and preview['packs'] == 1
    task('pack', 'unpack', str(target), '--execute')
    assert get('pack-source') == raw and get('pack-shared') == shared
    assert db.execute('SELECT count(*) FROM chunks WHERE pack_id=%s', (target,)).fetchone()[0] == 0

cli('maintenance', 'enable')
blocked = subprocess.run(command + ['pack', 'run'], capture_output=True, text=True)
assert blocked.returncode != 0
assert get('pack-source') == raw
cli('maintenance', 'disable')

# Removed sharing permits whole adjacent packs/blocks to coalesce after cooldown.
s3.delete_object(Bucket=bucket,Key='pack-shared')
wait(lambda: (cli('gc','run'),db.execute("SELECT count(*)=0 FROM streams WHERE object_key='pack-shared'").fetchone()[0])[1])
before_layout=db.execute("SELECT c.id,c.pack_id FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN objects o ON o.stream_id=e.stream_id WHERE o.key='pack-source' ORDER BY c.id").fetchall()
task('pack','run','--kind','repack')
assert before_layout==db.execute("SELECT c.id,c.pack_id FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN objects o ON o.stream_id=e.stream_id WHERE o.key='pack-source' ORDER BY c.id").fetchall()
db.execute("UPDATE chunks SET repack_after=now()-interval '2 hours',reference_changed_at=now()-interval '2 hours' WHERE id=ANY(%s)",([r[0] for r in before_layout],))
task('pack','run','--kind','repack')
assert get('pack-source')==raw
assert db.execute("SELECT count(DISTINCT c.pack_id)=1 AND bool_and(c.pack_id IS NOT NULL) FROM chunks c JOIN extents e ON e.chunk_id=c.id JOIN objects o ON o.stream_id=e.stream_id WHERE o.key='pack-source'").fetchone()[0]

# Segment at complete CDC boundaries and preserve byte-accurate reads across packs.
large=os.urandom(69*1024*1024+19)
s3.put_object(Bucket=bucket,Key='pack-large',Body=large)
task('pack','run')
assert get('pack-large')==large
assert get('pack-large',Range='bytes=32000000-36000000')==large[32000000:36000001]
packs=db.execute("SELECT DISTINCT p.id,p.raw_size FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id JOIN packs p ON p.id=c.pack_id WHERE o.key='pack-large'").fetchall()
assert len(packs)>=3 and all(n<=32*1024*1024 for _,n in packs),packs

# Already tiny independent frames are retained if the pack index makes them worse.
tiny=b''.join(bytes([i])*1024*1024 for i in range(16))
s3.put_object(Bucket=bucket,Key='pack-tiny-frames',Body=tiny)
task('pack','run')
assert get('pack-tiny-frames')==tiny
assert db.execute("SELECT bool_and(c.pack_id IS NULL) FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.key='pack-tiny-frames'").fetchone()[0]

compressed=b''.join((lambda part: part+part)(os.urandom(16384)) for _ in range(512))
s3.put_object(Bucket=bucket,Key='pack-compressed',Body=compressed)
hints=db.execute("SELECT c.id,c.compressed,c.stored_size FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.key='pack-compressed' ORDER BY e.offset_bytes").fetchall()
task('pack','run')
assert get('pack-compressed')==compressed
assert hints==db.execute("SELECT c.id,c.compressed,c.stored_size FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id WHERE o.key='pack-compressed' ORDER BY e.offset_bytes").fetchall()
assert db.execute("SELECT bool_and(p.compressed) FROM objects o JOIN extents e ON e.stream_id=o.stream_id JOIN chunks c ON c.id=e.chunk_id JOIN packs p ON p.id=c.pack_id WHERE o.key='pack-compressed'").fetchone()[0]

# Simulate reference retirement followed by logical GC before pack maintenance.
reclaim=os.urandom(20*1024*1024)
s3.put_object(Bucket=bucket,Key='pack-gc-first',Body=reclaim)
task('pack','run')
cli('maintenance','enable')
stream=db.execute("SELECT stream_id FROM objects WHERE key='pack-gc-first'").fetchone()[0]
rows=db.execute('SELECT e.offset_bytes,e.length,c.id,c.pack_id FROM extents e JOIN chunks c ON c.id=e.chunk_id WHERE stream_id=%s ORDER BY e.offset_bytes',(stream,)).fetchall()
cut=rows[1][0]+rows[1][1];old_pack=rows[0][3]
with db.transaction():
    db.execute('DELETE FROM extents WHERE stream_id=%s AND offset_bytes>=%s',(stream,cut))
    db.execute('UPDATE streams SET size=%s,etag=%s WHERE id=%s',(cut,hashlib.md5(reclaim[:cut]).hexdigest(),stream))
    db.execute("UPDATE chunks SET unreferenced_at=now()-interval '3 days' WHERE id=ANY(%s)",([r[2] for r in rows[2:]],))
cli('maintenance','disable')
cli('gc','run')
assert db.execute('SELECT bool_and(pack_id IS NULL) FROM chunks WHERE id=ANY(%s)',([r[2] for r in rows[2:]],)).fetchone()[0]
task('pack','run','--kind','reclaim')
assert db.execute('SELECT state FROM packs WHERE id=%s',(old_pack,)).fetchone()[0]=='retired'
assert get('pack-gc-first')==reclaim[:cut]

# Metadata-only maintenance must never treat indexed pack payloads as sweep orphans.
configuration=tomllib.loads(Path(os.environ['MOKYU_TEST_CONFIG']).read_text())['backend']
backend=boto3.client('s3',endpoint_url=configuration['endpoint'],region_name=configuration['region'],aws_access_key_id=configuration['access_key'],aws_secret_access_key=configuration['secret_key'])
orphan=configuration['prefix']+'/packs/aa/'+'a'*32
backend.put_object(Bucket=configuration['bucket'],Key=orphan,Body=b'orphan')
time.sleep(1.2)
swept=task('backend','sweep','--older-than','1s')
assert swept['detail']['candidates']==1,swept
assert orphan in swept['detail']['samples']

web=os.environ['MOKYU_TEST_WEB'];session=requests.Session()
login=session.post(web+'/api/login',headers={'Origin':web},json={'username':'tester','password':os.environ['MOKYU_TEST_PASSWORD']});login.raise_for_status()
headers={'Origin':web,'X-CSRF-Token':login.json()['csrf_token']}
page=session.get(web+'/api/packs',params={'limit':1});page.raise_for_status()
assert len(page.json()['packs'])==1 and isinstance(page.json()['packs'][0]['id'],str)
assert session.get(web+'/api/packs?limit=0').status_code==400
assert requests.get(web+'/api/packs').status_code==403
assert session.post(web+'/api/maintenance/pack/actions',json={'action':'run'}).status_code==403
assert session.post(web+'/api/maintenance/invalid/actions',headers=headers,json={'action':'run'}).status_code==400
assert session.post(web+'/api/maintenance/unpack/preview',headers=headers,json={'pack_id':'9007199254740993'}).status_code in (200,404)
assert db.execute('SELECT count(*) FROM pack_inputs').fetchone()[0]==0

# Browser workflows are covered by web/tests/insights.spec.ts and maintenance.spec.ts.
print('PASS pack creation, zero-cache reads, partial reuse, physical GC, unpack and maintenance')
print('PASS physical inspection, access statistics, cooldown/coalescing, CDC pack limits, independent compression hints, sweep and management access')
