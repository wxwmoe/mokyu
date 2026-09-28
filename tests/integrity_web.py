"""Integrity form, findings navigation, reports and localization in Chromium."""
import json
import os
import uuid
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from playwright.sync_api import sync_playwright

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
credential = json.loads(Path(os.environ['MOKYU_TEST_CREDENTIALS']).read_text())
bucket = credential['bucket']
url = os.environ['MOKYU_TEST_WEB']
results = Path(os.environ['MOKYU_TEST_RESULTS'])
results.mkdir(parents=True, exist_ok=True)
s3 = boto3.client('s3', endpoint_url=os.environ['MOKYU_TEST_ENDPOINT'], region_name='us-east-1',
    aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
    config=Config(s3={'addressing_style': 'path'}))
key = 'integrity-web/' + uuid.uuid4().hex + '/<img src=x onerror=window.injected=true>?+% #中//'
s3.put_object(Bucket=bucket, Key=key, Body=b'integrity browser fixture')
db = psycopg.connect(Path(os.environ['MOKYU_TEST_DATABASE_FILE']).read_text(), autocommit=True)
stream, chunk = db.execute('SELECT o.stream_id,e.chunk_id FROM objects o JOIN extents e ON e.stream_id=o.stream_id WHERE o.key=%s', (key,)).fetchone()
db.execute('DELETE FROM extents WHERE stream_id=%s', (stream,))
with db.cursor() as cursor:
    cursor.executemany('INSERT INTO extents(stream_id,offset_bytes,length,chunk_id) VALUES(%s,%s,1,%s)', [(stream, i * 2, chunk) for i in range(210)])

try:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(locale='en-US', viewport={'width': 1280, 'height': 900})
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.goto(url)
        page.get_by_label('Username', exact=True).fill('tester')
        page.get_by_label('Password', exact=True).fill(os.environ['MOKYU_TEST_PASSWORD'])
        page.get_by_role('button', name='Sign in', exact=True).click()
        page.locator('#browser:not([hidden])').wait_for()
        page.get_by_role('button', name='Background tasks', exact=True).click()
        page.locator('#integrity-create summary').click()
        assert page.locator('#integrity-key').is_disabled()
        page.get_by_label('Check bucket', exact=True).select_option(bucket)
        page.get_by_label('Exact object key (optional)', exact=True).fill(key)
        page.get_by_label('Check mode', exact=True).select_option('full')
        page.locator('#language').select_option('zh-CN')
        assert page.locator('#integrity-key').input_value() == key
        assert page.locator('#integrity-mode').input_value() == 'full'
        page.locator('#language').select_option('en')
        page.get_by_role('button', name='Start integrity check', exact=True).click()
        page.locator('#task-list').filter(has_text='Inspection completed with findings').wait_for(timeout=20000)
        assert page.locator('#integrity-create').is_hidden()
        assert page.locator('#integrity-report tbody tr').count() == 100
        assert not page.evaluate('Boolean(window.injected)')
        assert page.locator('#integrity-report img').count() == 0
        page.locator('#integrity-report').get_by_role('button', name='Load next page', exact=True).click()
        page.wait_for_function("() => !![...document.querySelectorAll('#integrity-report button')].find(b=>b.textContent==='Previous page')")
        assert page.locator('#integrity-report tbody tr').count() == 100
        page.locator('#integrity-report').get_by_role('button', name='Load next page', exact=True).click()
        page.wait_for_function("() => document.querySelectorAll('#integrity-report tbody tr').length === 10")
        page.locator('#integrity-report').get_by_role('button', name='Related objects', exact=True).first.click()
        page.locator('#issue-objects').get_by_role('button', name=bucket + '/' + key, exact=True).wait_for()
        with page.expect_download() as pending:
            page.get_by_role('link', name='Export report (JSONL)', exact=True).click()
        report = [json.loads(line) for line in Path(pending.value.path()).read_text().splitlines()]
        assert report[-1] == {'type': 'end', 'issues': 210}
        page.locator('#language').select_option('zh-CN')
        assert '映射存在缺口或重叠' in page.locator('#integrity-report').inner_text()
        assert '完整区块校验' in page.locator('#task-list').inner_text()
        page.screenshot(path=str(results / 'integrity-report-zh-CN.png'), full_page=True)
        page.set_viewport_size({'width': 390, 'height': 844})
        page.locator('#language').select_option('en')
        page.screenshot(path=str(results / 'integrity-report-mobile-en.png'), full_page=True)
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        page.locator('#issue-objects').get_by_role('button', name=bucket + '/' + key, exact=True).click()
        page.locator('#facts').filter(has_text=key).wait_for()
        page.go_back()
        page.locator('#task-list').filter(has_text='Inspection completed with findings').wait_for()
        page.reload()
        page.locator('#task-list').filter(has_text='Inspection completed with findings').wait_for()
        assert not errors, errors
        browser.close()
finally:
    s3.delete_object(Bucket=bucket, Key=key)
    db.close()
print('PASS integrity form, findings pagination, related objects, JSONL download, XSS, locales, mobile, history and reload', flush=True)
