"""Browser checks for both supported Web locales, against an isolated test setup."""
import os
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode
import boto3
from botocore.config import Config
from playwright.sync_api import sync_playwright

url = os.environ['MGW_TEST_WEB']
results = Path(os.environ['MGW_TEST_RESULTS'])
results.mkdir(parents=True, exist_ok=True)
credential = json.loads(Path(os.environ['MGW_TEST_CREDENTIALS']).read_text())
s3 = boto3.client('s3', endpoint_url=os.environ['MGW_TEST_ENDPOINT'], region_name='us-east-1',
                  aws_access_key_id=credential['access_key'], aws_secret_access_key=credential['secret_key'],
                  config=Config(s3={'addressing_style': 'path'}, max_pool_connections=8))
bucket = credential['bucket']
def fixture(i):
    s3.put_object(Bucket=bucket, Key=f'web-accept/file-{i:03}', Body=b'private pagination fixture')
with ThreadPoolExecutor(max_workers=4) as pool:
    list(pool.map(fixture, range(121)))
injection = '<img src=x onerror=window.injected=true>'
s3.put_object(Bucket=bucket, Key='web-accept/' + injection, Body=b'safe', Metadata={'probe': '<script>window.injected=true</script>'})
s3.put_object(Bucket=bucket, Key='web-accept/unsafe.html', Body=b'<script>window.injected=true</script>', ContentType='text/html')
for name, mime in [('image.png', 'image/png'), ('video.mp4', 'video/mp4')]:
    with (Path(os.environ['MGW_TEST_MEDIA']) / name).open('rb') as body:
        s3.put_object(Bucket=bucket, Key='web-accept/' + name, Body=body, ContentType=mime)

with sync_playwright() as playwright:
    browser = playwright.chromium.launch()
    context = browser.new_context(locale='en-US', viewport={'width': 1280, 'height': 900})
    page = context.new_page()
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(url)
    page.locator('html[lang="en"]').wait_for()
    page.get_by_role('heading', name='Administration sign in').wait_for()
    assert page.evaluate("async () => { const {messages} = await import('/i18n.js'); return JSON.stringify(Object.keys(messages.en).sort()) === JSON.stringify(Object.keys(messages['zh-CN']).sort()); }")
    assert page.evaluate("async () => { const {messages} = await import('/i18n.js'); return [...document.querySelectorAll('[data-i18n], [data-i18n-label]')].every(e => Object.hasOwn(messages.en, e.dataset.i18n || e.dataset.i18nLabel)); }")
    page.screenshot(path=str(results / 'web-login-en.png'), full_page=True)
    page.get_by_label('Username', exact=True).fill('tester')
    page.get_by_label('Password', exact=True).fill('incorrect-password')
    page.get_by_role('button', name='Sign in', exact=True).click()
    page.locator('#notice').filter(has_text='Request failed').wait_for()
    assert 'Request ID:' in page.locator('#notice').inner_text()
    page.locator('#language').select_option('zh-CN')
    assert '请求失败' in page.locator('#notice').inner_text()
    page.get_by_role('heading', name='管理登录').wait_for()
    page.screenshot(path=str(results / 'web-login-zh-CN.png'), full_page=True)
    page.reload()
    page.locator('html[lang="zh-CN"]').wait_for()
    page.get_by_label('用户名', exact=True).fill('tester')
    page.get_by_label('密码', exact=True).fill(os.environ['MGW_TEST_PASSWORD'])
    page.get_by_role('button', name='登录', exact=True).click()
    page.locator('#browser:not([hidden])').wait_for()
    page.locator('#buckets').select_option(label='test-media')
    page.locator('#files button').first.wait_for()
    page.get_by_role('button', name='服务状态', exact=True).click()
    page.locator('#facts').filter(has_text='检测到的内存预算').wait_for()
    page.locator('#statistics').filter(has_text='区块缓存命中率').wait_for()
    assert page.locator('#statistics .stats-cards>div').count() == 6
    assert page.get_by_role('table', name='数据库清理', exact=True).count() == 1
    page.screenshot(path=str(results / 'web-status-zh-CN.png'), full_page=True)
    page.locator('#language').select_option('en')
    assert 'Detected memory budget' in page.locator('#facts').inner_text()
    assert page.locator('#detail-title').inner_text() == 'Service status'
    assert 'HTTP requests' in page.locator('#statistics').inner_text()
    assert page.get_by_role('table', name='Database cleanup', exact=True).count() == 1
    page.get_by_role('button', name='Refresh statistics', exact=True).click()
    page.locator('#statistics').filter(has_text='Indexed remote size').wait_for()
    page.screenshot(path=str(results / 'web-status-en.png'), full_page=True)
    page.set_viewport_size({'width': 390, 'height': 844})
    page.screenshot(path=str(results / 'web-status-mobile-en.png'), full_page=True)
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
    page.set_viewport_size({'width': 1280, 'height': 900})
    page.get_by_role('button', name='Background tasks', exact=True).click()
    page.locator('#detail-title').filter(has_text='Background tasks').wait_for()
    page.get_by_role('button', name='CORS settings', exact=True).click()
    page.locator('#cors-form:not([hidden])').wait_for()
    old_cors = page.locator('#info').text_content()
    page.get_by_role('button', name='Fill Wasabi-style preset', exact=True).click()
    assert page.locator('#cors-rules [name="origins"]').input_value() == '*'
    page.locator('#language').select_option('zh-CN')
    assert page.locator('#cors-rules [name="max_age"]').input_value() == '86400'
    page.get_by_role('button', name='保存', exact=True).click()
    page.locator('#notice').filter(has_text='CORS 设置已保存').wait_for()
    page.screenshot(path=str(results / 'web-cors-zh-CN.png'), full_page=True)
    page.reload()
    page.locator('#browser:not([hidden])').wait_for()
    page.locator('#buckets').select_option(label=bucket)
    page.get_by_role('button', name='CORS 设置', exact=True).click()
    page.locator('#cors-rules [name="origins"]').wait_for()
    assert page.locator('#cors-rules [name="origins"]').input_value() == '*'
    page.get_by_role('button', name='添加规则', exact=True).click()
    assert page.locator('#cors-rules > fieldset').count() == 2
    page.get_by_role('button', name='删除规则', exact=True).last.click()
    page.locator('#language').select_option('en')
    page.screenshot(path=str(results / 'web-cors-en.png'), full_page=True)
    page.get_by_role('button', name='Clear rules', exact=True).click()
    page.get_by_role('button', name='Save', exact=True).click()
    page.locator('#notice').filter(has_text='CORS settings saved').wait_for()
    page.wait_for_function('() => document.querySelector("#info").textContent === "[]"')
    assert page.evaluate("""async (rules) => {
        const session = await fetch('/api/session').then(r => r.json());
        return (await fetch('/api/buckets/' + document.getElementById('buckets').value + '/cors', {
            method: 'PUT', headers: {'Content-Type': 'application/json', 'X-CSRF-Token': session.csrf_token}, body: rules,
        })).status;
    }""", old_cors) == 200
    page.get_by_role('button', name='Website settings', exact=True).click()
    page.get_by_label('Index filename', exact=True).fill('home.html')
    page.get_by_label('Error document key', exact=True).fill('errors/404.html')
    page.get_by_label('Enable website routing', exact=True).check()
    page.locator('#language').select_option('zh-CN')
    assert page.get_by_label('首页文件名', exact=True).input_value() == 'home.html'
    page.get_by_role('button', name='保存', exact=True).click()
    page.locator('#notice').filter(has_text='网站设置已保存').wait_for()
    page.screenshot(path=str(results / 'web-website-zh-CN.png'), full_page=True)
    page.reload()
    page.locator('#browser:not([hidden])').wait_for()
    page.locator('#buckets').select_option(label='test-media')
    page.get_by_role('button', name='网站设置', exact=True).click()
    page.get_by_label('首页文件名', exact=True).wait_for()
    assert page.get_by_label('首页文件名', exact=True).input_value() == 'home.html'
    assert page.get_by_label('启用网站路由', exact=True).is_checked()
    page.locator('#language').select_option('en')
    page.screenshot(path=str(results / 'web-website-en.png'), full_page=True)
    page.get_by_label('Enable website routing', exact=True).uncheck()
    page.get_by_label('Index filename', exact=True).fill('index.html')
    page.get_by_label('Error document key', exact=True).fill('404.html')
    page.get_by_role('button', name='Save', exact=True).click()
    page.locator('#notice').filter(has_text='Website settings saved').wait_for()
    page.get_by_role('button', name='multipart', exact=True).click()
    page.locator('#facts').filter(has_text='Object key').wait_for()
    assert page.get_by_role('link', name='Download original').count() == 1
    page.locator('#language').select_option('zh-CN')
    assert page.get_by_role('link', name='下载原文件').count() == 1
    assert 'multipart' in page.locator('#facts').inner_text()
    page.locator('#language').select_option('en')
    page.get_by_role('button', name='Refresh', exact=True).click()
    page.locator('#files button').filter(has_text='web-accept/').click()
    page.wait_for_function('() => document.querySelectorAll("#files tr").length === 100')
    page.locator('#more').click()
    page.wait_for_function('() => document.querySelectorAll("#files tr").length === 125')
    assert page.locator('#more').is_hidden()
    page.get_by_role('button', name=injection, exact=True).click()
    page.locator('#facts').filter(has_text=injection).wait_for()
    assert not page.evaluate('Boolean(window.injected)')
    assert page.locator('#info script, #facts img').count() == 0
    page.get_by_role('button', name='image.png', exact=True).click()
    page.get_by_role('button', name='Preview', exact=True).click()
    page.wait_for_function('() => document.querySelector("#preview img")?.naturalWidth === 640')
    page.screenshot(path=str(results / 'web-private-image-en.png'), full_page=True)
    page.get_by_role('button', name='video.mp4', exact=True).click()
    page.get_by_role('button', name='Preview', exact=True).click()
    page.wait_for_function('() => document.querySelector("#preview video")?.readyState >= 2')
    page.locator('#preview video').evaluate('v => { v.currentTime = 5; }')
    page.wait_for_function('() => document.querySelector("#preview video").currentTime >= 5 && !document.querySelector("#preview video").seeking')
    page.get_by_role('button', name='unsafe.html', exact=True).click()
    page.locator('#facts').filter(has_text='text/html').wait_for()
    assert page.get_by_role('button', name='Preview', exact=True).count() == 0
    bucket_id = page.locator('#buckets').input_value()
    active = page.request.get(url + '/api/download?' + urlencode({'bucket': bucket_id, 'key': 'web-accept/unsafe.html', 'preview': 'true'}))
    assert active.ok and active.headers['content-type'] == 'application/octet-stream'
    assert active.headers['content-disposition'] == 'attachment'
    assert 'sandbox' in active.headers['content-security-policy']
    assert active.headers['cache-control'] == 'private, no-store'
    with page.expect_download() as download:
        page.get_by_role('link', name='Download original', exact=True).click()
    assert Path(download.value.path()).read_bytes() == b'<script>window.injected=true</script>'
    assert not page.evaluate('Boolean(window.injected)')
    page.locator('#language').select_option('zh-CN')
    # A fresh tab has cookies but no per-tab CSRF value; /api/session must supply it.
    new_tab = context.new_page()
    new_tab.goto(url)
    new_tab.locator('#browser:not([hidden])').wait_for()
    new_tab.get_by_role('button', name='退出登录', exact=True).click()
    new_tab.get_by_role('heading', name='管理登录').wait_for()
    page.reload()
    page.locator('#language').select_option('en')
    page.set_viewport_size({'width': 390, 'height': 844})
    page.screenshot(path=str(results / 'web-mobile-en.png'), full_page=True)
    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
    assert not errors, errors
    context.close()
    chinese = browser.new_context(locale='zh-HK')
    translated = chinese.new_page()
    translated.goto(url)
    translated.locator('html[lang="zh-CN"]').wait_for()
    translated.get_by_role('heading', name='管理登录').wait_for()
    chinese.close()
    browser.close()
print('PASS en/zh-CN, pagination, private image/video seeking/download, injection isolation, session tab and mobile layout')
