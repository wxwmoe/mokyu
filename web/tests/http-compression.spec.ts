import { expect, test } from '@playwright/test'

test('static variants survive browser revalidation and coding changes', async ({ page, context, baseURL }) => {
  const network = await context.newCDPSession(page)
  await network.send('Network.enable')
  const responses: { url: string, status: number, encoding: string, etag: string }[] = []
  const requests = new Map<string, string>()
  network.on('Network.requestWillBeSent', event => requests.set(event.requestId, event.request.url))
  network.on('Network.responseReceivedExtraInfo', event => {
    const headers = Object.fromEntries(Object.entries(event.headers).map(([key, value]) => [key.toLowerCase(), String(value)]))
    responses.push({ url: requests.get(event.requestId) || '', status: event.statusCode,
      encoding: headers['content-encoding'] || 'identity', etag: headers.etag || '' })
  })
  await page.goto('/')
  const path = await page.locator('script[src^="/assets/"]').first().getAttribute('src')
  expect(path).toBeTruthy()
  const target = new URL(path!, baseURL).href + '?compression-test'
  let original = ''
  for (const encoding of ['zstd', 'br', 'gzip', 'identity', 'zstd', 'identity']) {
    await network.send('Network.setExtraHTTPHeaders', { headers: { 'Accept-Encoding': encoding } })
    for (const cache of ['no-cache', 'reload', 'no-cache'] as const) {
      const result = await page.evaluate(async ({ url, cache }) => {
        const response = await fetch(url, { cache })
        return { status: response.status, text: await response.text(), etag: response.headers.get('etag'), encoding: response.headers.get('content-encoding') }
      }, { url: target, cache })
      expect(result.status).toBe(200)
      if (!original) original = result.text
      expect(result.text).toBe(original)
      const wire = responses.filter(response => response.url === target).at(-1)
      // A 304 can reuse the earlier representation; its decoded bytes and retained validator must remain consistent.
      if (cache === 'reload' || wire?.status === 200) expect(result.encoding || 'identity').toBe(encoding)
      expect(result.etag?.startsWith('W/')).toBe((result.encoding || 'identity') !== 'identity')
    }
  }
  const observed = responses.filter(response => response.url === target)
  console.log(JSON.stringify(observed))
  expect(observed.some(response => response.status === 304)).toBeTruthy()
  expect(observed.some(response => response.status === 304 && !response.etag.startsWith('W/'))).toBeTruthy()
  expect(observed.some(response => response.status === 200 && response.encoding === 'identity')).toBeTruthy()
})
