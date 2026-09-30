import { createHash, randomUUID } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('gallery thumbnails, compact and expanded viewer, safe text and versioned deep link', async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const reply = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await reply.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const bucket = (await (await request.get('/api/buckets')).json()).find((b: any) => b.name === 'media')
  const prefix = 'ui-preview-' + randomUUID().slice(0, 8) + '/'
  const picture = readFileSync('public/assets/apple-touch-icon.png')
  const files: [string, string, Buffer][] = [['mochi.png', 'image/png', picture], ['readme.txt', 'text/plain', Buffer.from('<script>document.body.remove()</script>\nA little room for love.')], ['motion.mp4', 'video/mp4', Buffer.alloc(100)]]
  const errors: string[] = [], requests: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  page.on('console', message => { if (message.type() === 'error' && /Content Security Policy|Refused to/i.test(message.text())) errors.push(message.text()) })
  page.on('request', request => requests.push(request.url()))
  try {
    for (const [name, mime, data] of files) {
      const created = await request.post(`/api/buckets/${bucket.id}/uploads`, { headers, data: { client_id: randomUUID(), key: prefix + name, file_name: name, size: String(data.length), content_type: mime } })
      expect(created.status()).toBe(201)
      const upload = await created.json()
      const part = await request.put(`/api/uploads/${upload.id}/parts/1`, { headers: { ...headers, 'X-Content-SHA256': createHash('sha256').update(data).digest('base64') }, data })
      expect(part.ok()).toBe(true)
      expect((await request.post(`/api/uploads/${upload.id}/complete`, { headers, data: { parts: [await part.json()] } })).ok()).toBe(true)
    }
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/media$/)
    const location = '/media/' + bucket.id + '?' + new URLSearchParams({ prefix })
    await page.goto(location)
    await page.getByRole('button', { name: en.galleryView, exact: true }).click()
    const mochi = page.locator('.gallery-card').filter({ hasText: 'mochi.png' })
    await expect(mochi.locator('img')).toBeVisible()
    expect(requests.filter(url => url.includes('/object/content'))).toEqual([])
    await mochi.click()
    const viewer = page.getByRole('dialog', { name: 'mochi.png', exact: true })
    await expect(viewer).toBeVisible()
    await expect(viewer.getByRole('button', { name: new RegExp(en.loadOriginal) })).toBeVisible()
    expect(await viewer.locator('.object-viewer-body').evaluate(el => el.scrollHeight - el.clientHeight)).toBeLessThan(4)
    await page.screenshot({ path: '/results/step12-object-light.png', fullPage: true })
    const download = viewer.getByRole('link', { name: en.download, exact: true })
    const content = await request.get((await download.getAttribute('href'))!)
    expect(content.ok()).toBe(true); expect(await content.body()).toEqual(picture)
    await viewer.getByRole('button', { name: en.expandView, exact: true }).click()
    await expect(viewer.locator('.original-image')).toBeVisible()
    await expect.poll(() => viewer.locator('.original-image').evaluate((img: HTMLImageElement) => img.naturalWidth)).toBeGreaterThan(0)
    await viewer.getByRole('button', { name: en.hideInspector, exact: true }).click()
    await expect(viewer.locator('.object-inspector')).not.toBeVisible()
    await viewer.getByRole('button', { name: en.showInspector, exact: true }).click()
    await page.screenshot({ path: '/results/step12-object-wide.png', fullPage: true })
    await page.keyboard.press('Escape')
    await expect(mochi).toBeFocused()
    await page.locator('.gallery-card').filter({ hasText: 'readme.txt' }).click()
    await expect(page.locator('.text-preview pre')).toHaveText('<script>document.body.remove()</script>\nA little room for love.')
    await expect(page.locator('body')).toContainText('Mokyu')
    await page.keyboard.press('Escape')
    const catalog = await (await request.get(`/api/buckets/${bucket.id}/objects?` + new URLSearchParams({ q: prefix + 'mochi.png', mode: 'exact' }))).json()
    await page.goto(location + '&' + new URLSearchParams({ object: prefix + 'mochi.png', version: catalog.objects[0].id }))
    await expect(page.getByRole('dialog', { name: 'mochi.png', exact: true })).toBeVisible()
    await page.keyboard.press('Escape')
    await page.getByRole('button', { name: en.darkSwitch, exact: true }).click()
    await page.setViewportSize({ width: 390, height: 844 })
    await page.locator('.gallery-card').filter({ hasText: 'mochi.png' }).click()
    await expect(viewer).toBeVisible()
    await page.screenshot({ path: '/results/step12-object-mobile-dark.png', fullPage: true })
    const box = await viewer.boundingBox()
    expect(box!.x).toBeGreaterThanOrEqual(0); expect(box!.x + box!.width).toBeLessThanOrEqual(390)
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    expect(errors).toEqual([])
  } finally {
    const result = await (await request.get(`/api/buckets/${bucket.id}/objects?` + new URLSearchParams({ prefix, recursive: 'true' }))).json()
    if (result.objects?.length) await request.post('/api/objects/actions', { headers, data: { bucket: bucket.id, action: 'delete', objects: result.objects.map((o: any) => ({ key: o.object_key, version: o.id })) } })
  }
})
