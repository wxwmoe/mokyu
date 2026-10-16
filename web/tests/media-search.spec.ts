import { readFileSync } from 'node:fs'
import { randomUUID, createHash } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('search, type chips, sorted results, styled date picker and URL restoration', async ({ page, request }) => {
  test.setTimeout(90000)
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const bucket = (await (await request.get('/api/v1/buckets')).json()).find((b: any) => b.name === 'media')
  const prefix = 'ui-search-' + randomUUID().slice(0, 8) + '/'
  const files: [string, number, string][] = [['heart.png', 8, 'image/png'], ['star.png', 12, 'image/png'], ['memory.txt', 15, 'text/plain'], ['video.mp4', 70, 'video/mp4'], ['folder/note.txt', 4, 'text/plain']]
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  page.on('request', request => { if (new URL(request.url()).pathname.endsWith('/catalog')) errors.push('obsolete catalog progress request') })
  page.on('console', message => { if (message.type() === 'error' && /Content Security Policy|Refused to/i.test(message.text())) errors.push(message.text()) })
  try {
    for (const [name, size, mime] of files) {
      const reply = await request.post(`/api/v1/buckets/${bucket.id}/uploads`, { headers, data: { client_id: randomUUID(), key: prefix + name, file_name: name, size: String(size), content_type: mime } })
      expect(reply.status()).toBe(201)
      const upload = await reply.json(), data = Buffer.alloc(size, 45)
      const part = await request.put(`/api/v1/uploads/${upload.id}/parts/1`, { headers: { ...headers, 'X-Content-SHA256': createHash('sha256').update(data).digest('base64') }, data })
      expect(part.ok()).toBe(true)
      expect((await request.post(`/api/v1/uploads/${upload.id}/complete`, { headers, data: { parts: [await part.json()] } })).ok()).toBe(true)
    }
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/overview$/); await page.goto('/media')
    await page.goto('/media/' + bucket.id + '?' + new URLSearchParams({ prefix }))
    await expect(page.getByRole('button', { name: 'folder/', exact: true })).toBeVisible()
    await page.getByRole('searchbox', { name: en.mediaSearch, exact: true }).fill('heart')
    await page.getByRole('button', { name: en.search, exact: true }).click()
    await expect(page.getByRole('button', { name: 'heart.png', exact: true })).toBeVisible()
    await expect(page.getByRole('button', { name: 'star.png', exact: true })).not.toBeVisible()
    await page.reload()
    await expect(page.getByRole('searchbox', { name: en.mediaSearch, exact: true })).toHaveValue('heart')
    await page.getByRole('button', { name: en.clearFilters, exact: true }).click()
    await page.getByRole('button', { name: en.category_image, exact: true }).click()
    await expect(page.locator('tbody tr')).toHaveCount(2)
    await page.getByRole('combobox', { name: en.mediaSort, exact: true }).click()
    await page.getByRole('option', { name: en.sortLargest, exact: true }).click()
    await expect(page.locator('tbody tr').first()).toContainText('star.png')
    await page.getByRole('button', { name: en.moreFilters, exact: true }).click()
    const dialog = page.getByRole('dialog', { name: en.moreFilters, exact: true })
    await dialog.getByRole('textbox', { name: en.minimumSize, exact: true }).fill('9')
    await dialog.getByRole('button', { name: en.chooseDate.replace('{label}', en.modifiedFrom), exact: true }).click()
    const calendar = page.locator('.calendar-popover')
    await expect(calendar).toBeVisible()
    await expect(calendar).toHaveCSS('border-radius', '15px')
    await page.screenshot({ path: '/results/step11-calendar.png', fullPage: true })
    await calendar.locator('[data-today][role=button]').click()
    await expect(calendar).not.toBeVisible()
    await dialog.getByRole('button', { name: en.applyFilters, exact: true }).click()
    await expect(page.locator('tbody tr')).toHaveCount(1)
    await expect(page.locator('tbody tr')).toContainText('star.png')
    await page.reload()
    await expect(page.locator('tbody tr')).toHaveCount(1)
    await page.screenshot({ path: '/results/step11-media.png', fullPage: true })
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    expect(await page.locator('select').count()).toBe(0)
    expect(errors).toEqual([])
  } finally {
    const result = await (await request.get(`/api/v1/buckets/${bucket.id}/objects?` + new URLSearchParams({ prefix, recursive: 'true' }))).json()
    if (result.items?.length) {
      const cleanup = await request.post('/api/v1/media/actions', { headers, data: { bucket: bucket.id, action: 'delete', objects: result.items.map((o: any) => ({ client_id: randomUUID(), key: o.object_key, version: o.id })) } })
      expect(cleanup.ok()).toBe(true)
    }
  }
})
