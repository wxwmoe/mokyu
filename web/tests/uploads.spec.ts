import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('bounded browser multipart, pause, reload, full accepted-part validation, completion and cancellation', async ({ page, request }) => {
  test.setTimeout(150000)
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const login = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const bucket = (await (await request.get('/api/buckets')).json()).find((b: any) => b.name === 'media')
  const filename = 'ui-upload-' + randomUUID().slice(0, 8) + '.bin'
  const buffer = Buffer.alloc(32 * 1024 * 1024 + 73, 61)
  const payload = { name: filename, mimeType: 'application/octet-stream', buffer }
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  page.on('console', message => { if (message.type() === 'error' && /Content Security Policy|Refused to/i.test(message.text())) errors.push(message.text()) })
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/media$/)
  await page.goto('/media/' + bucket.id)
  await page.getByRole('button', { name: en.uploadTitle, exact: true }).click()
  const dialog = page.getByRole('dialog', { name: en.uploadTitle, exact: true })
  await dialog.locator('input[type=file]').setInputFiles(payload)
  let release!: () => void, entered!: () => void, count = 0
  const held = new Promise<void>(resolve => { release = resolve })
  const both = new Promise<void>(resolve => { entered = resolve })
  await page.route('**/api/uploads/*/parts/*', async route => { if (++count === 2) entered(); await held; await route.continue() })
  await dialog.getByRole('button', { name: en.startUpload, exact: true }).click()
  await both
  const row = page.locator('.transfer-row').filter({ hasText: filename })
  await row.getByRole('button', { name: en.pauseUpload, exact: true }).click()
  release()
  await expect(row).toHaveAttribute('data-state', 'paused', { timeout: 30000 })
  expect(count).toBe(2)
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.screenshot({ path: '/results/step10-transfers.png', fullPage: true })
  await page.unroute('**/api/uploads/*/parts/*')
  const transfer = (await (await request.get('/api/uploads?bucket=' + bucket.id)).json()).uploads.find((t: any) => t.object_key === filename)
  expect(transfer.received_bytes).toBe(String(32 * 1024 * 1024))
  await page.reload()
  await expect(row.getByRole('button', { name: en.reselectFile, exact: true })).toBeVisible()
  const wrong = Buffer.from(buffer); wrong[0] = 62
  const wrongChooser = page.waitForEvent('filechooser')
  await row.getByRole('button', { name: en.reselectFile, exact: true }).click()
  await (await wrongChooser).setFiles({ ...payload, buffer: wrong })
  await expect(row.getByRole('alert')).toHaveText(en.error_ResumeMismatch)
  const chooser = page.waitForEvent('filechooser')
  await row.getByRole('button', { name: en.reselectFile, exact: true }).click()
  await (await chooser).setFiles(payload)
  await expect(row).not.toBeVisible({ timeout: 30000 })
  const saved = await (await request.get('/api/uploads/' + transfer.id)).json()
  expect([saved.state, saved.remote_state]).toEqual(['completed', 'stored'])
  const download = await request.get('/api/download?' + new URLSearchParams({ bucket: bucket.id, key: filename }))
  expect((await download.body()).equals(buffer)).toBe(true)
  const incomplete = await (await request.post(`/api/buckets/${bucket.id}/uploads`, { headers, data: { client_id: randomUUID(), key: filename + '-cancel', file_name: filename + '-cancel', size: '1' } })).json()
  await page.getByRole('button', { name: en.refresh, exact: true }).click()
  const cancelled = page.locator('.transfer-row').filter({ hasText: filename + '-cancel' })
  await cancelled.getByRole('button', { name: en.cancelUpload, exact: true }).click()
  await page.getByRole('dialog', { name: en.cancelUpload }).getByRole('button', { name: en.cancelUpload, exact: true }).click()
  await expect(cancelled).not.toBeVisible()
  expect((await (await request.get('/api/uploads/' + incomplete.id)).json()).state).toBe('aborted')
  expect(errors).toEqual([])
})
