import { readFileSync } from 'node:fs'
import { expect, test } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

for (const type of ['local', 's3']) test('save journal settings for ' + type, async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const origin = process.env.MOKYU_TEST_WEB!
  const response = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  const headers = { Origin: origin, 'X-CSRF-Token': (await response.json()).csrf_token }
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const stores = await (await request.get('/api/v1/storages')).json()
  const storage = stores.find((item: { type: string }) => item.type === type)
  test.skip(!storage, 'requires a ' + type + ' storage')
  await request.post('/api/v1/account/reauth', { headers, data: { password } })
  const path = '/api/v1/storages/' + encodeURIComponent(storage.name) + '/journal'
  const previous = await (await request.get(path)).json()
  try {
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/overview$/)
    await page.goto('/storages/' + encodeURIComponent(storage.name))
    await page.getByRole('button', { name: en.journalSettings, exact: true }).click()
    const dialog = page.getByRole('dialog', { name: en.journalSettings, exact: true })
    if (type === 'local') {
      await expect(dialog.getByRole('combobox')).toHaveCount(0)
      await expect(dialog.getByRole('spinbutton')).toHaveCount(3)
      await dialog.getByRole('spinbutton', { name: en.journalSegmentLimit, exact: true }).fill('0.0625')
    } else {
      await dialog.getByRole('combobox', { name: en.journalWriteMode, exact: true }).click()
      await page.getByRole('option', { name: en.journalPut, exact: true }).click()
      await dialog.getByRole('spinbutton', { name: en.journalPutTarget, exact: true }).fill('4')
      await dialog.getByRole('spinbutton', { name: en.journalWait, exact: true }).fill('2')
    }
    const saving = page.waitForRequest(r => r.method() === 'PUT' && new URL(r.url()).pathname === path)
    await dialog.getByRole('button', { name: en.save, exact: true }).click()
    const sent = (await saving).postDataJSON().policy
    if (type === 'local') expect(Object.keys(sent).sort()).toEqual(['segment_bytes', 'snapshot_page_bytes', 'snapshot_trigger_bytes'])
    else expect(sent.mode).toBe('put')
    await expect(dialog).not.toBeVisible()
    if (type === 'local') {
      await expect.poll(async () => (await (await request.get(path)).json()).policy.segment_bytes).toBe(65536)
    } else {
      await expect.poll(async () => (await (await request.get(path)).json()).policy.put_target_bytes).toBe(4096)
      expect((await (await request.get(path)).json()).policy.put_max_wait_seconds).toBe(2)
    }
  } finally {
    const current = await (await request.get(path)).json()
    const restored = await request.put(path, { headers, data: { revision: current.revision, policy: previous.policy } })
    expect(restored.ok()).toBeTruthy()
  }
})
