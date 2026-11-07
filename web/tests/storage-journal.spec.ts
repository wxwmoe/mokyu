import { readFileSync } from 'node:fs'
import { expect, test } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('save a managed storage journal policy', async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const origin = process.env.MOKYU_TEST_WEB!
  const response = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  const headers = { Origin: origin, 'X-CSRF-Token': (await response.json()).csrf_token }
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const stores = await (await request.get('/api/v1/storages')).json()
  const storage = stores.find((item: { type: string }) => item.type === 's3')
  test.skip(!storage, 'requires a managed S3 storage')
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
    await dialog.getByRole('combobox', { name: en.journalWriteMode, exact: true }).click()
    await page.getByRole('option', { name: en.journalPut, exact: true }).click()
    await dialog.getByRole('spinbutton', { name: en.journalPutTarget, exact: true }).fill('4')
    await dialog.getByRole('spinbutton', { name: en.journalWait, exact: true }).fill('2')
    await dialog.getByRole('button', { name: en.save, exact: true }).click()
    await expect(dialog).not.toBeVisible()
    await expect.poll(async () => (await (await request.get(path)).json()).policy.put_target_bytes).toBe(4096)
    expect((await (await request.get(path)).json()).policy.put_max_wait_seconds).toBe(2)
  } finally {
    const current = await (await request.get(path)).json()
    const restored = await request.put(path, { headers, data: { revision: current.revision, policy: previous.policy } })
    expect(restored.ok()).toBeTruthy()
  }
})
