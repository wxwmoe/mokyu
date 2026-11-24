import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('edits and reopening discard in-flight recovery previews', async ({ page }) => {
  const api = page.context().request
  const origin = process.env.MOKYU_TEST_WEB!
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const auth = await api.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(auth.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await auth.json()).csrf_token }
  const profile = await (await api.get('/api/v1/account')).json()
  const preferences = { display_name: profile.display_name, theme: profile.theme, avatar_email: profile.avatar_email, avatar_enabled: profile.avatar_enabled }
  const buckets = await (await api.get('/api/v1/buckets')).json()
  const bucket = buckets.find((item: { name: string }) => item.name === 'media')
  const base = '/api/v1/buckets/' + bucket.id
  const dialog = page.getByRole('dialog', { name: en.timeTitle })
  const preview = dialog.getByRole('button', { name: en.timePreview })
  const apply = dialog.getByRole('button', { name: en.timeApply })
  const entry = page.locator('.time-window').getByRole('button', { name: en.timePreview })
  await page.route('**' + base + '/time-window', route => route.fulfill({ json: { seconds: 3600, latest_us: Date.now() * 1000, gaps: [] } }))
  try {
    expect((await api.put('/api/v1/account', { headers, data: { ...preferences, locale: 'en' } })).ok()).toBe(true)
    await page.goto('/buckets/' + bucket.id)
    await page.getByRole('tab', { name: en.versionTitle }).click()
    for (const change of ['prefix', 'method', 'reopen']) {
      let received!: () => void, release!: () => void
      const arrival = new Promise<void>(resolve => { received = resolve })
      const held = new Promise<void>(resolve => { release = resolve })
      const endpoint = '**' + base + '/time-restore'
      await page.route(endpoint, async route => {
        const prefix = route.request().postDataJSON().target_prefix
        received()
        await held
        await route.fulfill({ json: { id: 'old', phase: 'preview', digest: 'old', watermark: 1, counts: { add: 1, missing: 0 }, items: [{ key: 'a', target: prefix + 'a', state: 'add' }] } })
      })
      await entry.click()
      await dialog.getByRole('combobox', { name: en.timeMethod }).click()
      await page.getByRole('option', { name: en.timeDest_copy }).click()
      await dialog.getByLabel(en.timeScope_prefix, { exact: true }).fill('photos/')
      await dialog.getByLabel(en.timePrefix).fill('old/')
      await preview.click()
      await arrival
      const delivered = page.waitForResponse(response => response.url().endsWith('/time-restore'))
      try {
        if (change === 'prefix') await dialog.getByLabel(en.timePrefix).fill('new/')
        if (change === 'method') {
          await dialog.getByRole('combobox', { name: en.timeMethod }).click()
          await page.getByRole('option', { name: en.timeDest_in_place }).click()
        }
        if (change === 'reopen') { await page.keyboard.press('Escape'); await entry.click() }
      } finally { release() }
      await (await delivered).finished()
      await expect(preview).toBeEnabled()
      await expect(apply).toHaveCount(0)
      await expect(dialog.locator('.time-results')).toHaveCount(0)
      await page.unroute(endpoint)
      await page.keyboard.press('Escape')
    }
  } finally {
    await api.put('/api/v1/account', { headers, data: { ...preferences, locale: profile.locale } })
  }
})
