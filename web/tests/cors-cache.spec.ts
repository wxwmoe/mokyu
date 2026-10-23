import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('CORS runtime limits remain draft values until explicitly saved', async ({ page }, info) => {
  const request = page.context().request
  const origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.status()).toBe(200)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const url = '/api/v1/runtime-settings/requests'
  const original = await (await request.get(url)).json()
  expect(original.value.cors_cache_size).toBe('1MiB')
  expect(original.value.cors_cache_entries).toBe(256)
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
    for (const theme of ['light', 'dark']) {
      await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })
      await page.goto('/runtime-config')
      await expect(page.locator('.cluster-scopes')).toHaveCount(0)
      await page.locator('.runtime-section-list article').filter({ hasText: text.runtimeGroupRequests }).getByRole('button').click()
      const editor = page.getByRole('dialog', { name: text.runtimeGroupRequests, exact: true })
      await editor.locator('[data-runtime-field="requests.cors_cache_size"] input').fill('2MiB')
      await editor.locator('[data-runtime-field="requests.cors_cache_entries"] input').fill('512')
      expect(await (await request.get(url)).json()).toEqual(original)
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 1000 })
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      }
      await editor.screenshot({ path: info.outputPath(`cors-${locale}-${theme}.png`) })
      await editor.getByRole('button', { name: text.cancel, exact: true }).click()
    }
  }
  expect((await request.put(url, { headers, data: { revision: original.revision, value: { ...original.value, cors_cache_entries: 0 } } })).status()).toBe(400)
  expect((await request.put(url, { headers, data: { revision: original.revision, value: { ...original.value, cors_cache_size: '1TiB' } } })).status()).toBe(400)
  expect(await (await request.get(url)).json()).toEqual(original)
  expect(errors).toEqual([])
})
