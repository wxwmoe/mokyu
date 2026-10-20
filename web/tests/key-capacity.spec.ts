import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('key capacity is accessible, localized and independent of cluster navigation', async ({ page }) => {
  test.setTimeout(120_000)
  const request = page.context().request, origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const created = await request.post('/api/v1/keys', { headers, data: { name: 'capacity-' + randomUUID().slice(0, 8), purpose: 'block' } })
  expect(created.status()).toBe(201)
  const key = await created.json()
  const real = await (await request.get('/api/v1/keys')).json()
  expect(real.find((row: { id: string }) => row.id === key.id).nonce_capacity.remaining).toBe('100')
  const state = await (await request.get('/api/v1/system/status')).json()
  expect(state.deployment_mode).toBe('single')
  expect(Array.isArray(state.nonce_alerts)).toBe(true)
  const observed = new Date().toISOString(), errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  // Visual states use read-only fixtures; the real API contract above remains exercised.
  const keys = [
    { state: 'normal', remaining: '100', tone: 'mint', reason: null, days: null },
    { state: 'warning', remaining: '98', tone: 'rose', reason: 'forecast', days: 45 },
    { state: 'normal', remaining: '41', tone: 'mint', reason: null, days: null },
    { state: 'normal', remaining: '40', tone: 'cream', reason: null, days: null },
    { state: 'normal', remaining: '21', tone: 'cream', reason: null, days: null },
    { state: 'normal', remaining: '20', tone: 'rose', reason: null, days: null },
    { state: 'exhausted', remaining: '0', tone: 'rose', reason: 'exhausted', days: null },
    { state: 'unknown', remaining: null, tone: 'gray', reason: 'counter', days: null },
  ].map((value, index) => ({ ...key, id: randomUUID(), name: `capacity-${index}`, in_use: index % 2 === 1, nonce_capacity: {
    key_id: key.id, name: `capacity-${index}`, storage_count: 1, pending: false, state: value.state,
    remaining: value.remaining, sampled_at: observed, stale: value.state === 'unknown',
    counters: [0, 1].map(type_tag => ({ type_tag, reserved_id: '9223372036854775807', available: '64', remaining: value.remaining, estimated_days: value.days, observed_seconds: 300, reason: value.reason })),
  }, tone: value.tone }))
  const revoked = { ...key, id: randomUUID(), name: 'revoked-key', revoked_at: observed }
  await page.route('**/api/v1/keys', route => route.fulfill({ json: [...keys, revoked] }))
  await page.route('**/api/v1/system/status', route => route.fulfill({ json: { ...state, nonce_alerts: keys.slice(1).map(row => row.nonce_capacity) } }))
  try {
    for (const [locale, messages] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
      for (const theme of ['light', 'dark']) {
        await page.setViewportSize({ width: 1440, height: 1100 })
        expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
        await page.goto('/credentials?tab=managed')
        await expect(page.getByText(messages.nonceBannerTitle, { exact: true })).toBeVisible()
        await page.evaluate(() => document.fonts.ready)
        const revokedRow = page.locator('.managed-key-row').filter({ hasText: revoked.name })
        await expect(revokedRow.locator('.managed-key-state .badge')).toHaveAttribute('data-tone', 'gray')
        await expect(revokedRow.getByText(messages.keyRevoked, { exact: true })).toBeVisible()
        for (const item of keys) {
          const row = page.locator('.managed-key-row').filter({ hasText: item.name })
          await expect(row.locator('.managed-key-state > .badge').first()).toHaveAttribute('data-tone', item.in_use ? 'sky' : 'mint')
          const badge = row.locator('.node-state[tabindex]')
          await expect(badge).toHaveAttribute('data-tone', item.tone)
          expect(await badge.evaluate(el => el.previousElementSibling?.classList.contains('badge'))).toBe(true)
          if (item.nonce_capacity.remaining != null) await expect(badge).toContainText(/\b(?:100|[0-9]{1,2})%/)
          await badge.scrollIntoViewIfNeeded()
          await page.evaluate(() => new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))))
          await badge.focus()
          await expect(badge).toBeFocused()
          await expect(badge).toHaveAttribute('aria-describedby', /.+/)
          await expect(page.locator('.tooltip'), item.name).toContainText(messages.nonceCapacityHint)
          await page.keyboard.press('Escape')
          await expect(page.locator('.tooltip')).toHaveCount(0)
        }
        for (const width of [1440, 1024, 768, 640, 390, 320]) {
          await page.setViewportSize({ width, height: 1000 })
          expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
          if (width === 1440 || width === 320) await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/capacity-${locale}-${theme}-${width}.png`, fullPage: true })
        }
      }
    }
    expect(errors).toEqual([])
  } finally {
    expect((await request.delete('/api/v1/keys/' + key.id, { headers })).status()).toBe(204)
  }
})
