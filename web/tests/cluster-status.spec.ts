import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'

test('cluster views keep node identity and read-only configuration across locales', async ({ page }) => {
  const request = page.context().request, origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const nodes = (await (await request.get('/api/v1/cluster/nodes')).json()).items
  expect(nodes.length).toBeGreaterThanOrEqual(4)
  const target = nodes.find((node: { role: string }) => node.role === 'data')
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  if (process.env.MOKYU_TEST_SECOND_CONTROL) {
    let next = false
    await page.route('**/api/v1/**', async route => {
      if (route.request().method() !== 'GET') return route.continue()
      next = !next
      const url = new URL(route.request().url())
      const backend = next ? process.env.MOKYU_TEST_SECOND_CONTROL! : origin
      const response = await route.fetch({ url: backend + url.pathname + url.search, headers: route.request().headers() })
      await route.fulfill({ response })
    })
  }
  for (const locale of ['en', 'zh-CN', 'ja']) {
    for (const theme of ['light', 'dark']) {
      expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
      await page.setViewportSize({ width: 1440, height: 1040 })
      await page.goto('/service')
      await expect(page.locator('.node-row')).toHaveCount(nodes.length)
      for (const width of [1440, 1024, 768, 640, 480, 390, 320]) {
        await page.setViewportSize({ width, height: 1040 })
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      }
      await page.setViewportSize({ width: 1440, height: 1040 })
      await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/cluster-${locale}-${theme}.png`, fullPage: true })
      await page.locator('.node-row').filter({ hasText: target.name }).click()
      await expect(page.getByRole('dialog')).toBeVisible()
      await expect(page.getByRole('dialog').locator('.technical-key')).toHaveText(target.id)
      await page.setViewportSize({ width: 390, height: 844 })
      await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/node-${locale}-${theme}.png` })
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      await page.keyboard.press('Escape')
      await expect(page.getByRole('dialog')).toHaveCount(0)
      await page.goto('/runtime-config')
      await expect(page.locator('.cluster-scopes .tab-trigger')).toHaveCount(2)
      for (const tab of await page.locator('.cluster-scopes .tab-trigger').all()) {
        await expect(tab).toHaveCSS('text-decoration-line', 'none')
        await tab.hover()
        await expect(tab).toHaveCSS('text-decoration-line', 'none')
        await tab.focus()
        await expect(tab).toBeFocused()
      }
      await expect(page.locator('.runtime-section-list article')).toHaveCount(4)
      await expect(page.locator('.config-origin-badge[data-origin="config"]')).toHaveCount(0)
      const draft = page.locator('[data-runtime-field="web.session_lifetime"] input')
      await draft.fill('11h')
      await page.locator('.cluster-scopes .tab-trigger').nth(1).click()
      await expect(page.locator('.node-picker')).toBeVisible()
      await page.locator('.cluster-scopes .tab-trigger').first().click()
      await expect(draft).toHaveValue('11h')
      await page.goto(`/runtime-config?view=node&node=${target.id}&section=database`)
      await expect(page.locator('.node-config-context .technical-key')).toHaveText(target.id)
      await expect(page.locator('.config-edit-button')).toHaveCount(0)
      await expect(page.locator('.runtime-form, .runtime-section-list')).toHaveCount(0)
      await expect(page.locator('.config-origin-badge[data-origin="environment"]')).toHaveCount(3)
      await expect(page.locator('.config-value-mode-badge[data-mode="read"]').first()).toHaveCSS('color', theme === 'light' ? 'rgb(75, 85, 99)' : 'rgb(203, 213, 225)')
      if (locale !== 'en') {
        const hints = await page.locator('.config-copy .field-help').allTextContents()
        expect(hints.every(hint => !hint.includes('\u3002'))).toBe(true)
      }
      await page.reload()
      await expect(page.locator('.node-config-context .technical-key')).toHaveText(target.id)
      await page.setViewportSize({ width: 320, height: 844 })
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      await page.setViewportSize({ width: 1440, height: 1040 })
      await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/config-${locale}-${theme}.png`, fullPage: true })
      for (const path of ['/overview', '/storage']) {
        const response = page.waitForResponse(r => new URL(r.url()).pathname === '/api/v1/usage/runtime')
        await page.goto(path)
        const runtime = (await (await response).json()).cluster
        await expect(page.locator('.runtime-coverage strong')).toBeVisible()
        const partial = runtime.stale || runtime.current.reported_nodes < runtime.current.expected_nodes || runtime.current.rate_nodes < runtime.current.expected_nodes
        if (partial) await expect(page.locator('.runtime-coverage')).toHaveClass(/is-partial/)
        else await expect(page.locator('.runtime-coverage')).not.toHaveClass(/is-partial/)
        await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}${path}-${locale}-${theme}.png`, fullPage: true })
        await page.setViewportSize({ width: 320, height: 844 })
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
        await page.setViewportSize({ width: 1440, height: 1040 })
      }
    }
  }
  expect(errors).toEqual([])
})
