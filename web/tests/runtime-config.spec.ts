import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'

test('single runtime observations preserve the configuration and service layout', async ({ page }) => {
  const request = page.context().request
  const origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const reply = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(reply.status()).toBe(200)
  const headers = { Origin: origin, 'X-CSRF-Token': (await reply.json()).csrf_token }
  const errors: string[] = [], clusterRequests: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  page.on('request', request => { if (new URL(request.url()).pathname.includes('/cluster/')) clusterRequests.push(request.url()) })
  const status = await (await request.get('/api/v1/system/status')).json()
  expect(status.deployment_mode).toBe('single')
  expect(status.current_node).toBeNull()
  for (const locale of ['en', 'zh-CN', 'ja']) {
    for (const theme of ['light', 'dark']) {
      expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
      await page.goto('/runtime-config?node=invalid&view=node')
      await expect(page.locator('.config-board')).toHaveCount(3)
      await expect(page.locator('.runtime-section-list article')).toHaveCount(4)
      await expect(page.locator('.config-origin-badge[data-origin="environment"]')).toHaveCount(10)
      const version = page.locator('.config-fields article').filter({ has: page.locator('code', { hasText: /^database\.version$/ }) })
      await expect(version.locator('.config-value-mode-badge')).toHaveAttribute('data-mode', 'read')
      await expect(version.locator('button')).toHaveCount(0)
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 1040 })
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      }
      await page.setViewportSize({ width: 1440, height: 1040 })
      await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/runtime-config-${locale}-${theme}.png`, fullPage: true })
      await page.goto('/service')
      await expect(page.locator('.config-board')).toHaveCount(0)
      await expect(page.getByRole('combobox')).toHaveCount(0)
    }
  }
  expect(clusterRequests).toEqual([])
  expect(errors).toEqual([])
})
