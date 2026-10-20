import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('native pack IDs remain strings and fit the storage UI', async ({ page, request }) => {
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const origin = process.env.MOKYU_TEST_WEB!
  const login = await request.post('/api/v1/auth/login', {
    headers: { Origin: origin }, data: { username: 'tester', password },
  })
  expect(login.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const preferences = await request.put('/api/v1/account', {
    headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false },
  })
  expect(preferences.ok()).toBe(true)
  const response = await request.get('/api/v1/storage/packs?limit=1')
  expect(response.ok()).toBe(true)
  const pack = (await response.json()).items[0]
  expect(pack.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/overview$/)
  await page.goto('/storage')
  await expect(page.locator('.pack-card').first()).toBeVisible()
  for (const width of [1440, 1024, 768, 640, 390, 320]) {
    await page.setViewportSize({ width, height: 900 })
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    expect(await page.locator('.pack-card > strong').first().evaluate(element =>
      element.scrollWidth <= element.clientWidth + 1)).toBe(true)
    if (width === 1440 || width === 390) {
      await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/file-identities-${width}.png`, fullPage: true })
    }
  }
  await page.setViewportSize({ width: 390, height: 900 })
  await page.goto('/storage?pack=' + pack.id)
  await expect(page.getByRole('dialog')).toBeVisible()
  await expect(page.getByRole('dialog')).toContainText(pack.id)
  await page.getByRole('tab', { name: en.referencingFiles, exact: true }).click()
  await page.keyboard.press('Escape')
  await expect(page.getByRole('dialog')).not.toBeVisible()
  expect(errors).toEqual([])
})
