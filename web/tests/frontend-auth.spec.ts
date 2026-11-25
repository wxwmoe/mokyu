import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

const password = () => (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()

test('refreshes a session and restores the storage page after reopening', async ({ browser, request }) => {
  const origin = process.env.MOKYU_TEST_WEB!
  const login = await request.post('/api/v1/auth/login', {
    headers: { Origin: origin },
    data: { username: 'tester', password: password() },
  })
  expect(login.status()).toBe(200)
  const loginBody = await login.json()
  expect(loginBody.refresh_token).toMatch(/^[0-9a-f]{64}$/)
  const cookies = login.headersArray().filter(header => header.name.toLowerCase() === 'set-cookie').map(header => header.value)
  expect(cookies.some(cookie => cookie.startsWith('mokyu_session=') && cookie.includes('Path=/api/v1;'))).toBe(true)
  expect(cookies.some(cookie => cookie.startsWith('mokyu_refresh=') && !cookie.includes('Max-Age=0'))).toBe(false)

  const refresh = await request.post('/api/v1/auth/refresh', { headers: { Origin: origin }, data: { refresh_token: loginBody.refresh_token } })
  expect(refresh.status()).toBe(200)
  const refreshBody = await refresh.json()
  expect(refreshBody.session_id).toMatch(/^[0-9a-f-]{36}$/)
  expect(refreshBody.refresh_token).toMatch(/^[0-9a-f]{64}$/)

  const context = await browser.newContext({ storageState: await request.storageState() })
  await context.addInitScript(token => localStorage.setItem('mokyu.auth.refresh', token), refreshBody.refresh_token)
  const page = await context.newPage()
  try {
    await page.goto('/storages')
    await expect(page.getByRole('heading', { name: en.storages, exact: true })).toBeVisible()
    await expect(page.locator('.store-card').filter({ hasText: 'demo' })).toBeVisible()
    const state = await context.storageState()
    await context.close()

    const reopened = await browser.newContext({ storageState: state })
    const reopenedPage = await reopened.newPage()
    try {
      await reopenedPage.goto('/storages')
      await expect(reopenedPage.getByRole('heading', { name: en.storages, exact: true })).toBeVisible()
    } finally {
      await reopened.close()
    }
  } finally {
    if (!context.pages().length) return
    await context.close()
  }
})

test('logout renews a stale CSRF token and keeps the session on a failed retry', async ({ page }) => {
  const origin = process.env.MOKYU_TEST_WEB!
  const login = await page.request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password: password() } })
  expect(login.ok()).toBe(true)
  // This tab still has the session view from before another tab renewed it.
  await page.route('**/api/v1/auth/session', async route => {
    const response = await route.fetch()
    await route.fulfill({ response, json: { ...await response.json(), csrf_token: 'previous-token' } })
  }, { times: 1 })
  await page.goto('/overview')
  const menu = page.getByRole('button', { name: en.accountMenu, exact: true })
  const logout = page.getByRole('menuitem', { name: en.signOut, exact: true })
  let attempts = 0
  await page.route('**/api/v1/auth/logout', route => ++attempts === 2
    ? route.fulfill({ status: 503, json: { code: 'SlowDown' } }) : route.continue())
  await menu.click()
  const mismatch = page.waitForResponse(r => r.url().endsWith('/auth/logout') && r.status() === 403)
  await logout.click()
  expect((await (await mismatch).json()).code).toBe('CsrfTokenMismatch')
  await expect.poll(() => attempts).toBe(2)
  await expect(menu).toBeVisible()
  expect((await page.request.get('/api/v1/auth/session')).ok()).toBe(true)
  await menu.click()
  await logout.click()
  await expect(page).toHaveURL(/\/login$/)
  await page.goto('/overview')
  await expect(page).toHaveURL(/\/login(?:\?.*)?$/)
  expect((await page.request.get('/api/v1/auth/session')).ok()).toBe(false)
})
