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
  const cookies = login.headersArray().filter(header => header.name.toLowerCase() === 'set-cookie').map(header => header.value)
  expect(cookies.some(cookie => cookie.startsWith('mokyu_session=') && cookie.includes('Path=/api/v1;'))).toBe(true)
  expect(cookies.some(cookie => cookie.startsWith('mokyu_refresh=') && cookie.includes('Path=/api/v1/auth/refresh;'))).toBe(true)

  const refresh = await request.post('/api/v1/auth/refresh', { headers: { Origin: origin } })
  expect(refresh.status()).toBe(200)
  expect((await refresh.json()).session_id).toMatch(/^[0-9a-f-]{36}$/)

  const context = await browser.newContext({ storageState: await request.storageState() })
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
