import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect, type Page } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

const password = () => (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
async function login(page: Page, name = 'tester', secret = password()) {
  await page.goto('/login'); await page.getByRole('textbox', { name: en.username, exact: true }).fill(name)
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(secret)
  await page.getByRole('button', { name: en.signIn, exact: true }).click(); await expect(page).toHaveURL(/\/overview$/)
}

test('three languages, themes, responsive pages, searchable choices and visible errors', async ({ page, request }) => {
  test.setTimeout(150000)
  const auth = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password: password() } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await auth.json()).csrf_token }
  const preference = (locale: string, theme: string) => request.put('/api/me', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })
  await preference('en', 'light'); await page.emulateMedia({ reducedMotion: 'reduce' }); await login(page)
  const errors: string[] = []; page.on('pageerror', e => errors.push(e.message))
  const routes = ['overview', 'media', 'storage', 'maintenance', 'tasks', 'service', 'users', 'projects', 'credentials', 'tokens', 'audit', 'account', 'transfers', 'buckets']
  try {
    for (const [locale, theme, width, height] of [['en', 'light', 1440, 1000], ['zh-CN', 'light', 1440, 1000], ['zh-CN', 'dark', 390, 844], ['ja', 'light', 1440, 1000], ['ja', 'light', 768, 1024], ['ja', 'dark', 320, 740], ['en', 'light', 640, 450]] as const) {
      await preference(locale, theme); await page.setViewportSize({ width, height })
      for (const route of routes) {
        await page.goto('/' + route); await expect(page.locator('main h1')).toBeVisible()
        await expect(page.locator('html')).toHaveAttribute('lang', locale)
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme)
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), `${locale}/${theme}/${width}/${route}`).toBe(true)
        await expect(page.locator('select')).toHaveCount(0)
        if ((route === 'overview' || route === 'maintenance') && width !== 640) {
          await page.locator('.insight-card, .policy-card').first().waitFor(); await page.evaluate(() => scrollTo(0, 0))
          await page.screenshot({ path: `/results/step17-${route}-${locale}-${theme}-${width}.png` })
        }
      }
    }
    await preference('en', 'light'); await page.setViewportSize({ width: 1280, height: 900 }); await page.goto('/overview')
    const scope = page.getByRole('combobox', { name: en.insightScope, exact: true })
    await scope.fill('no-such-bucket-for-search'); await expect(page.getByText(en.noChoiceMatches)).toBeVisible()
    await scope.fill('media'); await page.getByRole('option', { name: /^media / }).click()
    await expect(page).toHaveURL(/bucket=/); await expect(scope).toHaveValue('media')
    await page.route('**/api/buckets', route => route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ code: 'SlowDown', request_id: 'polish-request-id' }) }))
    await page.goto('/media'); await expect(page.locator('.inline-error')).toContainText('polish-request-id')
    await expect(page.getByRole('button', { name: en.copyRequestId })).toBeVisible()
    await page.unroute('**/api/buckets'); await page.goto('/media'); await expect(page.locator('.bucket-card').first()).toBeVisible()
    expect(errors).toEqual([])
  } finally { await preference('en', 'light') }
})

test('identity changes in another tab clear private views and cancel stale session state', async ({ page, context, request }) => {
  const auth = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password: password() } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await auth.json()).csrf_token }
  await request.post('/api/me/reauth', { headers, data: { password: password() } })
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const name = 'tabs-' + randomUUID().slice(0, 8), secret = randomUUID()
  const created = await request.post('/api/users', { headers, data: { username: name, password: secret, role: 'member', must_change_password: false } })
  expect(created.status()).toBe(201); const id = (await created.json()).id
  const other = await context.newPage()
  try {
    await login(page); await other.goto('/users'); await expect(other.locator('.person-row').first()).toBeVisible()
    await other.goto('/account'); await other.getByRole('textbox', { name: en.displayName, exact: true }).fill('Unsaved private name')
    await page.getByRole('button', { name: en.accountMenu }).click(); await page.getByRole('menuitem', { name: en.signOut }).click()
    await expect(other).toHaveURL(/\/login$/); await expect(other.locator('.person-row')).toHaveCount(0)
    await login(page, name, secret); await expect(other).toHaveURL(/\/overview$/)
    await expect(other.getByRole('link', { name: en.users, exact: true })).toHaveCount(0)
    expect((await (await other.request.get('/api/session')).json()).username).toBe(name)
    await other.goto('/media'); await expect(other.locator('.bucket-card')).toHaveCount(0)
  } finally { await other.close(); await request.delete('/api/users/' + id, { headers }) }
})
