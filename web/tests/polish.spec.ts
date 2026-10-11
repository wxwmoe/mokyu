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
  const auth = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password: password() } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await auth.json()).csrf_token }
  const preference = (locale: string, theme: string) => request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })
  await preference('en', 'light'); await page.emulateMedia({ reducedMotion: 'reduce' }); await login(page)
  const errors: string[] = []; page.on('pageerror', e => errors.push(e.message))
  const routes = ['overview', 'media', 'storage', 'maintenance', 'tasks', 'service', 'runtime-config', 'users', 'projects', 'credentials', 'tokens', 'audit', 'account', 'transfers', 'buckets']
  try {
    for (const [locale, theme, width, height] of [['en', 'light', 1440, 1000], ['zh-CN', 'light', 1440, 1000], ['zh-CN', 'dark', 390, 844], ['ja', 'light', 1440, 1000], ['ja', 'light', 768, 1024], ['ja', 'dark', 320, 740], ['en', 'light', 640, 450]] as const) {
      await preference(locale, theme); await page.setViewportSize({ width, height })
      for (const route of routes) {
        await page.goto('/' + route); await expect(page.locator('main h1')).toBeVisible()
        await expect(page.locator('html')).toHaveAttribute('lang', locale)
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme)
        if (route === 'runtime-config') {
          await page.locator('.config-source-badge').first().waitFor()
          await page.locator('.advanced-config > summary').click()
          expect(await page.locator('.config-source-badge').evaluateAll(nodes => nodes.every(node => node.scrollWidth <= node.clientWidth))).toBe(true)
        }
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
    if (await scope.evaluate(element => element.tagName === 'INPUT')) {
      await scope.fill('no-such-bucket-for-search'); await expect(page.getByText(en.noChoiceMatches)).toBeVisible()
      await scope.fill('media'); await page.getByRole('option', { name: /^media / }).click()
      await expect(page).toHaveURL(/bucket=/); await expect(scope).toHaveValue('media')
    } else {
      await scope.click(); await expect(page.getByRole('option').first()).toBeVisible(); await page.keyboard.press('Escape')
    }
    await page.route('**/api/v1/buckets', route => route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ code: 'SlowDown', request_id: 'polish-request-id' }) }))
    await page.goto('/media'); await expect(page.locator('.inline-error')).toContainText('polish-request-id')
    await expect(page.getByRole('button', { name: en.copyRequestId })).toBeVisible()
    await page.unroute('**/api/v1/buckets'); await page.goto('/media'); await expect(page.locator('.bucket-card').first()).toBeVisible()
    expect(errors).toEqual([])
  } finally { await preference('en', 'light') }
})

test('identity changes in another tab clear private views and cancel stale session state', async ({ page, context, request }) => {
  const auth = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password: password() } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await auth.json()).csrf_token }
  await request.post('/api/v1/account/reauth', { headers, data: { password: password() } })
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const name = 'tabs-' + randomUUID().slice(0, 8), secret = randomUUID()
  const created = await request.post('/api/v1/users', { headers, data: { username: name, password: secret, role: 'member', must_change_password: false } })
  expect(created.status()).toBe(201); const id = (await created.json()).id
  const other = await context.newPage()
  try {
    await login(page); await other.goto('/users'); await expect(other.locator('.person-row').first()).toBeVisible()
    await other.goto('/account'); await other.getByRole('textbox', { name: en.displayName, exact: true }).fill('Unsaved private name')
    await page.getByRole('button', { name: en.accountMenu }).click(); await page.getByRole('menuitem', { name: en.signOut }).click()
    await expect(other).toHaveURL(/\/login$/); await expect(other.locator('.person-row')).toHaveCount(0)
    await login(page, name, secret); await expect(other).toHaveURL(/\/overview$/)
    await expect(other.getByRole('link', { name: en.users, exact: true })).toHaveCount(0)
    expect((await (await other.request.get('/api/v1/auth/session')).json()).username).toBe(name)
    await other.goto('/media'); await expect(other.locator('.bucket-card')).toHaveCount(0)
  } finally { await other.close(); await request.delete('/api/v1/users/' + id, { headers }) }
})

test('list focus and footer rhythm stay consistent', async ({ page }) => {
  test.setTimeout(90000)
  await login(page)
  const focus = async (path: string, selector: string) => {
    await page.goto('/' + path)
    const row = page.locator(selector).first()
    await row.waitFor()
    await row.focus()
    return row.evaluate(element => {
      const style = getComputedStyle(element)
      const icon = element.querySelector('.policy-icon, .audit-icon, .avatar-fallback, .key-symbol')
      return { background: style.backgroundColor, iconBackground: icon ? getComputedStyle(icon).backgroundColor : '' }
    })
  }
  const audit = await focus('audit', '.audit-row')
  const tasks = await focus('tasks', '.task-row')
  const users = await focus('users', '.person-row')
  const credentials = await focus('credentials', '.key-row')
  expect(tasks.background).toBe(audit.background)
  expect(users.background).toBe(audit.background)
  expect(tasks.iconBackground).not.toBe(tasks.background)
  expect(users.iconBackground).not.toBe(users.background)
  expect(credentials.background).toBe('rgba(0, 0, 0, 0)')

  const footerGap = async (path: string, selector: string) => {
    await page.goto('/' + path)
    const footer = page.locator(selector)
    await footer.waitFor()
    return footer.evaluate(element => {
      const parent = element.parentElement!
      const footerBox = element.getBoundingClientRect()
      const parentBox = parent.getBoundingClientRect()
      const style = getComputedStyle(element)
      return { borderTop: style.borderTopStyle, gap: Math.round(parentBox.bottom - footerBox.bottom) }
    })
  }
  const auditFooter = await footerGap('audit', '.list-footer')
  const taskFooter = await footerGap('tasks', '.list-footer')
  const usersFooter = await footerGap('users', '.users-footer')
  expect(taskFooter.gap).toBe(auditFooter.gap)
  expect(taskFooter.borderTop).toBe('none')
  expect(usersFooter.borderTop).toBe('none')

  await page.goto('/buckets')
  const bucket = await page.locator('.bucket-card').first().getAttribute('href')
  expect(bucket).toBeTruthy()
  await page.goto(bucket!)
  await expect(page.locator('.bucket-settings > .tabs-list')).toBeVisible()
  expect(await page.locator('.bucket-settings > .tabs-list').evaluate(element => getComputedStyle(element).marginTop)).toBe('0px')
})
