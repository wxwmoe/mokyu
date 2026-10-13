import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect, type Page } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import ja from '../src/locales/i18n.ja.js'
import zh from '../src/locales/i18n.zh-CN.js'

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
        await expect(page.locator('meta[http-equiv="content-language"]')).toHaveAttribute('content', locale)
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme)
        if (route === 'runtime-config') {
          await page.locator('.config-origin-badge').first().waitFor()
          await page.locator('.advanced-config > summary').click()
          expect(await page.locator('.config-origin-badge, .config-value-mode-badge').evaluateAll(nodes => nodes.every(node => node.scrollWidth <= node.clientWidth))).toBe(true)
          if (locale === 'en') {
            const origin = page.locator('.config-fields article').filter({ hasText: 'web.origin' }).first()
            await expect(origin.locator('.config-copy > strong')).toHaveText(en.runtimeField_origin)
            await expect(origin.locator('.config-value .config-origin-badge')).toHaveCount(1)
            await expect(origin.locator('.config-value .config-value-mode-badge')).toHaveCount(1)
            await expect(origin.locator('.config-value .config-value-chip')).toHaveCount(1)
            await expect(origin.locator('.config-value small')).toHaveCount(1)
            await expect(origin.locator('.config-meta small')).toHaveText(en.appliedSetting)
            await expect(origin.locator('.config-edit-button')).toHaveCount(1)
            await origin.getByRole('button', { name: en.editConfigField, exact: true }).click()
            await expect(page.locator('[data-runtime-field="web.origin"] input')).toBeFocused()
          }
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

test('pack creation editing lands on the switch without crowding its actions', async ({ page, request }) => {
  test.setTimeout(90000)
  const auth = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password: password() } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await auth.json()).csrf_token }
  const { display_name, locale, theme, avatar_email, avatar_enabled } = await (await request.get('/api/v1/account')).json()
  const preferences = { display_name, locale, theme, avatar_email, avatar_enabled }
  const statusResponse = await request.get('/api/v1/maintenance', { headers })
  expect(statusResponse.ok()).toBe(true)
  const status = await statusResponse.json() as { pack_creation_enabled: boolean; pack_configured: boolean }
  expect(status.pack_configured).toBe(true)
  try {
    expect((await request.put('/api/v1/account', { headers, data: { ...preferences, locale: 'en', theme: 'light' } })).ok()).toBe(true)
    await login(page)
    await page.setViewportSize({ width: 1440, height: 1000 })
    let writes = 0
    page.on('request', requestEvent => { if (requestEvent.method() === 'POST' && requestEvent.url().endsWith('/api/v1/maintenance/pack-creation')) writes++ })
    await page.goto('/runtime-config')
    const field = page.locator('.config-fields article').filter({ hasText: 'pack.creation_enabled' }).first()
    await expect(field).toBeVisible()
    await field.getByRole('button', { name: en.editConfigField, exact: true }).click()
    await expect(page).toHaveURL(/\/maintenance\?focus=pack-creation$/)
    const toggle = page.locator('[data-maintenance-focus="pack-creation"]')
    await expect(toggle).toBeFocused()
    await expect(toggle).toHaveClass(/is-highlighted/)
    expect(writes).toBe(0)
    const geometry = await page.locator('.unpack-controls').evaluate(element => {
      const setting = element.querySelector('.pack-setting')!.getBoundingClientRect()
      const actions = element.querySelector('.unpack-actions')!.getBoundingClientRect()
      return { setting, actions }
    })
    expect(geometry.setting.width).toBeGreaterThan(0)
    expect(geometry.actions.width).toBeGreaterThan(0)
    expect(geometry.setting.right).toBeLessThanOrEqual(geometry.actions.left + 1)
    await toggle.click()
    await expect(toggle).toHaveAttribute('aria-checked', String(!status.pack_creation_enabled))
    await expect(toggle).toBeEnabled()
    await page.route('**/api/v1/maintenance/pack-creation', route => route.fulfill({ status: 409, contentType: 'application/json', body: JSON.stringify({ code: 'OperationAborted', request_id: 'pack-toggle-rejected' }) }))
    await toggle.click()
    await expect(page.getByRole('button', { name: 'pack-toggle-rejected', exact: true })).toBeVisible()
    await expect(toggle).toHaveAttribute('aria-checked', String(!status.pack_creation_enabled))
    await page.unroute('**/api/v1/maintenance/pack-creation')
    await page.getByRole('link', { name: en.runtimeConfig, exact: true }).click()
    await expect(field.locator('.config-value-chip')).toHaveText(String(!status.pack_creation_enabled))
    await field.getByRole('button', { name: en.editConfigField, exact: true }).click()
    await expect(toggle).toBeFocused()
    for (const width of [1440, 1024, 768, 640, 480, 390, 320]) {
      await page.setViewportSize({ width, height: 1000 })
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), String(width)).toBe(true)
      const boxes = await page.locator('.unpack-controls > div').evaluateAll(elements => elements.map(element => element.getBoundingClientRect().toJSON()))
      expect(boxes[0]!.right <= boxes[1]!.left || boxes[0]!.bottom <= boxes[1]!.top, String(width)).toBe(true)
      await page.locator('.recovery-card').filter({ has: toggle }).screenshot({ path: `/results/pack-controls-${width}.png` })
    }
  } finally {
    const current = await (await request.get('/api/v1/maintenance', { headers })).json() as { pack_creation_enabled: boolean }
    if (current.pack_creation_enabled !== status.pack_creation_enabled) expect((await request.post('/api/v1/maintenance/pack-creation', { headers, data: { enabled: status.pack_creation_enabled } })).ok()).toBe(true)
    expect((await request.put('/api/v1/account', { headers, data: preferences })).ok()).toBe(true)
  }
})

test('language hints follow the selected locale without discarding form input', async ({ page }) => {
  await page.goto('/login')
  let dictionary = en
  for (const [value, next] of [['ja', ja], ['zh-CN', zh], ['en', en]] as const) {
    await page.getByRole('textbox', { name: dictionary.username, exact: true }).fill('language-draft')
    await page.getByRole('button', { name: dictionary.language, exact: true }).click()
    await page.getByRole('menuitem', { name: en[`language_${value}`], exact: true }).click()
    await expect(page.locator('html')).toHaveAttribute('lang', value)
    await expect(page.locator('meta[http-equiv="content-language"]')).toHaveAttribute('content', value)
    await expect(page.getByRole('textbox', { name: next.username, exact: true })).toHaveValue('language-draft')
    await page.reload()
    await expect(page.locator('html')).toHaveAttribute('lang', value)
    await expect(page.locator('meta[http-equiv="content-language"]')).toHaveAttribute('content', value)
    dictionary = next
  }
})

test('service state notices appear across workspace routes', async ({ page }) => {
  await page.route('**/api/v1/system/status', route => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({
      version: '0.0.4', started_at: '2026-10-12T00:00:00Z', maintenance: false,
      mode_mismatch: true, recovery_required: false, restart_required: true,
      counters: [], backend_queues: {}, backend_operations: {}, catalog: {},
      inventory_as_of: null, inventory_error: null, inventory_stale: false,
    }),
  }))
  await login(page)
  const routes = ['overview', 'media', 'storage', 'transfers', 'maintenance', 'buckets', 'users', 'credentials', 'storages', 'tasks', 'audit', 'runtime-config', 'service']
  for (const route of routes) {
    await page.goto('/' + route)
    await expect(page.locator('.overview-status-stack')).toContainText(en.modeMismatchBannerTitle)
    await expect(page.locator('.overview-status-stack')).toContainText(en.restartBannerTitle)
    await expect(page.locator('.overview-status-banner')).toHaveCount(2)
  }
  await page.goto('/overview')
  const order = await page.locator('.overview-status-stack').evaluate(element => !!(element.compareDocumentPosition(document.querySelector('.overview-heading')!) & Node.DOCUMENT_POSITION_FOLLOWING))
  expect(order).toBe(true)
})

test('maintenance notice stays off its workspace and opens the maintenance page', async ({ page }) => {
  await page.route('**/api/v1/system/status', route => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({
      version: '0.0.4', started_at: '2026-10-12T00:00:00Z', maintenance: true,
      mode_mismatch: false, recovery_required: false, restart_required: false,
      counters: [], backend_queues: {}, backend_operations: {}, catalog: {},
      inventory_as_of: null, inventory_error: null, inventory_stale: false,
    }),
  }))
  await page.route('**/api/v1/maintenance', route => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({ maintenance: true, controls: [], concurrency: 0, pack_configured: false, pack_creation_paused: false, pack_creation_enabled: false, preparing_packs: '0', active_operations: 0, backend_prefixes: [], min_storage_duration: '0s' }),
  }))
  await login(page)
  await page.goto('/overview')
  const banner = page.locator('.overview-status-banner')
  await expect(banner).toContainText(en.maintenanceBannerTitle)
  await expect(banner.getByRole('link')).toHaveAttribute('href', '/maintenance')
  await expect(banner.getByRole('link')).toContainText(en.openMaintenance)
  await page.goto('/maintenance')
  await expect(page.locator('.overview-status-banner')).toHaveCount(0)
  await expect(page.getByText(en.maintenanceActiveHint, { exact: true })).toBeVisible()
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
  const usersFooter = await footerGap('users', '.list-footer')
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
