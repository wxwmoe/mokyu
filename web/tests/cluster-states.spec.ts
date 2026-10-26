import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'

test('node states stay consistent across lists, details and configuration', async ({ page }) => {
  test.setTimeout(120_000)
  const request = page.context().request, origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const seed = (await (await request.get('/api/v1/cluster/nodes')).json()).items.find((node: { role: string }) => node.role === 'data')
  const config = await (await request.get(`/api/v1/cluster/nodes/${seed.id}/config`)).json()
  const detail = await (await request.get(`/api/v1/cluster/nodes/${seed.id}/status`)).json()
  const overview = await (await request.get('/api/v1/cluster/status')).json()
  const cases = [
    { contact: 'online', work: 'normal', configuration: 'applied', tones: ['sky', 'mint', 'mint'] },
    { contact: 'not_reported', work: 'starting', configuration: 'equivalent', tones: ['cream', 'sky', 'sky'] },
    { contact: 'unresponsive', work: 'recovery', configuration: 'restart_required', tones: ['rose', 'rose', 'cream'] },
    { contact: 'released', work: 'maintenance', configuration: 'not_applicable', tones: ['gray', 'lilac', 'gray'] },
    { contact: 'online', work: 'restricted', configuration: 'unknown', tones: ['sky', 'cream', 'gray'] },
    { contact: 'online', work: 'unknown', configuration: 'applied', tones: ['sky', 'gray', 'mint'] },
  ]
  const nodes = cases.map(state => ({ ...seed, ...state, settings: Object.fromEntries(Object.keys(config.revisions).map(section => [section, state.configuration])) }))
  let currentNode = nodes[0]!
  const summaries = [
    { section: 'web', states: ['applied', 'not_applicable'], tone: 'mint', count: /1\s*\/\s*1/ },
    { section: 'requests', states: ['applied', 'equivalent'], tone: 'sky', count: /2\s*\/\s*2/ },
    { section: 'uploads', states: ['applied', 'unknown', 'restart_required'], tone: 'cream', count: /1\s*\/\s*3/ },
    { section: 'metrics', states: ['applied', 'unknown'], tone: 'gray', count: /1\s*\/\s*2/ },
    { section: 'maintenance', states: ['not_applicable'], tone: 'gray', count: /No relevant nodes/ },
  ]
  let settings = summaries.flatMap(row => row.states.map(state => ({ section: row.section, state, count: 1 })))
  // Override read responses only; leave the preview's node reports and settings intact.
  await page.route(/\/api\/v1\/cluster\/nodes(?:\?|$)/, route => route.fulfill({ json: { items: [currentNode], next_cursor: null } }))
  await page.route('**/api/v1/cluster/nodes/**', route => {
    const [, id, resource] = new URL(route.request().url()).pathname.match(/\/nodes\/([^/]+)\/(config|status)$/) || []
    if (id !== currentNode.id) return route.continue()
    const fields = [{ ...config.fields[0], key: 'replica_database.host', value_mode: 'inherited' }]
    return route.fulfill({ json: resource === 'config' ? { ...config, node: currentNode, fields } : { ...detail, node: currentNode } })
  })
  await page.route('**/api/v1/cluster/status', route => route.fulfill({ json: { ...overview, settings } }))
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  for (const theme of ['light', 'dark']) {
    expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
    const colors: Record<string, string> = theme === 'light'
      ? { sky: 'rgb(71, 126, 155)', mint: 'rgb(71, 126, 112)', lilac: 'rgb(130, 88, 174)', cream: 'rgb(152, 117, 54)', rose: 'rgb(176, 57, 87)', gray: 'rgb(75, 85, 99)' }
      : { sky: 'rgb(154, 203, 232)', mint: 'rgb(157, 207, 188)', lilac: 'rgb(201, 169, 241)', cream: 'rgb(228, 200, 137)', rose: 'rgb(245, 160, 175)', gray: 'rgb(203, 213, 225)' }
    for (const node of nodes) {
      currentNode = node
      await page.goto('/service')
      const row = page.locator('.node-row')
      for (const [column, tone] of node.tones.entries()) await expect(row.locator('.node-state').nth(column)).toHaveCSS('color', colors[tone]!)
      await page.setViewportSize({ width: 320, height: 844 })
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      await page.setViewportSize({ width: 1440, height: 1040 })
      await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/state-${node.work}-${theme}.png`, fullPage: true })
      await row.click()
      const dialog = page.getByRole('dialog')
      await expect(dialog).toBeVisible()
      for (const [column, tone] of node.tones.entries()) await expect(dialog.locator('.node-detail-status .node-state').nth(column)).toHaveCSS('color', colors[tone]!)
      await dialog.locator('a[href*="/runtime-config"]').click()
      await expect(page.locator('.node-config-context .node-detail-status .node-state')).toHaveCSS('color', colors[node.tones[2]!]!)
      const groups = page.locator('.node-config-revisions .node-state')
      await expect(groups).toHaveCount(Object.keys(config.revisions).length)
      for (const badge of await groups.all()) await expect(badge).toHaveCSS('color', colors[node.tones[2]!]!)
      await expect(page.locator('.config-value-mode-badge[data-mode="inherited"]')).toHaveCSS('color', colors.sky!)
      await page.setViewportSize({ width: 320, height: 844 })
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      await page.setViewportSize({ width: 1440, height: 1040 })
    }
    await page.goto('/runtime-config')
    for (const summary of summaries) {
      const badge = page.locator(`.node-state[href="/service?section=${summary.section}"]`)
      await expect(badge).toHaveCSS('color', colors[summary.tone]!)
      await expect(badge).toContainText(summary.count)
      await expect(badge).toHaveCSS('text-decoration-line', 'none')
      await badge.focus()
      await expect(badge).toBeFocused()
    }
    await page.setViewportSize({ width: 320, height: 844 })
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    await page.setViewportSize({ width: 1440, height: 1040 })
    await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/summaries-${theme}.png`, fullPage: true })
  }
  settings = []
  await page.reload()
  await expect(page.locator('.runtime-config-card a.node-state')).toHaveCount(6)
  for (const badge of await page.locator('.runtime-config-card a.node-state').all()) {
    await expect(badge).toHaveAttribute('data-tone', 'gray')
    await expect(badge).toContainText('No relevant nodes')
  }
  expect(errors).toEqual([])
})
