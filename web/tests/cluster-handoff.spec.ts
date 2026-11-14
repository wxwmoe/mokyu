import { readFileSync } from 'node:fs'
import { expect, test } from '@playwright/test'

test('a node can go offline and resume through the service page', async ({ page }) => {
  test.setTimeout(150_000)
  const origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const api = page.context().request
  const login = await api.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const nodes = (await (await api.get('/api/v1/cluster/nodes')).json()).items
  const target = nodes.find((row: { role: string; contact: string }) => row.role === 'data' && row.contact === 'online')
  expect(target).toBeDefined()
  const url = '/api/v1/cluster/nodes/' + target.id + '/status'
  const state = async () => {
    const reply = await api.get(url)
    expect(reply.ok()).toBe(true)
    return (await reply.json()).handoff.state
  }
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  for (const locale of ['zh-CN', 'en', 'ja']) {
    expect((await api.put('/api/v1/account', { headers, data: { display_name: '', locale, theme: 'light', avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
    await page.goto('/service')
    await page.locator('.node-row').filter({ hasText: target.name }).click()
    const panel = page.locator('.node-handoff')
    await expect(panel.getByRole('status')).toBeVisible()
    await expect(panel.getByRole('button')).toHaveCount(1)
    for (const width of [1280, 390, 320]) {
      await page.setViewportSize({ width, height: 900 })
      expect(await panel.evaluate(el => el.scrollWidth <= el.clientWidth + 1)).toBe(true)
    }
    if (locale !== 'en') continue
    await panel.getByRole('button', { name: 'Prepare to go offline' }).click()
    await expect.poll(state, { timeout: 40_000 }).toBe('offline_ready')
    await expect(panel.getByRole('status')).toHaveText('Safe to stop', { timeout: 20_000 })
    await panel.getByRole('button', { name: 'Resume service' }).click()
    await expect.poll(state, { timeout: 40_000 }).toBe('serving')
    await expect(panel.getByRole('status')).toHaveText('Serving', { timeout: 20_000 })
  }
  expect(errors).toEqual([])
})
