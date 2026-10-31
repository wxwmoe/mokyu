import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test.skip(!process.env.MOKYU_TEST_REPLICATION_FIXTURE, 'Requires isolated source and destination buckets')

test('manage an independent replica through both bucket settings pages', async ({ page, request }, info) => {
  test.setTimeout(150000)
  const fixture = JSON.parse(readFileSync(process.env.MOKYU_TEST_REPLICATION_FIXTURE!, 'utf8'))
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  expect(login.status()).toBe(200)
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  async function preferences(locale: string, theme: string) {
    expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBeTruthy()
  }
  await preferences('en', 'light')
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/overview$/)
  async function open(id: string, title: string) {
    await page.goto('/buckets/' + id)
    await page.getByRole('tab', { name: title, exact: true }).click()
    await expect(page.getByRole('tabpanel').getByRole('heading', { name: title })).toBeVisible()
  }
  const base = '/api/v1/buckets/' + fixture.source.id + '/redundancy'
  const panel = page.getByRole('tabpanel')
  const dialog = page.getByRole('dialog')
  await open(fixture.source.id, en.redundancyTitle)
  await panel.getByRole('button', { name: en.redundancyAdd }).click()
  await dialog.getByRole('combobox', { name: en.replicationTarget, exact: true }).fill(fixture.target.name)
  await page.getByRole('option', { name: fixture.target.name, exact: true }).click()
  await dialog.getByRole('textbox', { name: en.redundancyPrefix }).fill('copy/')
  await dialog.getByRole('spinbutton', { name: en.redundancyQueueLimit }).fill('3')
  await dialog.getByRole('button', { name: en.redundancyPreview }).click()
  await expect(dialog.getByText(en.redundancyReview, { exact: false })).toBeVisible()
  expect((await (await request.get(base)).json()).links).toHaveLength(0)
  await dialog.getByRole('button', { name: en.redundancyAdd }).click()
  await expect(dialog).toHaveCount(0)
  const rows = async () => (await (await request.get(base)).json()).links
  await expect.poll(async () => (await rows())[0]?.status, { timeout: 30000 }).toBe('healthy')
  const [link] = await rows()
  expect(link.settings).toMatchObject({ target_bucket: fixture.target.id, target_prefix: 'copy/', max_pending_keys: 3 })
  expect(link.confirmed).toBe(2)
  async function command(label: string, state: string, prefix?: string) {
    await panel.getByRole('button', { name: label, exact: true }).click()
    if (prefix) await dialog.getByRole('textbox', { name: en.redundancyScanPrefix }).fill(prefix)
    await dialog.getByRole('button', { name: label, exact: true }).click()
    await expect(dialog).toHaveCount(0)
    await expect.poll(async () => (await rows())[0]?.status, { timeout: 30000 }).toBe(state)
    await panel.getByRole('button', { name: en.refresh }).click()
  }
  await command(en.redundancyAction_pause, 'paused')
  await command(en.redundancyAction_resume, 'healthy')
  await command(en.redundancyAction_reconcile, 'healthy', 'photos/')
  for (const [locale, theme, width, labels] of [
    ['en', 'light', 1280, en], ['zh-CN', 'dark', 375, zh], ['ja', 'light', 768, ja],
  ] as const) {
    await preferences(locale, theme)
    await page.setViewportSize({ width, height: 900 })
    await open(fixture.target.id, labels.redundancyTitle)
    await expect(panel.getByText(labels.redundancyIncoming, { exact: true })).toBeVisible()
    await expect(panel.getByRole('button', { name: labels.redundancyAction_pause })).toHaveCount(0)
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width)
    await info.attach('incoming-' + locale, { body: await page.screenshot(), contentType: 'image/png' })
  }
  await panel.getByRole('button', { name: ja.redundancyAction_remove }).click()
  await dialog.getByRole('button', { name: ja.redundancyAction_remove, exact: true }).click()
  await expect.poll(rows, { timeout: 30000 }).toHaveLength(0)
  expect(errors).toEqual([])
})
