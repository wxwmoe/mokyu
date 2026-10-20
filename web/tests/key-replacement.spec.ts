import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('key replacement previews, resumes and saves the complete scope once', async ({ page }) => {
  test.setTimeout(120_000)
  const request = page.context().request, origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const storageView = async (name: string) => (await (await request.get('/api/v1/storages')).json()).find((row: { name: string }) => row.name === name)
  const prefix = 'replace-' + randomUUID().slice(0, 8)
  const created = await request.post('/api/v1/keys', { headers, data: { name: prefix, purpose: 'block' } })
  expect(created.status()).toBe(201)
  const source = await created.json(), storages = []
  for (const suffix of ['local', 'disabled']) {
    const name = prefix + '-' + suffix
    const created = await request.post('/api/v1/storages', { headers, data: { name, type: 'local', path: process.env.MOKYU_TEST_FSYNC_ROOT + '/' + name, initialize: true } })
    expect(created.status()).toBe(201)
    expect((await request.put('/api/v1/storages/' + name + '/key', { headers, data: { key: source.name } })).ok()).toBe(true)
    if (suffix === 'disabled') expect((await request.put('/api/v1/storages/' + name, { headers, data: { enabled: false } })).ok()).toBe(true)
    storages.push(await storageView(name))
  }
  const preferences = async (locale: string, theme: string) => expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
  await preferences('en', 'light')
  await page.goto('/credentials?tab=managed&replace_key=' + source.id)
  let dialog = page.getByRole('dialog', { name: en.replaceStorages, exact: true })
  await expect(dialog).toBeVisible()
  await dialog.getByRole('textbox', { name: en.keyName, exact: true }).fill(prefix + '-new')
  const response = page.waitForResponse(reply => reply.url().endsWith('/replace-preview') && reply.request().method() === 'POST')
  await dialog.getByRole('button', { name: en.replacePreview, exact: true }).click()
  const preview = await (await response).json()
  await expect(dialog.getByRole('button', { name: en.replaceConfirm, exact: true })).toBeVisible()
  const url = page.url()
  expect(url).toContain('replacement=' + preview.id)
  expect((await (await request.get('/api/v1/keys')).json()).some((key: { name: string }) => key.name === prefix + '-new')).toBe(false)
  for (const storage of storages) expect((await storageView(storage.name)).block_key_id).toBe(source.id)
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  for (const [locale, messages] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
    for (const theme of ['light', 'dark']) {
      await preferences(locale, theme)
      await page.goto(url)
      dialog = page.getByRole('dialog', { name: messages.replaceStorages, exact: true })
      await expect(dialog.getByRole('button', { name: messages.replaceConfirm, exact: true })).toBeVisible()
      await expect(dialog.getByText(messages.disabled, { exact: true })).toBeVisible()
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 1000 })
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
        expect(await dialog.evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true)
        if (width === 1440 || width === 320) await page.screenshot({ path: `${process.env.MOKYU_TEST_RESULTS}/replacement-${locale}-${theme}-${width}.png`, fullPage: true })
      }
    }
  }
  await preferences('en', 'light')
  await page.goto(url)
  dialog = page.getByRole('dialog', { name: en.replaceStorages, exact: true })
  const committed = page.waitForResponse(reply => reply.url().endsWith('/key-replacements/' + preview.id) && reply.request().method() === 'POST')
  await dialog.getByRole('button', { name: en.replaceConfirm, exact: true }).click()
  const saved = await (await committed).json()
  expect(saved.pending).toEqual([])
  await expect(dialog.getByRole('status')).toContainText(en.replaceSaved)
  for (const storage of storages) {
    const after = await storageView(storage.name)
    expect(after.block_key_id).toBe(preview.target)
    expect(after.profile_revision).toBe(storage.profile_revision + 1)
    expect(after.enabled).toBe(storage.enabled)
  }
  await page.reload()
  await expect(dialog.getByRole('status')).toContainText(en.replaceSaved)
  await dialog.getByRole('button', { name: en.replaceRetry, exact: true }).click()
  await expect(dialog.getByRole('button', { name: en.replaceRetry, exact: true })).toHaveCount(0)
  expect((await (await request.get('/api/v1/keys')).json()).filter((key: { name: string }) => key.name === prefix + '-new')).toHaveLength(1)
  expect((await (await request.get('/api/v1/key-replacements/' + preview.id)).json()).result).toEqual(saved.result)
  expect(errors).toEqual([])
})
