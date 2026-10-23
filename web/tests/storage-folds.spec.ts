import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect, type Page } from '@playwright/test'
import { openStorageGroup } from './storage-form'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

async function authenticate(page: Page) {
  const request = page.context().request
  const origin = process.env.MOKYU_TEST_WEB!
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const response = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(response.status()).toBe(200)
  return { request, headers: { Origin: origin, 'X-CSRF-Token': (await response.json()).csrf_token } }
}

test('storage presets stay visible while advanced groups preserve folded drafts', async ({ page }, info) => {
  test.setTimeout(120_000)
  const { request, headers } = await authenticate(page)
  const before = await (await request.get('/api/v1/storages')).json()
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
    for (const theme of ['light', 'dark']) {
      expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
      await page.goto('/storages')
      await page.getByRole('button', { name: text.createStore, exact: true }).click()
      const editor = page.getByRole('dialog', { name: text.createStore, exact: true })
      await expect(editor.getByRole('combobox', { name: text.storeIoPreset, exact: true })).toBeVisible()
      await expect(editor.getByRole('textbox', { name: text.storeEndpoint, exact: true })).toBeVisible()
      await expect(editor.locator('details.profile-group')).toHaveCount(5)
      await expect(editor.locator('details.profile-group[open]')).toHaveCount(0)
      await expect(editor.locator('.profile-changes')).toHaveCount(0)
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 1000 })
        await editor.locator('.storage-policy').scrollIntoViewIfNeeded()
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
        if (width === 1440 || width === 320) await editor.screenshot({ path: info.outputPath(`folds-${locale}-${theme}-${width}.png`) })
      }
      const lifecycle = editor.locator('[data-profile-group="storeGroupLifecycle"]')
      await lifecycle.locator('summary').focus()
      await page.keyboard.press('Enter')
      const timeout = editor.getByRole('textbox', { name: text.profile_request_timeout, exact: true })
      await expect(timeout).toBeVisible()
      await timeout.fill('31s')
      await lifecycle.locator('summary').focus()
      await page.keyboard.press('Space')
      await expect(timeout).toBeHidden()
      await expect(lifecycle.locator('summary')).toContainText(text.storeChangedFields.replace('{count}', '1'))
      await openStorageGroup(editor, 'storeGroupLifecycle')
      await expect(timeout).toHaveValue('31s')
      await timeout.fill('30s')
      await expect(lifecycle.locator('.profile-changes')).toHaveCount(0)
      await lifecycle.locator('summary').click()
      await editor.getByRole('combobox', { name: text.storeType, exact: true }).click()
      await page.getByRole('option', { name: text.storeType_local, exact: true }).click()
      await expect(editor.getByRole('textbox', { name: text.storePath, exact: true })).toBeVisible()
      await expect(editor.getByRole('combobox', { name: text.storeIoPreset, exact: true })).toBeVisible()
      await expect(editor.locator('details.profile-group[open]')).toHaveCount(0)
      await expect(editor.locator('.profile-changes')).toHaveCount(0)
      await openStorageGroup(editor)
      await expect(editor.getByRole('combobox', { name: text.profile_file_delivery, exact: true })).toBeVisible()
      await editor.getByRole('button', { name: text.cancel, exact: true }).click()
    }
  }
  expect(await (await request.get('/api/v1/storages')).json()).toEqual(before)
  expect(errors).toEqual([])
})

test('folded fields reveal validation failures and save or cancel without losing state', async ({ page }) => {
  const { request, headers } = await authenticate(page)
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const name = 'folds-' + randomUUID().slice(0, 8)
  const path = '/api/v1/storages/' + name
  expect((await request.post('/api/v1/storages', { headers, data: { name, type: 'local', path: process.env.MOKYU_TEST_FSYNC_ROOT + '/' + name, initialize: true } })).status()).toBe(201)
  const original = await (await request.get(path + '/profile')).json()
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/storages')
  const card = page.locator('.store-card').filter({ has: page.getByRole('heading', { name, exact: true }) })
  for (const label of [en.storeReadConcurrency, en.storeUploadConcurrency, en.storeControlConcurrency]) {
    await expect(card.getByText(label, { exact: true })).toHaveCount(0)
  }
  await card.getByRole('link', { name: en.storeDetails, exact: true }).click()
  await expect(page.getByText(en.storeReadConcurrency, { exact: true })).toBeVisible()
  await page.getByRole('button', { name: en.editStore, exact: true }).first().click()
  const editor = page.getByRole('dialog', { name: en.editStore, exact: true })
  await expect(editor.locator('details[open]')).toHaveCount(0)
  await openStorageGroup(editor, 'storeGroupWorker')
  const concurrency = editor.getByRole('spinbutton', { name: en.profile_read_concurrency, exact: true })
  await concurrency.fill('1.5')
  await editor.locator('[data-profile-group="storeGroupWorker"] summary').click()
  let writes = 0
  page.on('request', request => { if (request.url().endsWith(path) && request.method() === 'PUT') writes++ })
  await editor.getByRole('button', { name: en.save, exact: true }).click()
  await expect(concurrency).toBeVisible()
  await expect(concurrency).toBeFocused()
  expect(writes).toBe(0)
  await concurrency.fill('2')
  await openStorageGroup(editor, 'storeGroupLifecycle')
  const retries = editor.getByRole('spinbutton', { name: en.profile_max_retries, exact: true })
  await retries.fill('1.5')
  for (const group of await editor.locator('details.profile-group').all()) {
    if (await group.getAttribute('open') !== null) await group.locator('summary').click()
  }
  const malformed = page.waitForResponse(response => response.url().endsWith(path) && response.request().method() === 'PUT')
  await editor.getByRole('button', { name: en.save, exact: true }).click()
  expect((await malformed).status()).toBe(422)
  await expect(retries).toBeFocused()
  await expect(retries).toHaveValue('1.5')
  await retries.fill(String(original.max_retries))
  await openStorageGroup(editor)
  const age = editor.getByRole('textbox', { name: en.profile_public_cache_max_age, exact: true })
  await age.fill('0s')
  for (const group of await editor.locator('details.profile-group').all()) {
    if (await group.getAttribute('open') !== null) await group.locator('summary').click()
  }
  const rejected = page.waitForResponse(response => response.url().endsWith(path) && response.request().method() === 'PUT')
  await editor.getByRole('button', { name: en.save, exact: true }).click()
  expect((await rejected).status()).toBe(400)
  await expect(age).toBeFocused()
  await expect(age).toHaveValue('0s')
  await expect(concurrency).toHaveValue('2')
  expect(await (await request.get(path + '/profile')).json()).toEqual(original)
  await age.fill('5s')
  for (const group of await editor.locator('details.profile-group').all()) {
    if (await group.getAttribute('open') !== null) await group.locator('summary').click()
  }
  const saved = page.waitForResponse(response => response.url().endsWith(path) && response.request().method() === 'PUT')
  await editor.getByRole('button', { name: en.save, exact: true }).click()
  expect((await saved).status()).toBe(200)
  await page.goto('/storages?edit=' + name)
  await expect(editor.locator('details[open]')).toHaveCount(0)
  await expect(editor.locator('.profile-changes')).toHaveCount(0)
  await openStorageGroup(editor, 'storeGroupWorker')
  await expect(concurrency).toHaveValue('2')
  await concurrency.fill('3')
  await editor.locator('[data-profile-group="storeGroupWorker"] summary').click()
  await editor.getByRole('button', { name: en.cancel, exact: true }).click()
  expect((await (await request.get(path + '/profile')).json()).read_concurrency).toBe(2)
  expect(errors).toEqual([])
})
