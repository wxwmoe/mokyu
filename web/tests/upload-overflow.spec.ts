import { openStorageGroup } from './storage-form'
import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('upload cache overflow stays in the storage draft until saved', async ({ page, request }, info) => {
  test.setTimeout(120_000)
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const origin = process.env.MOKYU_TEST_WEB!
  const auth = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(auth.status()).toBe(200)
  const headers = { Origin: origin, 'X-CSRF-Token': (await auth.json()).csrf_token }
  const backend = JSON.parse(readFileSync(process.env.MOKYU_TEST_BACKEND!, 'utf8'))
  const name = 'overflow-ui-' + randomUUID().slice(0, 8)
  const created = await request.post('/api/v1/storages', { headers, data: {
    name, type: 's3', endpoint: backend.endpoint, bucket: backend.bucket, region: backend.region,
    prefix: [backend.prefix, name].filter(Boolean).join('/'), access_key: backend.access_key,
    secret_key: backend.secret_key, initialize: true,
  } })
  expect(created.status()).toBe(201)
  const url = '/api/v1/storages/' + name + '/profile'
  const original = await (await request.get(url)).json()
  expect(original.upload_cache_overflow).toBe('fallback')
  const localName = name + '-local'
  expect((await request.post('/api/v1/storages', { headers, data: { name: localName, type: 'local', path: process.env.MOKYU_TEST_FSYNC_ROOT + '/' + localName, initialize: true } })).status()).toBe(201)
  const localUrl = '/api/v1/storages/' + localName + '/profile'
  const local = await (await request.get(localUrl)).json()
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/overview$/)
  for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
    for (const theme of ['light', 'dark']) {
      const profile = await (await request.get(url)).json()
      expect((await request.put(url, { headers, data: { ...profile, upload_cache_overflow: 'fallback', write_processing: 'inline' } })).ok()).toBe(true)
      expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
      await page.goto('/storages?edit=' + name)
      const editor = page.getByRole('dialog', { name: text.editStore, exact: true })
      await openStorageGroup(editor)
      const processing = editor.getByRole('combobox', { name: text.profile_write_processing, exact: true })
      await expect(processing).toBeEnabled()
      await processing.click()
      await page.getByRole('option', { name: text.storeProcessing_background, exact: true }).click()
      expect((await (await request.get(url)).json()).write_processing).toBe('inline')
      const overflow = editor.getByRole('combobox', { name: text.profile_upload_cache_overflow, exact: true })
      await expect(overflow).toHaveAttribute('aria-describedby', 'storage-upload-overflow-hint')
      await expect(overflow).toHaveText(text.storeOverflow_fallback)
      await overflow.click()
      await page.getByRole('option', { name: text.storeOverflow_reject, exact: true }).click()
      expect((await (await request.get(url)).json()).upload_cache_overflow).toBe('fallback')
      for (const width of [1440, 390, 320]) {
        await page.setViewportSize({ width, height: 900 })
        await overflow.scrollIntoViewIfNeeded()
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
        await expect(overflow).toBeVisible()
      }
      await editor.screenshot({ path: info.outputPath('overflow-' + locale + '-' + theme + '-320.png') })
      await editor.getByRole('button', { name: text.cancel, exact: true }).click()
      expect((await (await request.get(url)).json()).upload_cache_overflow).toBe('fallback')
      expect((await (await request.get(url)).json()).write_processing).toBe('inline')
      await page.goto('/storages?edit=' + name)
      await openStorageGroup(editor)
      await expect(processing).toHaveText(text.storeProcessing_inline)
      await processing.click()
      await page.getByRole('option', { name: text.storeProcessing_background, exact: true }).click()
      await expect(overflow).toHaveText(text.storeOverflow_fallback)
      await overflow.scrollIntoViewIfNeeded()
      await overflow.focus()
      await overflow.press('Space')
      await page.getByRole('option', { name: text.storeOverflow_reject, exact: true }).focus()
      await page.keyboard.press('Enter')
      await expect(overflow).toHaveText(text.storeOverflow_reject)
      const saved = page.waitForResponse(reply => reply.url().endsWith('/api/v1/storages/' + name) && reply.request().method() === 'PUT')
      await editor.getByRole('button', { name: text.save, exact: true }).click()
      expect((await saved).status()).toBe(200)
      await expect(editor).toHaveCount(0)
      expect((await (await request.get(url)).json()).upload_cache_overflow).toBe('reject')
      expect((await (await request.get(url)).json()).write_processing).toBe('background')
    }
  }
  await page.goto('/storages?edit=' + localName)
  const editor = page.getByRole('dialog', { name: ja.editStore, exact: true })
  await openStorageGroup(editor)
  await expect(editor.getByRole('combobox', { name: ja.profile_write_processing, exact: true })).toBeEnabled()
  await expect(editor.getByRole('combobox', { name: ja.profile_upload_cache_overflow, exact: true })).toBeDisabled()
  expect((await (await request.get(localUrl)).json()).revision).toBe(local.revision)
  expect(errors).toEqual([])
})
