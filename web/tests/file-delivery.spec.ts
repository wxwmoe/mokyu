import { openStorageGroup } from './storage-form'
import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('local proxy delivery and prefixes are explicit drafts independent of I/O presets', async ({ page, request }, info) => {
  test.setTimeout(240_000)
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const origin = process.env.MOKYU_TEST_WEB!
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const name = 'delivery-ui-' + randomUUID().slice(0, 8)
  expect((await request.post('/api/v1/storages', { headers, data: { name, type: 'local', path: process.env.MOKYU_TEST_FSYNC_ROOT + '/' + name, initialize: true } })).status()).toBe(201)
  const url = '/api/v1/storages/' + name + '/profile'
  const original = await (await request.get(url)).json()
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/overview$/)
  for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
    for (const theme of ['light', 'dark']) {
      await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })
      await page.goto('/storages?edit=' + name)
      const editor = page.getByRole('dialog', { name: text.editStore, exact: true })
      await openStorageGroup(editor)
      const mode = editor.getByRole('combobox', { name: text.profile_file_delivery, exact: true })
      await expect(mode).toContainText(text.storeDelivery_application)
      await expect(editor.getByRole('textbox', { name: text.profile_file_delivery_prefix, exact: true })).toHaveCount(0)
      for (const option of [text.storeDelivery_x_accel_redirect, text.storeDelivery_x_sendfile]) {
        await mode.scrollIntoViewIfNeeded()
        await mode.press('ArrowDown')
        await expect(page.getByRole('option', { name: option, exact: true })).toBeVisible()
        await page.getByRole('option', { name: option, exact: true }).focus()
        await page.keyboard.press('Enter')
        const prefix = editor.getByRole('textbox', { name: text.profile_file_delivery_prefix, exact: true })
        await prefix.fill(option === text.storeDelivery_x_accel_redirect ? '/_mokyu_raw/' : '/srv/raw/')
        await expect(editor.getByText(text.storeDeliveryHint, { exact: true })).toBeVisible()
        for (const width of [1440, 768, 640, 390, 320]) {
          await page.setViewportSize({ width, height: 900 })
          await mode.scrollIntoViewIfNeeded()
          expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
        }
      }
      await editor.screenshot({ path: info.outputPath('delivery-' + locale + '-' + theme + '-320.png') })
      expect(await (await request.get(url)).json()).toEqual(original)
      await editor.getByRole('button', { name: text.cancel, exact: true }).click()
    }
  }
  await page.goto('/storages?edit=' + name)
  const editor = page.getByRole('dialog', { name: ja.editStore, exact: true })
  await openStorageGroup(editor)
  const mode = editor.getByRole('combobox', { name: ja.profile_file_delivery, exact: true })
  await mode.click()
  await page.getByRole('option', { name: ja.storeDelivery_x_accel_redirect, exact: true }).click()
  await editor.getByRole('textbox', { name: ja.profile_file_delivery_prefix, exact: true }).fill('/_mokyu_raw')
  await editor.getByRole('button', { name: ja.storeIoPreview, exact: true }).click()
  await page.getByRole('dialog', { name: ja.storeIoPreview, exact: true }).getByRole('button', { name: ja.storeIoApplyDraft, exact: true }).click()
  await expect(mode).toContainText(ja.storeDelivery_x_accel_redirect)
  const save = page.waitForResponse(reply => reply.url().endsWith('/api/v1/storages/' + name) && reply.request().method() === 'PUT')
  await editor.getByRole('button', { name: ja.save, exact: true }).click()
  expect((await save).status()).toBe(200)
  expect((await (await request.get(url)).json()).file_delivery_prefix).toBe('/_mokyu_raw/')
  await page.goto('/storages?edit=' + name)
  await openStorageGroup(editor)
  await mode.click()
  await page.getByRole('option', { name: ja.storeDelivery_application, exact: true }).click()
  const restore = page.waitForResponse(reply => reply.url().endsWith('/api/v1/storages/' + name) && reply.request().method() === 'PUT')
  await editor.getByRole('button', { name: ja.save, exact: true }).click()
  expect((await restore).status()).toBe(200)
  expect((await (await request.get(url)).json()).file_delivery_prefix).toBeNull()
  expect(errors).toEqual([])
})
