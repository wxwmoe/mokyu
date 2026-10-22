import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('file durability stays in the draft and saves only for the selected storage', async ({ page, request }, info) => {
  test.setTimeout(120_000)
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const origin = process.env.MOKYU_TEST_WEB!
  const auth = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(auth.status()).toBe(200)
  const headers = { Origin: origin, 'X-CSRF-Token': (await auth.json()).csrf_token }
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const name = 'buffered-ui-' + randomUUID().slice(0, 8)
  const created = await request.post('/api/v1/storages', { headers, data: { name, type: 'local', path: process.env.MOKYU_TEST_FSYNC_ROOT + '/' + name, initialize: true } })
  expect(created.status()).toBe(201)
  const url = '/api/v1/storages/' + name + '/profile'
  const original = await (await request.get(url)).json()
  expect(original.write_durability).toBe('durable')
  const database = await (await request.get('/api/v1/runtime-settings/io')).json()
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
      const select = editor.getByRole('combobox', { name: text.profile_write_durability, exact: true })
      await expect(select).toBeVisible()
      await expect(editor.locator('#storage-durability-hint')).toHaveText(text.storeDurabilityHint)
      await select.click()
      await page.getByRole('option', { name: text.storeDurability_buffered, exact: true }).click()
      expect((await (await request.get(url)).json()).write_durability).toBe('durable')
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 900 })
        await select.scrollIntoViewIfNeeded()
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      }
      await editor.screenshot({ path: info.outputPath('buffered-' + locale + '-' + theme + '-320.png') })
      await editor.getByRole('button', { name: text.cancel, exact: true }).click()
      expect((await (await request.get(url)).json()).revision).toBe(original.revision)
    }
  }
  await page.goto('/storages?edit=' + name)
  const editor = page.getByRole('dialog', { name: ja.editStore, exact: true })
  const select = editor.getByRole('combobox', { name: ja.profile_write_durability, exact: true })
  await select.focus()
  await select.press('Space')
  await page.getByRole('option', { name: ja.storeDurability_buffered, exact: true }).focus()
  await page.keyboard.press('Enter')
  const saved = page.waitForResponse(reply => reply.url().endsWith('/api/v1/storages/' + name) && reply.request().method() === 'PUT')
  await editor.getByRole('button', { name: ja.save, exact: true }).click()
  expect((await saved).status()).toBe(200)
  expect((await (await request.get(url)).json()).write_durability).toBe('buffered')
  expect((await (await request.get(url)).json()).revision).toBe(original.revision + 1)
  expect(await (await request.get('/api/v1/runtime-settings/io')).json()).toEqual(database)
  expect(errors).toEqual([])
})
