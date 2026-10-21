import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('SQLite synchronization stays in the draft and reports the running value', async ({ page, request }, info) => {
  test.setTimeout(120_000)
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const origin = process.env.MOKYU_TEST_WEB!
  const auth = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(auth.status()).toBe(200)
  const headers = { Origin: origin, 'X-CSRF-Token': (await auth.json()).csrf_token }
  const url = '/api/v1/runtime-settings/io'
  const initial = await (await request.get(url)).json()
  expect(initial.value.sqlite_synchronous).toBe('full')
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
      await page.goto('/runtime-config')
      const before = await (await request.get(url)).json()
      const section = page.locator('.runtime-section-list article').filter({ hasText: text.runtimeGroupIo })
      await section.getByRole('button', { name: text.editRuntimeSettings, exact: true }).click()
      const editor = page.getByRole('dialog', { name: text.runtimeGroupIo, exact: true })
      const select = editor.getByRole('combobox', { name: text.runtimeField_sqlite_synchronous, exact: true })
      await select.click()
      await page.getByRole('option', { name: text.sqliteSync_normal, exact: true }).click()
      expect((await (await request.get(url)).json()).revision).toBe(before.revision)
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 900 })
        await select.scrollIntoViewIfNeeded()
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      }
      await editor.screenshot({ path: info.outputPath('sqlite-' + locale + '-' + theme + '-320.png') })
      await editor.getByRole('button', { name: text.cancel, exact: true }).click()
      expect((await (await request.get(url)).json()).revision).toBe(before.revision)
      await section.getByRole('button', { name: text.editRuntimeSettings, exact: true }).click()
      await select.focus()
      await select.press('Space')
      await page.getByRole('option', { name: text.sqliteSync_off, exact: true }).focus()
      await page.keyboard.press('Enter')
      const saved = page.waitForResponse(reply => reply.url().endsWith('/runtime-settings/io') && reply.request().method() === 'PUT')
      await editor.getByRole('button', { name: text.save, exact: true }).click()
      expect((await saved).status()).toBe(200)
      await expect(editor).toHaveCount(0)
      const after = await (await request.get(url)).json()
      expect(after.value.sqlite_synchronous).toBe('off')
      expect(after.restart_required).toBe(true)
      const observed = await (await request.get('/api/v1/system/config')).json()
      expect(observed.find((row: { key: string }) => row.key === 'database.synchronous').value).toBe('full')
      expect(observed.find((row: { key: string }) => row.key === 'database.journal_mode').value).toBe('wal')
      expect(await page.getByRole('tab').count()).toBe(0)
    }
  }
  expect(errors).toEqual([])
})
