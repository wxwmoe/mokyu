import { randomUUID } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('mirroring drafts, preview, revision conflicts and localized layout', async ({ page, request }, info) => {
  test.setTimeout(150000)
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const username = process.env.MOKYU_TEST_USERNAME || 'tester'
  const origin = process.env.MOKYU_TEST_MIRROR_ORIGIN || 'https://origin.example/assets/'
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username, password } })
  expect(login.status()).toBe(200)
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  async function language(locale: string, theme: string) {
    expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).status()).toBe(200)
  }
  await language('en', 'light')
  const options = await (await request.get('/api/v1/bucket-projects')).json()
  const name = 'ui-mirror-' + randomUUID().slice(0, 8)
  const response = await request.post('/api/v1/buckets', { headers, data: { name, project_id: options.projects[0].id, storage_id: options.storages[0].id } })
  expect(response.status()).toBe(201)
  const bucket = await response.json(), base = `/api/v1/buckets/${bucket.id}/mirroring`
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  try {
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill(username)
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/overview$/)
    await page.goto('/buckets/' + bucket.id)
    await page.getByRole('tab', { name: en.mirrorTitle }).click()
    await expect(page.getByRole('button', { name: en.mirrorAddRule })).toHaveCount(0)
    await page.getByText(en.mirrorActivity, { exact: true }).click()
    await expect(page.getByText(en.mirrorNoTasks, { exact: true })).toBeVisible()
    await page.screenshot({ path: info.outputPath('mirroring-disabled.png'), fullPage: true })
    await page.getByText(en.mirrorActivity, { exact: true }).click()
    await page.getByRole('switch', { name: en.mirrorEnable }).click()
    await page.getByRole('button', { name: en.mirrorAddRule }).click()
    const editor = page.getByRole('dialog', { name: en.mirrorEditRule, exact: true })
    await expect(editor.getByRole('textbox', { name: en.mirrorRuleName, exact: true })).toBeFocused()
    await editor.getByRole('textbox', { name: en.mirrorRuleName, exact: true }).fill('Media origin')
    await editor.getByRole('textbox', { name: en.mirrorPrefix, exact: true }).fill('media/')
    await editor.getByRole('textbox', { name: en.mirrorAddPrefix, exact: true }).fill('archive/')
    await editor.getByRole('checkbox', { name: en.mirrorStripPrefix }).check()
    await editor.getByRole('textbox', { name: en.mirrorUrl }).fill(origin)
    if (origin.startsWith('http:')) {
      await editor.getByText(en.mirrorConnectionOptions, { exact: true }).click()
      await editor.getByRole('checkbox', { name: en.mirrorAllowHttp }).check()
      await editor.getByRole('checkbox', { name: en.mirrorAllowPrivate }).check()
    }
    await editor.getByRole('button', { name: en.mirrorApplyRule }).click()
    await expect(editor).toHaveCount(0)
    await expect(page.getByRole('button', { name: en.mirrorAddRule })).toBeFocused()
    await page.getByRole('tab', { name: en.imageTitle, exact: true }).click()
    await page.getByRole('dialog', { name: en.unsavedChanges }).getByRole('button', { name: en.keepEditing }).click()
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect.poll(async () => (await (await request.get(base)).json()).config.enabled).toBe(true)
    await page.getByRole('textbox', { name: en.mirrorObjectKey, exact: true }).fill('media/photos/a.jpg')
    await page.getByRole('button', { name: en.mirrorPreview, exact: true }).click()
    await expect(page.locator('.mirror-path')).toContainText(origin.replace(/\/$/, '') + '/archive/photos/a.jpg')
    if (process.env.MOKYU_TEST_MIRROR_ORIGIN) {
      await page.getByRole('button', { name: en.mirrorTest, exact: true }).click()
      await expect(page.getByText(en.mirrorTestPassed, { exact: true })).toBeVisible()
    }
    // Another editor wins; the rejected draft must remain editable and refreshable.
    const remote = await (await request.get(base)).json()
    expect((await request.put(base, { headers, data: remote })).status()).toBe(200)
    await page.getByRole('switch', { name: en.mirrorEnable }).click()
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect(page.getByText(en.error_PreconditionFailed, { exact: true })).toBeVisible()
    await page.locator('.page-heading').getByRole('button', { name: en.refresh, exact: true }).click()
    await page.getByRole('dialog', { name: en.unsavedChanges }).getByRole('button', { name: en.discard }).click()
    await expect(page.getByRole('switch', { name: en.mirrorEnable })).toBeChecked()
    await expect(page.getByText(en.error_PreconditionFailed, { exact: true })).toHaveCount(0)
    await page.getByRole('tab', { name: en.imageTitle, exact: true }).click()
    await expect(page.getByRole('dialog', { name: en.unsavedChanges })).toHaveCount(0)

    for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
      for (const theme of ['light', 'dark']) {
        await language(locale, theme)
        await page.reload()
        await page.getByRole('tab', { name: text.mirrorTitle, exact: true }).click()
        await page.getByText(text.mirrorActivity, { exact: true }).click()
        await expect(page.getByText(text.mirrorNoTasks, { exact: true })).toBeVisible()
        for (const width of [1440, 1024, 768, 640, 390, 320]) {
          await page.setViewportSize({ width, height: 960 })
          expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
          if (width === 1440 || width === 320) await page.screenshot({ path: info.outputPath(`mirroring-page-${locale}-${theme}-${width}.png`), fullPage: true })
          await page.getByRole('button', { name: text.mirrorEdit, exact: true }).click()
          const dialog = page.getByRole('dialog', { name: text.mirrorEditRule, exact: true })
          await expect(dialog).toBeVisible()
          expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
          expect(await dialog.evaluate(el => el.scrollWidth <= el.clientWidth + 1)).toBe(true)
          await expect(dialog.getByRole('combobox', { name: text.mirrorSourceType })).toBeVisible()
          expect(await dialog.locator('select').count()).toBe(0)
          if (width === 1440 || width === 320) await page.screenshot({ path: info.outputPath(`mirroring-${locale}-${theme}-${width}.png`) })
          await page.keyboard.press('Escape')
          await expect(dialog).toHaveCount(0)
          await expect(page.getByRole('button', { name: text.mirrorEdit, exact: true })).toBeFocused()
        }
      }
    }
    expect(errors).toEqual([])
  } finally {
    await language('en', 'light')
    await request.delete('/api/v1/buckets/' + bucket.id, { headers, data: { confirm_name: name, confirmation: '' } })
  }
})

test('S3 origins keep independent literal prefixes through editing and preview', async ({ page, request }, info) => {
  test.setTimeout(90000)
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const username = process.env.MOKYU_TEST_USERNAME || 'tester'
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username, password } })
  expect(login.status()).toBe(200)
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  const options = await (await request.get('/api/v1/bucket-projects')).json()
  const name = 'ui-prefix-' + randomUUID().slice(0, 8)
  const created = await request.post('/api/v1/buckets', { headers, data: { name, project_id: options.projects[0].id, storage_id: options.storages[0].id } })
  expect(created.status()).toBe(201)
  const bucket = await created.json(), base = `/api/v1/buckets/${bucket.id}/mirroring`
  const connection = await request.post('/api/v1/external-buckets', { headers, data: {
    name, project_id: bucket.project_id,
    connection: { endpoint_url: 'https://storage.example.invalid', region: 'us-east-1', bucket: 'physical', prefix: 'imports' },
    access_key: 'fixture-access', secret_key: randomUUID(),
  } })
  expect(connection.status()).toBe(201)
  const external = await connection.json()
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  try {
    await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill(username)
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/overview$/)
    await page.goto('/buckets/' + bucket.id)
    await page.getByRole('tab', { name: en.mirrorTitle }).click()
    await page.getByRole('switch', { name: en.mirrorEnable }).click()
    await page.getByRole('button', { name: en.mirrorAddRule }).click()
    const editor = page.getByRole('dialog', { name: en.mirrorEditRule, exact: true })
    await editor.getByRole('textbox', { name: en.mirrorRuleName, exact: true }).fill('Primary and backup')
    await editor.getByRole('textbox', { name: en.mirrorPrefix, exact: true }).fill('media/')
    await editor.getByRole('textbox', { name: en.mirrorAddPrefix, exact: true }).fill('archive/')
    await editor.getByRole('checkbox', { name: en.mirrorStripPrefix }).check()
    await editor.getByRole('checkbox', { name: en.mirrorBackup, exact: true }).check()
    for (const [index, prefix] of ['primary/', 'fallback-'].entries()) {
      const section = editor.locator('.mirror-editor-source').nth(index)
      await section.getByRole('combobox', { name: en.mirrorSourceType }).click()
      await page.getByRole('option', { name: en.externalBuckets, exact: true }).click()
      await section.getByRole('combobox', { name: en.externalBuckets }).click()
      await page.getByRole('option', { name, exact: true }).click()
      await section.getByRole('textbox', { name: en.mirrorSourcePrefix, exact: true }).fill(prefix)
      await expect(section.locator('.mirror-source-location code')).toHaveText('s3://physical/imports/' + prefix)
    }
    await page.screenshot({ path: info.outputPath('mirroring-s3-sources.png') })
    await editor.getByRole('button', { name: en.mirrorApplyRule }).click()
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect.poll(async () => (await (await request.get(base)).json()).config.rules[0]?.backup?.prefix).toBe('fallback-')
    await page.reload()
    await page.getByRole('tab', { name: en.mirrorTitle }).click()
    await page.getByRole('textbox', { name: en.mirrorObjectKey, exact: true }).fill('media/photos/a +%2F.jpg')
    await page.getByRole('button', { name: en.mirrorPreview, exact: true }).click()
    await expect(page.locator('.mirror-path')).toContainText('s3://physical/imports/primary/archive/photos/a +%2F.jpg')
    await page.getByRole('button', { name: en.mirrorEdit, exact: true }).click()
    const field = editor.getByRole('textbox', { name: en.mirrorSourcePrefix, exact: true }).first()
    await expect(field).toHaveValue('primary/')
    await expect(editor.getByRole('textbox', { name: en.mirrorSourcePrefix, exact: true }).last()).toHaveValue('fallback-')
    await field.fill('')
    await editor.getByRole('button', { name: en.mirrorApplyRule }).click()
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect.poll(async () => (await (await request.get(base)).json()).config.rules[0].source.prefix).toBe('')
    for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
      await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme: 'dark', avatar_email: '', avatar_enabled: false } })
      await page.reload()
      await page.getByRole('tab', { name: text.mirrorTitle }).click()
      for (const width of [1440, 320]) {
        await page.setViewportSize({ width, height: 960 })
        await page.getByRole('button', { name: text.mirrorEdit, exact: true }).click()
        const dialog = page.getByRole('dialog', { name: text.mirrorEditRule, exact: true })
        await expect(dialog.getByRole('textbox', { name: text.mirrorRuleName, exact: true })).toBeFocused()
        expect(await dialog.evaluate(el => el.scrollWidth <= el.clientWidth + 1)).toBe(true)
        await page.screenshot({ path: info.outputPath(`mirroring-s3-${locale}-${width}.png`) })
        await page.keyboard.press('Escape')
        await expect(dialog).toHaveCount(0)
      }
    }
    expect(errors).toEqual([])
  } finally {
    await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
    await request.delete('/api/v1/buckets/' + bucket.id, { headers, data: { confirm_name: name, confirmation: '' } })
    await request.delete('/api/v1/external-buckets/' + external.id, { headers, data: { revision: external.revision } })
  }
})
