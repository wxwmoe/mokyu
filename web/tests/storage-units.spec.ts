import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import { openStorageGroup } from './storage-form'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
  test(`storage quantities keep exact thresholds and explicit units (${locale})`, async ({ page }, info) => {
    test.setTimeout(120_000)
    const request = page.context().request
    const origin = process.env.MOKYU_TEST_WEB!
    const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
    const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
    expect(login.status()).toBe(200)
    const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
    await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme: 'light', avatar_email: '', avatar_enabled: false } })
    const name = 'units-' + randomUUID().slice(0, 8), path = '/api/v1/storages/' + name
    expect((await request.post('/api/v1/storages', { headers, data: { name, type: 'local', path: process.env.MOKYU_TEST_FSYNC_ROOT + '/' + name, initialize: true } })).status()).toBe(201)
    const original = await (await request.get(path + '/profile')).json()
    const errors: string[] = []
    page.on('pageerror', error => errors.push(error.message))
    await page.goto('/storages?edit=' + name)
    const editor = page.getByRole('dialog', { name: text.editStore, exact: true })
    await openStorageGroup(editor, 'storeGroupCache')
    const size = editor.getByRole('textbox', { name: text.profile_compression_min_savings_bytes, exact: true })
    const percent = editor.getByRole('spinbutton', { name: text.profile_compression_min_savings_percent, exact: true })
    await expect(size).toHaveValue('256B')
    await expect(percent).toHaveValue('2')
    await expect(editor.locator('#storage-unit-compression_min_savings_percent')).toHaveText('%')
    let writes = 0
    page.on('request', request => { if (request.url().endsWith(path) && request.method() === 'PUT') writes++ })
    for (const invalid of ['-1B', '0.1B', 'NaN', '256XB', '9007199254740992B', '1e3B', '']) {
      await size.fill(invalid)
      await editor.getByRole('button', { name: text.save, exact: true }).click()
      await expect(size).toBeFocused()
      expect(await size.evaluate((element: HTMLInputElement) => element.checkValidity())).toBe(false)
      expect(writes).toBe(0)
    }
    for (const [input, formatted, expected] of [['0B', '0B', 0], ['256KiB', '256KiB', 262144], ['1.5KiB', '1536B', 1536], ['1MB', '1000000B', 1000000], ['0.25KiB', '256B', 256], ['256', '256B', 256]] as const) {
      await size.fill(input)
      await percent.focus()
      await expect(size).toHaveValue(formatted)
      const saved = page.waitForRequest(request => request.url().endsWith(path) && request.method() === 'PUT')
      const reply = page.waitForResponse(response => response.url().endsWith(path) && response.request().method() === 'PUT')
      await editor.getByRole('button', { name: text.save, exact: true }).click()
      expect((await saved).postDataJSON().profile.compression_min_savings_bytes).toBe(expected)
      expect((await reply).status()).toBe(200)
      const profile = await (await request.get(path + '/profile')).json()
      expect(profile.compression_min_savings_bytes).toBe(expected)
      expect(profile.compression_min_savings_percent).toBe(2)
      await page.goto('/storages?edit=' + name)
      await openStorageGroup(editor, 'storeGroupCache')
      await expect(size).toHaveValue(formatted)
    }
    for (const invalid of ['-1', '101', '2.5']) {
      await percent.fill(invalid)
      const before = writes
      await editor.getByRole('button', { name: text.save, exact: true }).click()
      await expect(percent).toBeFocused()
      expect(writes).toBe(before)
    }
    await percent.fill('2')
    await openStorageGroup(editor, 'storeGroupPacking')
    await expect(editor.locator('#storage-unit-pack_range_min_savings_percent')).toHaveText('%')
    await expect(editor.getByRole('textbox', { name: text.profile_pack_range_min_savings_bytes, exact: true })).toHaveValue('64MiB')
    await openStorageGroup(editor, 'storeGroupWorker')
    await expect(editor.locator('#storage-unit-integrity_bandwidth')).toHaveText('/s')
    await expect(editor.locator('#storage-unit-integrity_rps')).toHaveText(text.unitRequestsPerSecond)
    await editor.getByRole('button', { name: text.cancel, exact: true }).click()
    const after = await (await request.get(path + '/profile')).json()
    expect({ ...after, revision: original.revision }).toEqual(original)
    for (const theme of ['light', 'dark']) {
      await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })
      await page.goto('/storages?edit=' + name)
      await openStorageGroup(editor, 'storeGroupCache')
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 1000 })
        await size.scrollIntoViewIfNeeded()
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
        if (width === 1440 || width === 320) await editor.screenshot({ path: info.outputPath(`units-${theme}-${width}.png`) })
      }
      await editor.getByRole('button', { name: text.cancel, exact: true }).click()
    }
    await page.goto('/runtime-config')
    await page.locator('.advanced-config summary').click()
    for (const key of ['effective.memory_bytes', 'effective.slot_bytes', 'effective.response_cache_bytes', 'effective.write_memory_bytes', 'runtime.memory_available_at_start']) {
      const row = page.locator('.config-fields article').filter({ has: page.locator('code', { hasText: new RegExp('^' + key.replace('.', '\\.') + '$') }) })
      await expect(row.locator('.config-value-chip')).toHaveText(/^\d+(?:KiB|MiB|GiB|TiB|PiB|B)$/)
    }
    const task = randomUUID(), now = new Date().toISOString()
    await page.route('**/api/v1/tasks/' + task, route => route.fulfill({ json: { id: task, task_type: 'sweep', policy: 'sweep', status: 'completed', processed: 0, created_by: 'tester', source: 'manual', created_at: now, started_at: now, updated_at: now, detail: { bytes: '262144' } } }))
    await page.goto('/tasks?task=' + task)
    const detail = page.getByRole('dialog', { name: text.taskDetails, exact: true })
    await expect(detail.getByText(text.job_bytes, { exact: true }).locator('xpath=following-sibling::dd[1]')).toHaveText('256KiB')
    expect(errors).toEqual([])
  })
}
