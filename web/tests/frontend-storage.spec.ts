import { openStorageGroup } from './storage-form'
import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect, type Page } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

const password = () => (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
async function login(page: Page) {
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password())
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/overview$/)
}

test('storage details keep maintenance behind confirmation and fit a phone viewport', async ({ page }, info) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await login(page)
  await page.goto('/storages')
  await expect(page.getByRole('heading', { name: en.storages, exact: true })).toBeVisible()
  const card = page.locator('.store-card').filter({ hasText: 'demo' })
  await expect(card).toBeVisible()
  await card.getByRole('link', { name: en.storeDetails, exact: true }).click()
  await expect(page.getByRole('heading', { name: 'demo', exact: true })).toBeVisible()
  await page.screenshot({ path: info.outputPath('storage-details-desktop.png'), fullPage: true })
  await page.getByRole('button', { name: en.checkStore, exact: true }).click()
  const check = page.getByRole('dialog', { name: en.checkStore, exact: true })
  await expect(check).toBeVisible()
  await expect(check.getByRole('button', { name: en.checkStore, exact: true })).toBeDisabled()
  await check.getByRole('textbox').fill('demo')
  await check.getByRole('button', { name: en.checkStore, exact: true }).click()
  await expect(check).toHaveCount(0)
  await expect(page.getByText(en.storeChecked, { exact: true })).toBeVisible()

  await page.getByRole('button', { name: en.editStore, exact: true }).first().click()
  const editor = page.getByRole('dialog', { name: en.editStore, exact: true })
  await openStorageGroup(editor, 'storeGroupLifecycle')
  await expect(editor.getByText(en.profile_connect_timeout, { exact: true })).toBeVisible()
  await expect(editor.getByText('connect_timeout', { exact: true })).toHaveCount(0)
  await openStorageGroup(editor, 'storeGroupCache')
  await expect(editor.locator('[data-profile-group="storeGroupCache"]').getByText(en.storeOptionUnused, { exact: true }).first()).toBeVisible()
  const unavailableInput = editor.locator('.profile-fields input:disabled').first()
  await expect(unavailableInput).toBeVisible()
  expect(await unavailableInput.evaluate(element => getComputedStyle(element).cursor)).toBe('not-allowed')
  await editor.getByRole('button', { name: en.cancel, exact: true }).click()
  expect(errors).toEqual([])
  await expect(editor).toHaveCount(0)

  await page.setViewportSize({ width: 390, height: 844 })
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.screenshot({ path: info.outputPath('storage-details-mobile.png'), fullPage: true })
})

test('local storage can be disabled and restored, and invalid settings explain the failure', async ({ page, request }) => {
  const origin = process.env.MOKYU_TEST_WEB!
  const auth = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password: password() } })
  const headers = { Origin: origin, 'X-CSRF-Token': (await auth.json()).csrf_token }
  await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const path = '/api/v1/storages/demo'
  const storages = await (await request.get('/api/v1/storages')).json()
  expect(storages.filter((row: { enabled: boolean }) => row.enabled).map((row: { name: string }) => row.name)).toEqual(['demo'])
  const original = storages.find((row: { name: string }) => row.name === 'demo')
  await login(page)
  await page.goto('/storages')
  const card = page.locator('.store-card').filter({ hasText: 'demo' })
  try {
    const disabled = page.waitForResponse(reply => reply.url().endsWith(path) && reply.request().method() === 'PUT')
    await card.getByRole('button', { name: en.disableStore, exact: true }).click()
    expect((await disabled).status()).toBe(200)
    await expect(card.getByRole('button', { name: en.enableStore, exact: true })).toBeVisible()
    const enabled = page.waitForResponse(reply => reply.url().endsWith(path) && reply.request().method() === 'PUT')
    await card.getByRole('button', { name: en.enableStore, exact: true }).click()
    expect((await enabled).status()).toBe(200)
    await expect(card.getByRole('button', { name: en.disableStore, exact: true })).toBeVisible()

    const profile = await (await request.get(path + '/profile')).json()
    for (const key of ['cache_enabled', 'upload_cache', 'pack_enabled', 'pack_range_enabled']) expect(profile[key]).toBe(false)
    const invalid = await request.put(path + '/profile', { headers, data: { ...profile, cache_enabled: true } })
    expect(invalid.status()).toBe(400)
    expect((await invalid.json()).code).toBe('LocalStorageFeatures')
    const stale = await request.put(path + '/profile', { headers, data: { ...profile, revision: profile.revision - 1 } })
    expect(stale.status()).toBe(409)
    expect((await stale.json()).code).toBe('StorageProfileConflict')

    await card.getByRole('button', { name: en.editStore, exact: true }).click()
    const editor = page.getByRole('dialog', { name: en.editStore, exact: true })
    await editor.getByRole('textbox', { name: en.storePath, exact: true }).fill('/data/missing-storage-' + randomUUID())
    const rejected = page.waitForResponse(reply => reply.url().endsWith(path) && reply.request().method() === 'PUT')
    await editor.getByRole('button', { name: en.save, exact: true }).click()
    const failure = await rejected
    expect(failure.status()).toBe(409)
    const body = await failure.json()
    expect(body.code).toBe('StorageVerificationFailed')
    expect(body.request_id).toBeTruthy()
    await expect(page.getByText(en.error_StorageVerificationFailed, { exact: true })).toBeVisible()
    const unchanged = (await (await request.get('/api/v1/storages')).json()).find((row: { name: string }) => row.name === 'demo')
    expect(unchanged.path).toBe(original.path)
    expect(unchanged.profile_revision).toBe(profile.revision)
    await editor.getByRole('button', { name: en.cancel, exact: true }).click()
  } finally {
    expect((await request.put(path, { headers, data: { enabled: true } })).status()).toBe(200)
  }
})
