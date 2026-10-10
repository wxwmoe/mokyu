import { readFileSync } from 'node:fs'
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

test('storage details keep maintenance behind confirmation and fit a phone viewport', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await login(page)
  await page.goto('/storages')
  await expect(page.getByRole('heading', { name: en.storages, exact: true })).toBeVisible()
  const card = page.locator('.store-card').filter({ hasText: 'demo' })
  await expect(card).toBeVisible()
  await card.getByRole('link', { name: en.storeDetails, exact: true }).click()
  await expect(page.getByRole('heading', { name: 'demo', exact: true })).toBeVisible()
  await page.screenshot({ path: '/work/test-results/storage-details-desktop.png', fullPage: true })
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
  await expect(editor.getByText(en.profile_connect_timeout, { exact: true })).toBeVisible()
  await expect(editor.getByText('connect_timeout', { exact: true })).toHaveCount(0)
  await editor.getByRole('button', { name: en.cancel, exact: true }).click()
  expect(errors).toEqual([])
  await expect(editor).toHaveCount(0)

  await page.setViewportSize({ width: 390, height: 844 })
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.screenshot({ path: '/work/test-results/storage-details-mobile.png', fullPage: true })
})
