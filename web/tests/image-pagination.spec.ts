import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test.skip(!process.env.MOKYU_TEST_IMAGE_FIXTURE, 'Requires an isolated image bucket')

test('image style pagination handles full pages, filters and an emptied last page', async ({ page }) => {
  test.setTimeout(90000)
  const fixture = JSON.parse(readFileSync(process.env.MOKYU_TEST_IMAGE_FIXTURE!, 'utf8'))
  const origin = process.env.MOKYU_TEST_WEB!
  const login = await page.request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: {
    username: 'tester', password: readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim(),
  } })
  expect(login.status()).toBe(200)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  expect((await page.request.put('/api/v1/account', { headers, data: {
    display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false,
  } })).ok()).toBeTruthy()
  const base = '/api/v1/buckets/' + fixture.id + '/image-processing'
  const prefix = 'Pages.' + randomUUID() + '.'
  const name = (n: number) => prefix + String(n).padStart(3, '0')
  const names = (first: number, last: number) => Array.from({ length: last - first + 1 }, (_, i) => name(first + i))
  const rows = page.locator('.style-table tbody tr')
  const next = page.getByRole('button', { name: en.next, exact: true })
  const previous = page.getByRole('button', { name: en.previous, exact: true })
  let count = 0
  async function edit(edits: object[]) {
    const settings = (await (await page.request.get(base + '/styles?limit=1')).json()).settings
    expect((await page.request.put(base, { headers, data: { revision: settings.revision, edits } })).ok()).toBeTruthy()
  }
  async function search(value: string) {
    await page.getByRole('textbox', { name: en.imageNamePrefix, exact: true }).fill(value)
    await page.getByRole('button', { name: en.imageSearch, exact: true }).click()
  }
  async function expectRows(first: number, last: number) {
    await expect(rows.locator('td:first-child')).toHaveText(names(first, last))
  }
  try {
    for (const total of [0, 49, 50, 51, 100, 101]) {
      if (total > count) {
        await edit(names(count + 1, total).map(name => ({ action: 'put', style: {
          name, enabled: true, rule: { stages: [{
            operations: [{ operation: 'orient' }], format: 'jpeg', quality: 90,
            quality_mode: 'exact', progressive: false,
          }] },
        } })))
        count = total
      }
      await page.goto('/buckets/' + fixture.id)
      await page.getByRole('tab', { name: en.imageTitle, exact: true }).click()
      await search(prefix)
      if (!total) {
        await expect(rows).toHaveCount(0)
        await expect(page.locator('.styles-empty')).toBeVisible()
      } else await expectRows(1, Math.min(50, total))
      await expect(previous).toBeDisabled()
      for (let offset = 50; offset < total; offset += 50) {
        await expect(next).toBeEnabled()
        await next.click()
        await expectRows(offset + 1, Math.min(offset + 50, total))
        await expect(previous).toBeEnabled()
      }
      await expect(next).toBeDisabled()
      if (total === 101) {
        await page.getByRole('button', { name: en.imageDeleteStyle.replace('{name}', name(101)), exact: true }).click()
        await page.getByRole('dialog').getByRole('button', { name: en.imageRemove, exact: true }).click()
        await expect(page.getByRole('dialog')).toHaveCount(0)
        await expectRows(51, 100)
        await expect(next).toBeDisabled()
        count = 100
        await previous.click()
        await expectRows(1, 50)
        await expect(previous).toBeDisabled()
        await next.click()
        await expectRows(51, 100)
        await search(prefix + '0')
        await expectRows(1, 50)
        await expect(previous).toBeDisabled()
        await next.click()
        await expectRows(51, 99)
        await expect(next).toBeDisabled()
        await search(prefix + 'missing')
        await expect(rows).toHaveCount(0)
        await expect(previous).toBeDisabled()
        await expect(next).toBeDisabled()
      }
    }
  } finally {
    if (count) await edit(names(1, count).map(name => ({ action: 'remove', name })))
  }
})
