import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('edit project quotas with styled units and explicit zero versus unlimited', async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const login = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const name = 'ui-quota-' + randomUUID().slice(0, 8)
  const project = await (await request.post('/api/projects', { headers, data: { name } })).json()
  const path = `/api/quotas/project/${project.id}`
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  try {
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/overview$/); await page.goto('/media')
    await page.goto('/projects?project=' + project.id)
    await page.getByRole('button', { name: en.quotaEdit, exact: true }).click()
    const dialog = page.getByRole('dialog', { name: en.quotaEdit, exact: true })
    await dialog.getByRole('textbox', { name: new RegExp(en.quotaLogicalLimit) }).fill('1.5')
    await dialog.getByRole('combobox', { name: en.quotaUnit, exact: true }).first().click()
    await page.getByRole('option', { name: 'MiB', exact: true }).click()
    await dialog.getByRole('textbox', { name: en.quotaBucketLimit, exact: true }).fill('0')
    await expect(page.locator('select')).toHaveCount(0)
    await page.screenshot({ path: '/results/step09-quotas.png', fullPage: true })
    await dialog.getByRole('button', { name: en.save, exact: true }).click()
    await expect(dialog).not.toBeVisible()
    let saved = await (await request.get(path)).json()
    expect([saved.byte_limit, saved.inflight_limit, saved.bucket_limit]).toEqual(['1572864', null, '0'])
    await page.getByRole('button', { name: en.quotaEdit, exact: true }).click()
    await dialog.getByRole('textbox', { name: new RegExp(en.quotaLogicalLimit) }).fill('')
    await dialog.getByRole('textbox', { name: en.quotaBucketLimit, exact: true }).fill('')
    await dialog.getByRole('button', { name: en.save, exact: true }).click()
    await expect(dialog).not.toBeVisible()
    saved = await (await request.get(path)).json()
    expect([saved.byte_limit, saved.bucket_limit]).toEqual([null, null])
    expect(errors).toEqual([])
  } finally {
    await request.delete('/api/projects/' + project.id, { headers })
  }
})
