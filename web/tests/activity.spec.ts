import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('filter the journal, inspect an event and export its visible page', async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const reply = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await reply.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const name = 'ui-audit-' + randomUUID().slice(0, 8)
  const created = await request.post('/api/projects', { headers, data: { name } })
  expect(created.status()).toBe(201)
  const project = (await created.json()).id
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  try {
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/overview$/); await page.goto('/media')
    await page.getByRole('link', { name: en.audit, exact: true }).click()
    await page.getByRole('combobox', { name: en.auditAction, exact: true }).click()
    await page.getByRole('option', { name: en.auditGroup_project, exact: true }).click()
    const row = page.locator('.audit-row').filter({ hasText: name })
    await expect(row).toBeVisible()
    await expect(row.locator('.audit-icon')).toHaveCSS('color', 'rgb(71, 126, 112)')
    await expect(page.locator('select')).toHaveCount(0)
    await row.click()
    const dialog = page.getByRole('dialog', { name: en['auditAction_project.create'], exact: true })
    await expect(dialog).toBeVisible()
    await expect(dialog.getByText(created.headers()['x-request-id'], { exact: true })).toBeVisible()
    await dialog.getByText(en.auditTechnical, { exact: true }).click()
    await expect(dialog.locator('.audit-json')).toContainText(name)
    await page.screenshot({ path: '/results/step08-audit-light.png', fullPage: true })
    await page.keyboard.press('Escape')
    const pending = page.waitForEvent('download')
    await page.getByRole('button', { name: en.auditExport, exact: true }).click()
    const download = await pending
    expect(download.suggestedFilename()).toBe('mokyu-audit.jsonl')
    const lines = readFileSync((await download.path())!, 'utf8').trim().split('\n').map(value => JSON.parse(value))
    expect(lines.some(event => event.detail.name === name)).toBeTruthy()
    expect(lines.every(event => event.action.startsWith('project.'))).toBeTruthy()
    expect(errors).toEqual([])
  } finally { await request.delete('/api/projects/' + project, { headers }) }
})
