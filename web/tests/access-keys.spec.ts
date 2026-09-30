import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('create, save and revoke a scoped token through styled controls', async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const login = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const label = 'ui-token-' + randomUUID().slice(0, 8), errors: string[] = []
  let tokenId = ''
  page.on('pageerror', error => errors.push(error.message))
  page.on('console', message => { if (message.type() === 'error' && /Content Security Policy|Refused to/i.test(message.text())) errors.push(message.text()) })
  try {
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/overview$/); await page.goto('/media')
    await page.goto('/tokens')
    await page.getByRole('button', { name: en.createToken, exact: true }).click()
    const dialog = page.getByRole('dialog', { name: en.createToken, exact: true })
    await dialog.getByRole('textbox', { name: en.keyLabel, exact: true }).fill(label)
    await dialog.getByRole('combobox', { name: en.expires, exact: true }).click()
    await expect(page.getByRole('listbox')).toHaveCSS('border-radius', '15px')
    await page.getByRole('option', { name: en.expiry_30d, exact: true }).click()
    const bucket = dialog.getByRole('group', { name: 'media', exact: true })
    await bucket.getByRole('checkbox', { name: en['action_bucket.list'], exact: true }).check()
    await bucket.getByRole('checkbox', { name: en['action_object.read'], exact: true }).check()
    await expect(dialog.locator('select')).toHaveCount(0)
    const created = page.waitForResponse(response => response.url().endsWith('/api/tokens') && response.request().method() === 'POST' && response.status() === 201)
    await dialog.getByRole('button', { name: en.createToken, exact: true }).click()
    const data = await (await created).json()
    tokenId = data.token.id
    const secret = page.getByRole('dialog', { name: en.saveSecret, exact: true })
    await expect(secret.getByRole('textbox', { name: en.secretValue, exact: true })).toHaveValue(data.secret)
    await expect(secret.getByRole('button', { name: en.done, exact: true })).toBeDisabled()
    await page.keyboard.press('Escape')
    await expect(secret).toBeVisible()
    await secret.getByRole('checkbox', { name: en.secretSaved, exact: true }).check()
    await secret.getByRole('button', { name: en.done, exact: true }).click()
    await expect(secret).toHaveCount(0)
    await expect(page.getByText(data.secret, { exact: true })).toHaveCount(0)
    const listed = page.locator('.key-row').filter({ hasText: label })
    await listed.click()
    const detail = page.getByRole('dialog', { name: label, exact: true })
    await expect(detail.getByText(en['action_object.read'], { exact: true })).toBeVisible()
    await page.screenshot({ path: '/results/step07-token-light.png', fullPage: true })
    await detail.getByRole('button', { name: en.revokeKey, exact: true }).click()
    const revoke = page.getByRole('dialog', { name: en.revokeKey, exact: true })
    await revoke.getByRole('textbox').fill(label)
    await revoke.getByRole('button', { name: en.revokeKey, exact: true }).click()
    await expect(revoke).toHaveCount(0)
    await expect(listed.getByText(en.keyInactive, { exact: true })).toBeVisible()
    expect(errors).toEqual([])
  } finally { if (tokenId) await request.delete('/api/tokens/' + tokenId, { headers }) }
})
