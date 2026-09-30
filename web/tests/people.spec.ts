import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect, type Page } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

const password = () => (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
async function login(page: Page, username: string, secret: string) {
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill(username)
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(secret)
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
}

test('create a member, grant one bucket, rotate password and browse as that member', async ({ page, browser, request }) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  page.on('console', message => { if (message.type() === 'error' && /Content Security Policy|Refused to/i.test(message.text())) errors.push(message.text()) })
  const username = 'ui-' + randomUUID().slice(0, 12), initial = 'UI-account-' + randomUUID()
  const reply = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password: password() } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await reply.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  let id = ''
  const memberContext = await browser.newContext({ baseURL: process.env.MOKYU_TEST_WEB, locale: 'en-US' })
  try {
    await login(page, 'tester', password())
    await page.getByRole('link', { name: en.users, exact: true }).click()
    await page.getByRole('button', { name: en.createUser, exact: true }).click()
    const creation = page.getByRole('dialog', { name: en.createUser })
    await creation.getByRole('textbox', { name: en.username, exact: true }).fill(username)
    await creation.getByRole('textbox', { name: en.initialPassword, exact: true }).fill(initial)
    await expect(creation.getByRole('checkbox', { name: en.requireChange })).toBeChecked()
    // The API expiry boundary is covered separately; exercise the nested retry dialog here.
    let expired = false
    await page.route('**/api/users', async route => {
      if (route.request().method() === 'POST' && !expired) {
        expired = true
        await route.fulfill({ status: 403, contentType: 'application/json', body: JSON.stringify({ code: 'ReauthenticationRequired', error: 'Forbidden' }) })
      } else await route.continue()
    })
    const created = page.waitForResponse(response => response.url().endsWith('/api/users') && response.request().method() === 'POST' && response.status() === 201)
    await creation.getByRole('button', { name: en.createUser, exact: true }).click()
    const verification = page.getByRole('dialog', { name: en.verifyIdentity, exact: true })
    await verification.getByRole('textbox', { name: en.currentPassword, exact: true }).fill(password())
    await verification.getByRole('button', { name: en.continue, exact: true }).click()
    const result = await created
    expect(result.status()).toBe(201)
    id = (await result.json()).id
    const detail = page.getByRole('dialog', { name: username, exact: true })
    await expect(detail).toBeVisible()
    await detail.getByRole('link', { name: en.manageMembership, exact: true }).click()
    await page.locator('.project-card').filter({ has: page.getByRole('heading', { name: 'Default', exact: true }) }).click()
    await page.getByRole('button', { name: en.addMember, exact: true }).click()
    const membership = page.getByRole('dialog', { name: en.manageMembership, exact: true })
    const grant = membership.getByRole('group', { name: 'media', exact: true })
    await grant.getByRole('checkbox', { name: en['action_bucket.list'], exact: true }).check()
    await grant.getByRole('checkbox', { name: en['action_object.read'], exact: true }).check()
    await grant.getByRole('checkbox', { name: en['action_storage.inspect'], exact: true }).check()
    await expect(membership.locator('select')).toHaveCount(0)
    await membership.getByRole('combobox', { name: en.role, exact: true }).click()
    await expect(page.getByRole('listbox')).toHaveCSS('border-radius', '15px')
    await page.keyboard.press('Escape')
    await membership.getByRole('button', { name: en.save, exact: true }).click()
    await expect(membership).toHaveCount(0)
    await expect(page.locator('.member-row').filter({ hasText: username })).toBeVisible()
    await page.keyboard.press('Escape')
    await page.screenshot({ path: '/results/step06-projects-light.png', fullPage: true })
    const member = await memberContext.newPage()
    member.on('pageerror', error => errors.push(error.message))
    await login(member, username, initial)
    await expect(member).toHaveURL(/\/account$/)
    await expect(member.getByText(en.passwordRequired, { exact: true })).toBeVisible()
    await member.getByRole('textbox', { name: en.currentPassword, exact: true }).fill(initial)
    await member.getByRole('textbox', { name: en.newPassword, exact: true }).fill(initial + '-new')
    await member.getByRole('textbox', { name: en.confirmPassword, exact: true }).fill(initial + '-new')
    await member.getByRole('button', { name: en.passwordChange, exact: true }).click()
    await expect(member.getByText(en.passwordRequired, { exact: true })).toHaveCount(0)
    await member.getByRole('link', { name: en.media, exact: true }).click()
    await expect(member.locator('.bucket-card')).toHaveCount(1)
    await expect(member.getByRole('link', { name: en.users, exact: true })).toHaveCount(0)
    await member.goto('/users')
    await expect(member).toHaveURL(/\/media$/)
    await member.getByRole('link', { name: 'media Active', exact: true }).click()
    await member.getByRole('button', { name: 'small-part', exact: true }).click()
    await expect(member.getByRole('link', { name: en.download })).toBeVisible()
    expect(errors).toEqual([])
  } finally {
    await memberContext.close()
    if (id) expect((await request.delete('/api/users/' + id, { headers })).status()).toBe(204)
  }
})
