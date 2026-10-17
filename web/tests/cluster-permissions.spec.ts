import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'

test('signing out clears cluster data before a member signs in', async ({ page }) => {
  const origin=process.env.MOKYU_TEST_WEB!, request=page.context().request
  const password=readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const member=JSON.parse(readFileSync(process.env.MOKYU_TEST_OBSERVATIONS_FILE!, 'utf8'))
  const login=await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.ok()).toBe(true)
  const headers={ Origin:origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  expect((await request.put('/api/v1/account', { headers, data: { display_name:'',locale:'en',theme:'light',avatar_email:'',avatar_enabled:false } })).ok()).toBe(true)
  await page.goto('/service')
  await expect(page.locator('.node-row').first()).toBeVisible()
  await page.getByRole('button', { name:'Account menu', exact:true }).click()
  await page.getByRole('menuitem', { name:'Sign out', exact:true }).click()
  await expect(page).toHaveURL(/\/login$/)
  const calls:string[]=[]
  page.on('request', r => { if (new URL(r.url()).pathname.includes('/cluster/')) calls.push(r.url()) })
  await page.locator('input[name="username"]').fill(member.member)
  await page.locator('input[name="password"]').fill(member.password)
  await page.locator('.login-card button.primary').click()
  await expect(page).toHaveURL(/\/overview$/)
  await expect(page.locator('.cluster-scopes, .node-picker, .node-row, .runtime-coverage')).toHaveCount(0)
  await expect(page.locator('a[href="/service"], a[href="/runtime-config"]')).toHaveCount(0)
  await page.goto('/runtime-config?view=node&node='+member.node)
  await expect(page).toHaveURL(/\/media$/)
  expect(calls).toEqual([])
})
