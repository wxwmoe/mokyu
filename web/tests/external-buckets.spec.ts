import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test, expect, type Page, type APIRequestContext } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

const password = () => (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
async function login(page: Page, request: APIRequestContext) {
  const reply = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password: password() } })
  expect(reply.status()).toBe(200)
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await reply.json()).csrf_token }
  expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })).ok()).toBeTruthy()
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password())
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/overview$/)
  return headers
}

test('manage external connections without exposing credentials or overwriting concurrent edits', async ({ page, request }, info) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  const headers = await login(page, request)
  const name = 'External ' + randomUUID().slice(0, 8), secret = 'fixture-' + randomUUID()
  let id = '', failed = false
  try {
    await page.getByRole('link', { name: en.externalBuckets, exact: true }).click()
    await page.getByRole('button', { name: en.externalAdd, exact: true }).click()
    const creation = page.getByRole('dialog', { name: en.externalAdd, exact: true })
    await creation.getByRole('textbox', { name: en.name, exact: true }).fill(name)
    await creation.getByRole('textbox', { name: en.externalEndpoint, exact: true }).fill('https://storage.example.invalid')
    await creation.getByRole('textbox', { name: en.bucket, exact: true }).fill('media')
    await creation.getByRole('textbox', { name: en.externalPrefix, exact: true }).fill('imports')
    await creation.getByRole('textbox', { name: en.accessKeyId, exact: true }).fill('fixture-access')
    await creation.getByLabel(en.secretValue, { exact: true }).fill(secret)
    const created = page.waitForResponse(response => response.url().endsWith('/api/v1/external-buckets') && response.request().method() === 'POST')
    await creation.getByRole('button', { name: en.save, exact: true }).click()
    const reply = await created
    expect(reply.status()).toBe(201)
    const value = await reply.json()
    id = value.id
    expect(JSON.stringify(value)).not.toContain(secret)
    expect(value).not.toHaveProperty('access_key')
    const detail = page.getByRole('dialog', { name, exact: true })
    await expect(detail).toBeVisible()
    await detail.getByRole('button', { name: en.externalEdit, exact: true }).click()
    const editor = page.getByRole('dialog', { name: en.externalEdit, exact: true })
    await expect(editor.getByRole('textbox', { name: en.accessKeyId, exact: true })).toHaveValue('')
    await expect(editor.getByLabel(en.secretValue, { exact: true })).toHaveValue('')
    expect((await request.patch('/api/v1/external-buckets/' + id, { headers, data: { revision: value.revision, enabled: false } })).status()).toBe(200)
    await editor.getByRole('textbox', { name: en.externalPrefix, exact: true }).fill('updated')
    const conflict = page.waitForResponse(response => response.url().endsWith('/external-buckets/' + id) && response.request().method() === 'PATCH')
    await editor.getByRole('button', { name: en.save, exact: true }).click()
    expect((await conflict).status()).toBe(409)
    await expect(editor).toBeVisible()
    await editor.getByRole('button', { name: en.cancel, exact: true }).click()
    await page.reload()
    await detail.getByRole('button', { name: en.externalEdit, exact: true }).click()
    await editor.getByRole('textbox', { name: en.externalPrefix, exact: true }).fill('updated')
    await editor.getByRole('checkbox', { name: en.externalEnabled, exact: true }).check()
    const updated = page.waitForResponse(response => response.url().endsWith('/external-buckets/' + id) && response.request().method() === 'PATCH')
    await editor.getByRole('button', { name: en.save, exact: true }).click()
    expect((await updated).status()).toBe(200)
    await expect(detail.getByText('updated', { exact: true })).toBeVisible()
    await page.keyboard.press('Escape')
    await expect(page.getByRole('dialog')).toHaveCount(0)
    await expect(page).toHaveURL(/\/external-buckets$/)
    await page.screenshot({ path: info.outputPath('external-buckets.png'), fullPage: true })
    await test.step('remove the connection with its exact name', async () => {
      await page.locator('.key-row').filter({ hasText: name }).click()
      await detail.getByRole('button', { name: en.externalRemove, exact: true }).click()
      const removal = page.getByRole('dialog', { name: en.externalRemove, exact: true })
      await expect(removal.getByRole('button', { name: en.externalRemove, exact: true })).toBeDisabled()
      await removal.getByRole('textbox').fill(name)
      await removal.getByRole('button', { name: en.externalRemove, exact: true }).click()
      await expect(page.getByRole('dialog')).toHaveCount(0)
      await expect(page.locator('.key-row').filter({ hasText: name })).toHaveCount(0)
    })
    expect(errors).toEqual([])
  } catch (error) {
    failed = true; throw error
  } finally {
    if (id) {
      try {
        const current = await request.get('/api/v1/external-buckets/' + id)
        if (current.ok()) expect((await request.delete('/api/v1/external-buckets/' + id, { headers, data: { revision: (await current.json()).revision } })).status()).toBe(204)
      } catch (error) { if (!failed) throw error }
    }
  }
})

test('grant external buckets to members and API tokens through the existing permission forms', async ({ page, request }) => {
  const headers = await login(page, request)
  const project = (await (await request.get('/api/v1/projects')).json())[0]
  const suffix = randomUUID().slice(0, 8), name = 'External ' + suffix, username = 'external-' + suffix
  let external = '', user = '', token = ''
  try {
    const resource = await request.post('/api/v1/external-buckets', { headers, data: { project_id: project.id, name, connection: { endpoint_url: 'https://storage.example.invalid', region: 'us-east-1', bucket: 'media' }, access_key: 'fixture-access', secret_key: 'fixture-secret' } })
    expect(resource.status()).toBe(201); external = (await resource.json()).id
    const member = await request.post('/api/v1/users', { headers, data: { username, password: 'fixture-' + randomUUID(), role: 'member', must_change_password: false } })
    expect(member.status()).toBe(201); user = (await member.json()).id
    await page.goto(`/projects?project=${project.id}&user=${user}`)
    await page.getByRole('button', { name: en.addMember, exact: true }).click()
    const membership = page.getByRole('dialog', { name: en.manageMembership, exact: true })
    const grants = membership.getByRole('group', { name, exact: true })
    await expect(grants.getByRole('checkbox')).toHaveCount(2)
    await grants.getByRole('checkbox', { name: en['action_bucket.list'], exact: true }).check()
    await grants.getByRole('checkbox', { name: en['action_object.read'], exact: true }).check()
    await membership.getByRole('button', { name: en.save, exact: true }).click()
    await expect(membership).toHaveCount(0)
    const members = await (await request.get(`/api/v1/projects/${project.id}/members`)).json()
    expect(members.find((row: { user_id: string }) => row.user_id === user).external_grants).toEqual([{ bucket_id: external, actions: ['bucket.list', 'object.read'] }])
    await page.goto('/tokens')
    await page.getByRole('button', { name: en.createToken, exact: true }).click()
    const editor = page.getByRole('dialog', { name: en.createToken, exact: true })
    await editor.getByRole('textbox', { name: en.keyLabel, exact: true }).fill('External token ' + suffix)
    const scope = editor.getByRole('group', { name, exact: true })
    await expect(scope.getByRole('checkbox')).toHaveCount(4)
    await scope.getByRole('checkbox', { name: en['action_bucket.list'], exact: true }).check()
    const created = page.waitForResponse(response => response.url().endsWith('/api/v1/api-tokens') && response.request().method() === 'POST')
    await editor.getByRole('button', { name: en.createToken, exact: true }).click()
    const reply = await created
    expect(reply.status()).toBe(201)
    const result = await reply.json(); token = result.token.id
    expect(result.token.external_grants).toEqual([{ bucket_id: external, actions: ['bucket.list'] }])
    const save = page.getByRole('dialog', { name: en.saveSecret, exact: true })
    await save.getByRole('checkbox', { name: en.secretSaved, exact: true }).check()
    await save.getByRole('button', { name: en.done, exact: true }).click()
    await page.locator('.key-row').filter({ hasText: 'External token ' + suffix }).click()
    await expect(page.getByRole('dialog', { name: 'External token ' + suffix, exact: true }).getByText(name, { exact: true })).toBeVisible()
  } finally {
    if (token) expect((await request.delete('/api/v1/api-tokens/' + token, { headers })).status()).toBe(204)
    if (user) expect((await request.delete('/api/v1/users/' + user, { headers })).status()).toBe(204)
    if (external) {
      const current = await request.get('/api/v1/external-buckets/' + external)
      expect((await request.delete('/api/v1/external-buckets/' + external, { headers, data: { revision: (await current.json()).revision } })).status()).toBe(204)
    }
  }
})

test('run a connection test, retain its report, and stop unresolved recovery', async ({ page, request }, info) => {
  const headers = await login(page, request)
  const project = (await (await request.get('/api/v1/projects')).json())[0]
  const name = 'Probe ' + randomUUID().slice(0, 8), secret = 'fixture-' + randomUUID()
  const created = await request.post('/api/v1/external-buckets', { headers, data: { project_id: project.id, name, connection: { endpoint_url: 'http://127.0.0.1:9', allow_http: true, region: 'us-east-1', bucket: 'media' }, access_key: 'fixture-access', secret_key: secret } })
  expect(created.status()).toBe(201)
  const bucket = await created.json()
  let task = ''
  try {
    await page.goto('/external-buckets/' + bucket.id)
    const detail = page.getByRole('dialog', { name, exact: true })
    await expect(detail).toBeVisible()
    const started = page.waitForResponse(response => response.url().endsWith(`/external-buckets/${bucket.id}/test`) && response.request().method() === 'POST')
    await detail.getByRole('button', { name: en.externalTestStart, exact: true }).click()
    const reply = await started
    expect(reply.status()).toBe(202)
    task = (await reply.json()).task_id
    await expect(detail.getByRole('button', { name: en.externalTestStop, exact: true })).toBeVisible({ timeout: 30000 })
    await expect(detail.getByText(en.externalTestLocal, { exact: true })).toBeVisible()
    await expect(detail.getByRole('button', { name: en.externalTestStart, exact: true })).toBeDisabled()
    const download = page.waitForEvent('download')
    await detail.getByRole('link', { name: en.downloadReport, exact: true }).click()
    const file = await download
    const report = JSON.parse(readFileSync((await file.path())!, 'utf8'))
    expect(report.id).toBe(task)
    expect(report.state).toBe('failed')
    expect(report.cleanup_required).toBe(true)
    expect(JSON.stringify(report)).not.toContain(secret)
    await detail.getByRole('button', { name: en.externalTestStop, exact: true }).click()
    const confirmation = page.getByRole('dialog', { name: en.externalTestStop, exact: true })
    await expect(confirmation.getByRole('button', { name: en.externalTestStop, exact: true })).toBeDisabled()
    await confirmation.getByRole('textbox').fill(name)
    await confirmation.getByRole('button', { name: en.externalTestStop, exact: true }).click()
    await expect(confirmation).toHaveCount(0)
    await expect(detail.getByText(en.externalTestStopped, { exact: true })).toBeVisible()
    await expect(detail.getByRole('button', { name: en.resumeTask, exact: true })).toHaveCount(0)
    await expect(detail.getByRole('button', { name: en.externalTestStart, exact: true })).toBeEnabled()
    await page.screenshot({ path: info.outputPath('external-test-report.png'), fullPage: true })
    await page.emulateMedia({ reducedMotion: 'reduce' })
    for (const locale of ['en', 'zh-CN', 'ja']) {
      for (const theme of ['light', 'dark']) {
        expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBeTruthy()
        await page.reload()
        await expect(detail.getByRole('table')).toBeVisible()
        for (const width of [1280, 390, 320]) {
          await page.setViewportSize({ width, height: 844 })
          await detail.getByRole('table').scrollIntoViewIfNeeded()
          expect(await detail.evaluate(element => element.scrollWidth - element.clientWidth)).toBeLessThanOrEqual(1)
        }
        await page.screenshot({ path: info.outputPath(`external-test-${locale}-${theme}-320.png`), fullPage: true })
      }
    }
    expect((await request.delete(`/api/v1/external-buckets/${bucket.id}`, { headers, data: { revision: bucket.revision } })).status()).toBe(204)
    const retained = await request.get('/api/v1/external-tests/' + task)
    expect(retained.status()).toBe(200)
    expect((await retained.json()).external_id).toBeNull()
  } finally {
    const current = await request.get('/api/v1/external-buckets/' + bucket.id)
    if (current.ok()) {
      if (task) await request.post(`/api/v1/external-buckets/${bucket.id}/test/stop`, { headers, data: { test_id: task } })
      await request.delete('/api/v1/external-buckets/' + bucket.id, { headers, data: { revision: (await current.json()).revision } })
    }
  }
})
