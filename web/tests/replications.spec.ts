import { readFileSync, writeFileSync, unlinkSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test.skip(!process.env.MOKYU_TEST_REPLICATION_FIXTURE || !process.env.MOKYU_TEST_FAULT_DIR, 'Requires isolated seeded buckets and fault boundaries')

test('create, inspect and control durable replication tasks using the forms', async ({ page, request }, info) => {
  test.setTimeout(150000)
  const fixture = JSON.parse(readFileSync(process.env.MOKYU_TEST_REPLICATION_FIXTURE!, 'utf8'))
  const password = readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim()
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  expect(login.status()).toBe(200)
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  async function preferences(locale: string, theme: string) {
    expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBeTruthy()
  }
  await preferences('en', 'light')
  await page.goto('/login')
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
  await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
  await page.getByRole('button', { name: en.signIn, exact: true }).click()
  await expect(page).toHaveURL(/\/overview$/)
  await page.goto('/replications')

  async function choice(label: string, value: string, searchable = false) {
    const control = page.getByRole('combobox', { name: label, exact: true })
    if (searchable) await control.fill(value)
    else await control.click()
    await page.getByRole('option', { name: value, exact: true }).click()
  }
  async function form() {
    await page.getByRole('button', { name: en.replicationCreate, exact: true }).click()
    await choice(en.replicationSource, fixture.source.name, true)
    await choice(en.replicationTarget, fixture.target.name, true)
    return page.getByRole('dialog', { name: en.replicationCreate, exact: true })
  }
  async function start() {
    const reply = page.waitForResponse(response => response.url().endsWith('/api/v1/replications') && response.request().method() === 'POST')
    await page.getByRole('button', { name: en.replicationStart, exact: true }).click()
    const response = await reply
    expect(response.status()).toBe(202)
    return (await response.json()).task_id as string
  }
  const details = () => page.getByRole('dialog', { name: en.replications, exact: true })

  await form()
  await choice(en.replicationScope, en.replicationScope_prefix)
  await page.getByLabel(en.replicationPrefix, { exact: true }).fill('photos/')
  await page.getByLabel(en.replicationDestination, { exact: true }).fill('backup/')
  await choice(en.replicationPermissions, en.replicationPermission_public_read)
  await expect(page.locator('.mapping-preview')).toContainText('photos/*')
  await expect(page.locator('.mapping-preview')).toContainText('backup/*')
  const copied = await start()
  await expect(details().locator('.replication-summary')).toContainText(en.completed, { timeout: 30000 })
  await expect(details().locator('tbody tr')).toHaveCount(2)
  await expect(details().locator('tbody')).not.toContainText(en.replicationItem_planned)
  expect((await request.get(fixture.s3 + '/' + fixture.target.name + '/backup/one.jpg')).status()).toBe(200)
  const copiedOptions = (await (await request.get('/api/v1/replications/' + copied)).json()).options
  expect(copiedOptions.scope).toEqual({ selection: 'prefix', prefix: 'photos/' })
  expect(copiedOptions.permissions).toBe('public_read')
  await details().locator('.dialog-footer').getByRole('button', { name: en.close, exact: true }).click()

  await form()
  await choice(en.replicationOperation, en.replicationMove)
  await choice(en.replicationScope, en.replicationScope_objects)
  await page.getByLabel(en.replicationKeys).fill('photos/two.jpg')
  await page.getByLabel(en.replicationRename, { exact: true }).fill('renamed.jpg')
  await choice(en.replicationPermissions, en.replicationPermission_ignore)
  await expect(page.locator('.mapping-preview')).toContainText('renamed.jpg')
  const moved = await start()
  await expect(details().locator('.replication-summary')).toContainText(en.completed, { timeout: 30000 })
  const movedOptions = (await (await request.get('/api/v1/replications/' + moved)).json()).options
  expect(movedOptions).toMatchObject({ operation: 'move', target_prefix: '', target_key: 'renamed.jpg', permissions: 'ignore' })
  await expect(details().locator('tbody')).toContainText(en.replicationItem_completed)
  await details().locator('.dialog-footer').getByRole('button', { name: en.close, exact: true }).click()

  const marker = join(process.env.MOKYU_TEST_FAULT_DIR!, 'replication-batch')
  const hit = marker + '.hit'
  const remove = (path: string) => { if (existsSync(path)) unlinkSync(path) }
  try {
    remove(hit); writeFileSync(marker, '')
    await form()
    await choice(en.replicationOperation, en.copy)
    await choice(en.replicationScope, en.replicationScope_all)
    await start()
    await expect.poll(() => existsSync(hit)).toBeTruthy()
    await details().getByRole('button', { name: en.pauseTask, exact: true }).click()
    await expect(details().locator('.replication-summary')).toContainText(en.paused)
    remove(marker)
    await details().getByRole('button', { name: en.resumeTask, exact: true }).click()
    await expect(details().locator('.replication-summary')).toContainText(en.completed, { timeout: 30000 })
    await details().locator('.dialog-footer').getByRole('button', { name: en.close, exact: true }).click()
    remove(hit); writeFileSync(marker, '')
    await form()
    await start()
    await expect.poll(() => existsSync(hit)).toBeTruthy()
    await details().getByRole('button', { name: en.cancel, exact: true }).click()
    const confirmation = page.getByRole('dialog', { name: en.replicationCancel, exact: true })
    await expect(confirmation).toContainText(en.replicationCancelHint)
    await confirmation.getByRole('button', { name: en.replicationCancel, exact: true }).click()
    remove(marker)
    await expect(details().locator('.replication-summary')).toContainText(en.replicationPhase_cancelled, { timeout: 30000 })
    await expect(details().locator('tbody')).toContainText(en.replicationItem_cancelled)
    await details().locator('.dialog-footer').getByRole('button', { name: en.close, exact: true }).click()
  } finally { remove(marker); remove(hit) }

  await page.getByRole('button', { name: en.replicationCreate, exact: true }).click()
  await choice(en.replicationOperation, en.copy)
  for (const [label, resource] of [[en.replicationSource, fixture.external.source], [en.replicationTarget, fixture.external.target]]) {
    await page.getByRole('combobox', { name: label, exact: true }).fill(resource.name)
    await page.getByRole('option').filter({ hasText: resource.name }).click()
  }
  await page.getByRole('combobox', { name: en.replicationOperation, exact: true }).click()
  await expect(page.getByRole('option', { name: en.replicationMove, exact: true })).toHaveAttribute('aria-disabled', 'true')
  await page.keyboard.press('Escape')
  await choice(en.replicationScope, en.replicationScope_objects)
  await page.getByLabel(en.replicationKeys).fill('one.jpg')
  await page.getByLabel(en.replicationRename, { exact: true }).fill('preview.jpg')
  await choice(en.replicationPermissions, en.replicationPermission_inherit)
  const externalTask = await start()
  await expect(details().locator('.replication-summary')).toContainText(en.completed, { timeout: 30000 })
  const externalOptions = (await (await request.get('/api/v1/replications/' + externalTask)).json()).options
  expect(externalOptions).toMatchObject({ source_bucket: fixture.external.source.id, target_bucket: fixture.external.target.id,
    source_external: true, target_external: true, operation: 'copy', target_key: 'preview.jpg', permissions: 'inherit' })
  expect((await request.get(fixture.s3 + '/' + fixture.target.name + '/external/preview.jpg')).status()).toBe(200)
  await details().locator('.dialog-footer').getByRole('button', { name: en.close, exact: true }).click()

  await page.getByRole('button', { name: en.replicationCreate, exact: true }).click()
  await choice(en.replicationSource, fixture.source.name, true)
  await page.getByRole('combobox', { name: en.replicationTarget, exact: true }).fill(fixture.external.target.name)
  await page.getByRole('option').filter({ hasText: fixture.external.target.name }).click()
  await page.getByLabel(en.replicationKeys).fill('photos/one.jpg')
  await page.getByLabel(en.replicationRename, { exact: true }).fill('exported.jpg')
  const exportedTask = await start()
  await expect(details().locator('.replication-summary')).toContainText(en.completed, { timeout: 30000 })
  expect((await (await request.get('/api/v1/replications/' + exportedTask)).json()).options).toMatchObject({
    source_bucket: fixture.source.id, target_bucket: fixture.external.target.id, source_external: false, target_external: true })
  expect((await request.get(fixture.s3 + '/' + fixture.target.name + '/external/exported.jpg')).status()).toBe(200)
  await details().locator('.dialog-footer').getByRole('button', { name: en.close, exact: true }).click()

  await page.getByRole('button', { name: en.replicationCreate, exact: true }).click()
  await page.getByRole('combobox', { name: en.replicationSource, exact: true }).fill(fixture.external.source.name)
  await page.getByRole('option').filter({ hasText: fixture.external.source.name }).click()
  await choice(en.replicationTarget, fixture.target.name, true)
  await page.getByLabel(en.replicationKeys).fill('one.jpg')
  await page.getByLabel(en.replicationRename, { exact: true }).fill('imported.jpg')
  const importedTask = await start()
  await expect(details().locator('.replication-summary')).toContainText(en.completed, { timeout: 30000 })
  expect((await (await request.get('/api/v1/replications/' + importedTask)).json()).options).toMatchObject({
    source_bucket: fixture.external.source.id, target_bucket: fixture.target.id, source_external: true, target_external: false })
  expect((await request.get(fixture.s3 + '/' + fixture.target.name + '/imported.jpg')).status()).toBe(200)
  await details().locator('.dialog-footer').getByRole('button', { name: en.close, exact: true }).click()

  for (const [locale, theme, width, labels] of [['en', 'light', 1400, en], ['zh-CN', 'dark', 1400, zh], ['ja', 'light', 390, ja]] as const) {
    await preferences(locale, theme)
    await page.setViewportSize({ width, height: 940 })
    await page.reload()
    await expect(page.getByRole('heading', { name: labels.replications, exact: true })).toBeVisible()
    await expect(page.locator('.table-scroll tbody tr')).toHaveCount(7)
    await expect(page.getByRole('columnheader', { name: labels.replicationStatus, exact: true })).toBeVisible()
    const layout = await page.evaluate(() => ({ viewport: innerWidth, width: document.documentElement.scrollWidth, boxes: Array.from(document.querySelectorAll('.workspace, #main, .page, .page-heading, .table-scroll, .app-footer')).map(element => ({ name: element.className, width: element.getBoundingClientRect().width, right: element.getBoundingClientRect().right, minWidth: getComputedStyle(element).minWidth, overflow: getComputedStyle(element).overflowX })) }))
    expect(layout.width <= layout.viewport + 1, JSON.stringify(layout)).toBeTruthy()
    await page.screenshot({ path: info.outputPath(locale + '-list.png'), fullPage: true })
    await page.getByRole('button', { name: labels.replicationCreate, exact: true }).click()
    await expect(page.getByRole('dialog', { name: labels.replicationCreate, exact: true })).toBeVisible()
    await page.screenshot({ path: info.outputPath(locale + '-form.png'), fullPage: true })
    await page.keyboard.press('Escape')
    await expect(page.getByRole('dialog')).toHaveCount(0)
  }
  expect(errors).toEqual([])
})
