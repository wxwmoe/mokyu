import { randomUUID } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('bucket creation, settings conflicts, CORS, website, paused transfer and deletion', async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const login = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  await request.post('/api/me/reauth', { headers, data: { password } })
  const name = 'ui-bucket-' + randomUUID().slice(0, 8), projectName = name + '-project'
  const project = await (await request.post('/api/projects', { headers, data: { name: projectName } })).json()
  let id = ''
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  try {
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/media$/)
    await page.getByRole('link', { name: en.buckets, exact: true }).click()
    await page.getByRole('button', { name: en.createBucket, exact: true }).click()
    let dialog = page.getByRole('dialog', { name: en.createBucket, exact: true })
    await dialog.getByRole('textbox', { name: en.bucketName, exact: true }).fill(name)
    await dialog.getByRole('button', { name: en.createBucket, exact: true }).click()
    await expect(page.getByRole('heading', { name, exact: true })).toBeVisible()
    id = page.url().split('/').pop()!
    const path = `/api/buckets/${id}/settings`
    await page.getByRole('textbox', { name: en.publicBaseUrl, exact: true }).fill('https://media.example.test/')
    await page.getByRole('textbox', { name: en.bucketDomains, exact: true }).fill(name + '.test\n' + name + '-two.test')
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect.poll(async () => (await (await request.get(path)).json()).domains.length).toBe(2)
    await page.getByRole('tab', { name: 'CORS', exact: true }).click()
    await page.getByRole('button', { name: en.corsPreset, exact: true }).click()
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect.poll(async () => (await (await request.get(path)).json()).bucket.cors.length).toBe(1)
    await page.getByRole('tab', { name: en.bucketWebsite, exact: true }).click()
    await page.getByRole('checkbox', { name: en.enableWebsite, exact: true }).click()
    await page.getByRole('textbox', { name: en.indexDocument, exact: true }).fill('welcome.html')
    await page.getByRole('link', { name: en.browseFiles, exact: true }).click()
    dialog = page.getByRole('dialog', { name: en.unsavedChanges, exact: true })
    await dialog.getByRole('button', { name: en.keepEditing, exact: true }).click()
    const external = (await (await request.get(path)).json()).bucket
    await request.put(path, { headers, data: { revision: external.revision, cors: [], website_enabled: false, index_document: 'other.html', error_document: '', public_base_url: external.public_base_url, uploads_paused: false } })
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect(page.getByText(en.error_PreconditionFailed, { exact: true })).toBeVisible()
    await page.locator('.page-heading').getByRole('button', { name: en.refresh, exact: true }).click()
    await page.getByRole('dialog', { name: en.unsavedChanges }).getByRole('button', { name: en.discard, exact: true }).click()
    await expect(page.getByRole('textbox', { name: en.indexDocument, exact: true })).toHaveValue('other.html')
    await page.getByRole('checkbox', { name: en.enableWebsite, exact: true }).click()
    await page.getByRole('button', { name: en.save, exact: true }).click()
    await expect.poll(async () => (await (await request.get(path)).json()).bucket.website_enabled).toBe(true)
    await page.getByRole('button', { name: en.transferBucket, exact: true }).click()
    dialog = page.getByRole('dialog', { name: en.transferBucket, exact: true })
    await dialog.getByRole('combobox', { name: en.destinationProject, exact: true }).click()
    await page.getByRole('option', { name: projectName, exact: true }).click()
    await dialog.getByRole('button', { name: en.reviewTransfer, exact: true }).click()
    await dialog.getByRole('button', { name: en.pauseUploads, exact: true }).click()
    await expect(dialog.getByRole('textbox')).toBeVisible()
    await dialog.getByRole('textbox').fill(name)
    await expect(dialog.getByRole('button', { name: en.confirmTransfer, exact: true })).toBeEnabled()
    await page.screenshot({ path: '/results/step14-transfer.png' })
    await dialog.getByRole('button', { name: en.confirmTransfer, exact: true }).click()
    await expect.poll(async () => (await (await request.get(path)).json()).bucket.project_id).toBe(project.id)
    await expect(dialog).not.toBeVisible()
    await page.getByRole('button', { name: en.deleteEmptyBucket, exact: true }).click()
    dialog = page.getByRole('dialog', { name: en.deleteEmptyBucket, exact: true })
    await expect(dialog.getByRole('button', { name: en.deleteEmptyBucket, exact: true })).toBeDisabled()
    await dialog.getByRole('textbox').fill(name)
    await dialog.getByRole('button', { name: en.deleteEmptyBucket, exact: true }).click()
    await expect(page).toHaveURL(/\/buckets$/)
    expect((await request.get(path)).status()).toBe(404)
    expect(errors).toEqual([])
  } finally {
    if (id) await request.delete('/api/buckets/' + id, { headers, data: { confirm_name: name, confirmation: '' } })
    await request.delete('/api/projects/' + project.id, { headers })
  }
})
