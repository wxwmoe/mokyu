import { createHash, randomUUID } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('page selection, ACL, copy response loss, rename and metadata editing', async ({ page, request }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const login = await request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await login.json()).csrf_token }
  await request.put('/api/me', { headers, data: { display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false } })
  const bucket = (await (await request.get('/api/buckets')).json()).find((item: any) => item.name === 'media')
  const prefix = 'ui-actions-' + randomUUID().slice(0, 8) + '/'
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  const listing = async () => (await (await request.get(`/api/buckets/${bucket.id}/objects?` + new URLSearchParams({ prefix, recursive: 'true' }))).json()).objects
  try {
    for (const name of ['a.txt', 'b.txt', 'c.txt']) {
      const body = Buffer.from('A soft place for ' + name)
      const upload = await (await request.post(`/api/buckets/${bucket.id}/uploads`, { headers, data: { client_id: randomUUID(), key: prefix + name, file_name: name, size: String(body.length), content_type: 'text/plain' } })).json()
      const part = await request.put(`/api/uploads/${upload.id}/parts/1`, { headers: { ...headers, 'X-Content-SHA256': createHash('sha256').update(body).digest('base64') }, data: body })
      expect(part.ok()).toBe(true)
      expect((await request.post(`/api/uploads/${upload.id}/complete`, { headers, data: { parts: [await part.json()] } })).ok()).toBe(true)
    }
    await page.goto('/login')
    await page.getByRole('textbox', { name: en.username, exact: true }).fill('tester')
    await page.getByRole('textbox', { name: en.password, exact: true }).fill(password)
    await page.getByRole('button', { name: en.signIn, exact: true }).click()
    await expect(page).toHaveURL(/\/media$/)
    await page.goto(`/media/${bucket.id}?` + new URLSearchParams({ prefix }))
    await page.getByRole('button', { name: en.listView, exact: true }).click()
    await page.getByRole('checkbox', { name: en.selectObject.replace('{name}', prefix + 'a.txt'), exact: true }).click()
    await page.getByRole('checkbox', { name: en.selectObject.replace('{name}', prefix + 'c.txt'), exact: true }).click({ modifiers: ['Shift'] })
    await expect(page.locator('.selection-count')).toHaveText('3')
    await page.locator('.selection-bar').getByRole('button', { name: en.mediaActions }).click()
    await page.getByRole('menuitem', { name: en['mediaAction_public-read'], exact: true }).click()
    let dialog = page.getByRole('dialog', { name: en['mediaAction_public-read'], exact: true })
    await dialog.getByRole('button', { name: en['mediaAction_public-read'], exact: true }).click()
    await expect(dialog).toContainText(en.actionDoneHint)
    expect((await listing()).every((item: any) => item.public_read)).toBe(true)
    await dialog.getByRole('button', { name: en.close, exact: true }).last().click()
    await page.getByRole('checkbox', { name: en.selectPage, exact: true }).click()
    await page.locator('.selection-bar').getByRole('button', { name: en.mediaActions }).click()
    await page.getByRole('menuitem', { name: en.mediaAction_copy, exact: true }).click()
    dialog = page.getByRole('dialog', { name: en.mediaAction_copy, exact: true })
    await dialog.getByRole('textbox', { name: en.targetFolder, exact: true }).fill(prefix + 'copies/')
    let lose = true
    const ids: string[][] = []
    await page.route('**/api/media/actions', async route => {
      ids.push(route.request().postDataJSON().objects.map((item: any) => item.client_id))
      const response = await route.fetch()
      if (lose) { lose = false; await route.abort('failed') } else await route.fulfill({ response })
    })
    await dialog.getByRole('button', { name: en.mediaAction_copy, exact: true }).click()
    await expect(dialog.getByRole('button', { name: en.resumeAction })).toBeVisible()
    await dialog.getByRole('button', { name: en.resumeAction }).click()
    await expect(dialog).toContainText(en.actionDoneHint)
    expect(ids.length).toBe(2); expect(ids[0]).toEqual(ids[1])
    await page.unroute('**/api/media/actions')
    const copies = (await listing()).filter((item: any) => item.object_key.startsWith(prefix + 'copies/'))
    expect(copies.length).toBe(3); expect(copies.every((item: any) => !item.public_read)).toBe(true)
    await page.screenshot({ path: '/results/step13-action-results.png', fullPage: true })
    await dialog.getByRole('button', { name: en.close, exact: true }).last().click()
    await page.getByRole('button', { name: 'a.txt', exact: true }).click()
    await page.getByRole('dialog', { name: 'a.txt', exact: true }).getByRole('button', { name: en.mediaActions }).click()
    await page.getByRole('menuitem', { name: en.mediaAction_move, exact: true }).click()
    dialog = page.getByRole('dialog', { name: en.mediaAction_move, exact: true })
    await dialog.getByRole('textbox', { name: en.targetPath }).fill(prefix + 'renamed.txt')
    await dialog.getByRole('button', { name: en.mediaAction_move, exact: true }).click()
    await expect(dialog).toContainText(en.actionDoneHint)
    await dialog.getByRole('button', { name: en.close, exact: true }).last().click()
    await page.getByRole('button', { name: 'renamed.txt', exact: true }).click()
    await page.getByRole('dialog', { name: 'renamed.txt', exact: true }).getByRole('button', { name: en.mediaActions }).click()
    await page.getByRole('menuitem', { name: en.mediaAction_metadata, exact: true }).click()
    dialog = page.getByRole('dialog', { name: en.mediaAction_metadata, exact: true })
    await expect(dialog.getByRole('textbox', { name: en.metadata_content_type, exact: true })).toHaveValue('text/plain')
    await dialog.getByRole('textbox', { name: en.metadata_cache_control, exact: true }).fill('max-age=600')
    await dialog.getByRole('button', { name: en.close, exact: true }).click()
    const discard = page.getByRole('dialog', { name: en.discardTitle })
    await expect(discard).toBeVisible()
    await discard.getByRole('button', { name: en.keepEditing }).click()
    await dialog.getByRole('button', { name: en.mediaAction_metadata, exact: true }).click()
    await expect(dialog).toContainText(en.actionDoneHint)
    const renamed = (await listing()).find((item: any) => item.object_key === prefix + 'renamed.txt')
    const detail = await (await request.get(`/api/buckets/${bucket.id}/object?` + new URLSearchParams({ key: renamed.object_key, version: renamed.id }))).json()
    expect(detail.metadata.cache_control).toBe('max-age=600')
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    expect(errors).toEqual([])
  } finally {
    await page.unroute('**/api/media/actions')
    const items = await listing()
    if (items?.length) await request.post('/api/media/actions', { headers, data: { bucket: bucket.id, action: 'delete', objects: items.map((item: any) => ({ client_id: randomUUID(), key: item.object_key, version: item.id })) } })
  }
})
