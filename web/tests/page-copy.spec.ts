import { readFileSync } from 'node:fs'
import { test, expect, type Page } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

async function authenticate(page: Page, locale: string, theme: string) {
  const request = page.context().request, origin = process.env.MOKYU_TEST_WEB!
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username: 'tester', password } })
  expect(login.ok()).toBe(true)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  expect((await request.put('/api/v1/account', { headers, data: { display_name: '', locale, theme, avatar_email: '', avatar_enabled: false } })).ok()).toBe(true)
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  return { request, errors }
}

for (const [locale, text] of [['en', en], ['zh-CN', zh], ['ja', ja]] as const) {
  for (const theme of ['light', 'dark']) {
    test(`short actions preserve titles and other entry points (${locale}, ${theme})`, async ({ page }, info) => {
      const { errors } = await authenticate(page, locale, theme)
      expect(text.createCredential).not.toBe(text.createKey)
      expect(text.createStore).not.toBe(text.addStore)
      await page.goto('/credentials')
      await expect(page.locator('.page-heading h1')).toHaveText(text.keyManagement)
      await page.getByRole('button', { name: text.createKey, exact: true }).click()
      const credential = page.getByRole('dialog', { name: text.createCredential, exact: true })
      await expect(credential).toBeVisible()
      await expect(credential.getByRole('button', { name: text.createKey, exact: true })).toBeVisible()
      await expect(credential.getByRole('button', { name: text.createCredential, exact: true })).toHaveCount(0)
      await credential.getByRole('button', { name: text.cancel, exact: true }).click()
      await page.goto('/tokens')
      await expect(page.getByRole('button', { name: text.createToken, exact: true })).toBeVisible()

      await page.goto('/storages')
      await expect(page.locator('.page-heading h1')).toHaveText(text.storages)
      await page.getByRole('button', { name: text.addStore, exact: true }).click()
      const storage = page.getByRole('dialog', { name: text.createStore, exact: true })
      await expect(storage).toBeVisible()
      await storage.getByRole('button', { name: text.cancel, exact: true }).click()

      for (const [path, action, description] of [
        ['/buckets', text.createBucket, text.createBucketHint],
        ['/users', text.createUser, text.newUserHint],
        ['/credentials?tab=managed', text.createKey, text.createKeyHint],
      ]) {
        await page.goto(path!)
        await page.getByRole('button', { name: action!, exact: true }).click()
        const dialog = page.getByRole('dialog', { name: action!, exact: true })
        await expect(dialog.locator('.dialog-header p')).toHaveText(description!)
        expect((await dialog.locator('.dialog-header p').innerText()).trim()).not.toMatch(/[.\u3002]$/)
        for (const width of [1440, 320]) {
          await page.setViewportSize({ width, height: 1040 })
          expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
          await dialog.screenshot({ path: info.outputPath(`${path!.split('?')[0]!.slice(1)}-${width}.png`) })
        }
        await page.keyboard.press('Escape')
        await expect(dialog).toHaveCount(0)
      }

      await page.goto('/runtime-config')
      await expect(page.getByRole('heading', { name: text.controlSettings, exact: true })).toBeVisible()
      await expect(page.locator('.runtime-form .form-footer button')).toHaveText(text.saveControlSettings)
      for (const width of [1440, 1024, 768, 640, 390, 320]) {
        await page.setViewportSize({ width, height: 1040 })
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      }
      await page.locator('.runtime-form .form-footer').screenshot({ path: info.outputPath('save-settings-320.png') })

      // The empty-bucket entry must keep its longer label, independently of the storage button.
      await page.route('**/api/v1/buckets', route => route.fulfill({ json: [] }))
      await page.route('**/api/v1/bucket-projects', route => route.fulfill({ json: { projects: [], storages: [] } }))
      await page.goto('/buckets')
      await expect(page.locator('.heading-actions').getByRole('link', { name: text.createStore, exact: true })).toBeVisible()
      await expect(page.getByRole('link', { name: text.addStore, exact: true })).toHaveCount(0)
      expect(errors).toEqual([])
    })

    test(`cluster coverage stays at the page end and single mode stays unchanged (${locale}, ${theme})`, async ({ page }, info) => {
      test.setTimeout(90_000)
      const { request, errors } = await authenticate(page, locale, theme)
      const status = await (await request.get('/api/v1/system/status')).json()
      const runtime = await (await request.get('/api/v1/usage/runtime')).json()
      expect(status.deployment_mode).toBe('single')
      for (const path of ['/overview', '/storage', '/service']) {
        await page.goto(path)
        await expect(page.locator('.page-heading h1')).toBeVisible()
        await expect(page.locator('.runtime-coverage, .cluster-board, .cluster-scopes')).toHaveCount(0)
      }

      const at = '2026-10-19T14:58:28Z'
      const current = { at, window_start: at, scope: 'cluster', interval_seconds: 60, expected_nodes: 5, reported_nodes: 5, rate_nodes: 5, gauges: null, rates: null }
      let sample = { current: current as typeof current | null, history: [], stale: false }
      // Only cluster read responses are synthetic; authentication and the remaining pages use the isolated service.
      await page.route('**/api/v1/system/status', route => route.fulfill({ json: { ...status, deployment_mode: 'cluster' } }))
      await page.route('**/api/v1/cluster/status', route => route.fulfill({ json: { mode: 'cluster', enabled_nodes: 5, local_storages: 0, observed_at: at, nodes: [], settings: [] } }))
      await page.route('**/api/v1/cluster/nodes?*', route => route.fulfill({ json: { items: [], next_cursor: null } }))
      await page.route('**/api/v1/usage/runtime', route => route.fulfill({ json: { ...runtime, scope: 'cluster', cluster: sample } }))
      for (const state of ['complete', 'partial', 'baseline', 'stale', 'collecting']) {
        sample = { current: state === 'collecting' ? null : { ...current, reported_nodes: state === 'partial' ? 3 : 5, rate_nodes: state === 'partial' ? 2 : state === 'baseline' ? 4 : 5 }, history: [], stale: state === 'stale' }
        for (const path of ['/overview', '/storage', '/service']) {
          await page.goto(path)
          const coverage = page.locator('.runtime-coverage')
          await expect(coverage).toHaveCount(1)
          await expect(coverage).toBeVisible()
          expect(await coverage.evaluate(element => element.parentElement?.id === 'main' && element.nextElementSibling === null)).toBe(true)
          await expect(coverage).toHaveCSS('position', 'static')
          if (sample.current) {
            await expect(coverage.locator('strong')).toHaveText(text.clusterCoverage.replace('{reported}', String(sample.current.reported_nodes)).replace('{expected}', '5'))
            await expect(coverage).toContainText(text.clusterRateCoverage.replace('{count}', String(sample.current.rate_nodes)))
            await expect(coverage).toContainText('2026')
          } else await expect(coverage).toHaveText(text.clusterHistoryCollecting)
          if (['partial', 'baseline', 'stale'].includes(state)) {
            await expect(coverage).toHaveClass(/is-partial/)
            expect(await coverage.locator('strong').evaluate(element => getComputedStyle(element).color !== getComputedStyle(element.parentElement!).color)).toBe(true)
          } else await expect(coverage).not.toHaveClass(/is-partial/)
          if (state === 'stale') await expect(coverage.locator('.badge')).toHaveText(text.clusterReportStale)
          else await expect(coverage.locator('.badge')).toHaveCount(0)
          for (const width of state === 'complete' ? [1440, 1024, 768, 640, 390, 320] : [320]) {
            await page.setViewportSize({ width, height: 1040 })
            await coverage.scrollIntoViewIfNeeded()
            expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
            if (width === 1440) {
              await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' }))
              await page.screenshot({ path: info.outputPath(`${path.slice(1)}-${state}-1440.png`), fullPage: true })
            }
          }
          if (state === 'complete' || state === 'stale') {
            await page.evaluate(() => window.scrollTo({ top: 0, behavior: 'instant' }))
            await page.screenshot({ path: info.outputPath(`${path.slice(1)}-${state}-320.png`), fullPage: true })
          }
        }
      }
      expect(errors).toEqual([])
    })
  }
}
