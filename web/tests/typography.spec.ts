import { test, expect } from '@playwright/test'
import { readFileSync } from 'node:fs'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('fonts follow visible glyphs and locale priority while localized login fits the page', async ({ page }) => {
  const fonts: string[] = [], errors: string[] = []
  page.on('request', request => { if (request.resourceType() === 'font') fonts.push(request.url()) })
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/login')
  await expect(page.getByRole('button', { name: en.signIn, exact: true })).toBeVisible()
  await page.getByRole('button', { name: en.language, exact: true }).hover()
  await expect(page.locator('.tooltip')).toBeVisible()
  await expect(page.getByRole('button', { name: en.language, exact: true })).toHaveAccessibleDescription(`${en.language} · ${en.language_en}`)
  await page.getByRole('textbox', { name: en.username, exact: true }).hover()
  expect(fonts.some(url => /resource-han-rounded|zen-maru-gothic/.test(url))).toBe(false)
  await page.getByRole('textbox', { name: en.username, exact: true }).fill('2026-\u56fe\u7247-\u7e41\u9ad4\u6a94\u6848-\u5199\u771f-\u30e2\u30c1.png')
  await expect.poll(() => page.evaluate(() => [...document.fonts].some(font => font.family.includes('Resource Han Rounded CN') && font.status === 'loaded'))).toBe(true)
  await page.evaluate(() => document.fonts.ready)
  expect(fonts.some(url => url.includes('resource-han-rounded'))).toBe(true)
  expect(fonts.some(url => url.includes('zen-maru-gothic'))).toBe(false)
  expect(await page.evaluate(() => [...document.fonts].filter(font => font.family.includes('Resource Han Rounded CN')).every(font => font.status === 'loaded'))).toBe(false)
  await page.getByRole('textbox', { name: en.username, exact: true }).clear()

  for (const [code, dictionary, family, filename] of [
    ['zh-CN', zh, 'Resource Han Rounded CN', 'resource-han-rounded'],
    ['ja', ja, 'Zen Maru Gothic', 'zen-maru-gothic'],
  ] as const) {
    await page.getByRole('button', { name: code === 'zh-CN' ? en.language : zh.language, exact: true }).click()
    await page.getByRole('menuitem', { name: dictionary[`language_${code}`], exact: true }).click()
    await expect(page.locator('html')).toHaveAttribute('lang', code)
    const language = page.getByRole('button', { name: dictionary.language, exact: true })
    await expect(language.locator('.locale-icon')).toBeVisible()
    await page.getByRole('textbox', { name: dictionary.username, exact: true }).hover()
    await language.hover()
    await expect(page.locator('.tooltip')).toBeVisible()
    await expect(language).toHaveAccessibleDescription(`${dictionary.language} · ${dictionary[`language_${code}`]}`)
    await language.click()
    await expect(page.getByRole('menuitem', { name: dictionary[`language_${code}`], exact: true }).locator('svg')).toBeVisible()
    await expect(page.locator('.tooltip')).toHaveCount(0)
    await page.keyboard.press('Escape')
    await expect(language).toBeFocused()
    const families = await page.locator('body').evaluate(element => getComputedStyle(element).fontFamily.split(',').map(value => value.trim().replaceAll('"', '')))
    expect(families.slice(0, 3)).toEqual(['Nunito Variable', family, code === 'ja' ? 'Resource Han Rounded CN' : 'Zen Maru Gothic'])
    await expect(page.getByRole('button', { name: dictionary.signIn, exact: true })).toBeVisible()
    await expect.poll(() => page.evaluate(family => [...document.fonts].some(font => font.family.includes(family) && font.status === 'loaded'), family)).toBe(true)
    await page.evaluate(() => document.fonts.ready)
    expect(fonts.some(url => url.includes(filename))).toBe(true)
    expect(fonts.every(url => url.startsWith(process.env.MOKYU_TEST_WEB!))).toBe(true)
    if (code === 'zh-CN') expect(fonts.some(url => url.includes('zen-maru-gothic'))).toBe(false)
    for (const width of [1440, 820, 390, 320]) {
      await page.setViewportSize({ width, height: 940 })
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${code}/${width}`).toBe(true)
      await expect(page.getByRole('button', { name: dictionary.signIn, exact: true })).toBeVisible()
    }
    await page.reload()
    await expect(page.locator('html')).toHaveAttribute('lang', code)
  }
  expect(errors).toEqual([])
})

test('unavailable font files keep the localized login usable', async ({ page }) => {
  await page.route(/\.woff2?(?:\?|$)/, route => route.abort())
  await page.goto('/login')
  await page.getByRole('button', { name: en.language, exact: true }).click()
  await page.getByRole('menuitem', { name: zh['language_zh-CN'], exact: true }).click()
  await expect(page.getByRole('button', { name: zh.signIn, exact: true })).toBeEnabled()
  await page.getByRole('textbox', { name: zh.username, exact: true }).fill('font-fallback')
  await expect(page.getByRole('textbox', { name: zh.username, exact: true })).toHaveValue('font-fallback')
})

test('closed selects translate immediately and searchable choices retain their selection', async ({ page }) => {
  const password = (process.env.MOKYU_TEST_PASSWORD || readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8')).trim()
  const auth = await page.request.post('/api/login', { headers: { Origin: process.env.MOKYU_TEST_WEB! }, data: { username: 'tester', password } })
  expect(auth.ok()).toBe(true)
  const headers = { Origin: process.env.MOKYU_TEST_WEB!, 'X-CSRF-Token': (await auth.json()).csrf_token }
  const profile = await (await page.request.get('/api/me')).json()
  const original = Object.fromEntries(['display_name', 'locale', 'theme', 'avatar_email', 'avatar_enabled'].map(key => [key, profile[key]]))
  const buckets = await (await page.request.get('/api/buckets')).json()
  const bucket = buckets.find((item: { actions: string[] }) => item.actions.includes('storage.inspect'))
  expect(bucket).toBeTruthy()
  await page.route('**/api/projects', route => route.fulfill({ json: [] }))
  try {
    for (const searchable of [false, true]) {
      await page.request.put('/api/me', { headers, data: { ...original, locale: 'en' } })
      const options = searchable ? Array.from({ length: 11 }, (_, index) => ({ ...bucket, id: index ? `test-${index}` : bucket.id, name: index ? `other-${index}` : bucket.name })) : [bucket]
      await page.route('**/api/buckets', route => route.fulfill({ json: options }))
      await page.goto('/overview')
      const scope = page.locator(`.scope-picker ${searchable ? 'input' : 'button'}[role="combobox"]`)
      async function selected(label: string) {
        if (searchable) await expect(scope).toHaveValue(label)
        else await expect(scope).toHaveText(label)
      }
      await selected(en.wholeDeployment)
      let previous = en
      for (const [code, dictionary] of [['zh-CN', zh], ['ja', ja], ['en', en]] as const) {
        await page.getByRole('button', { name: previous.language, exact: true }).click()
        await page.getByRole('menuitem', { name: dictionary[`language_${code}`], exact: true }).click()
        await expect(page.locator('html')).toHaveAttribute('lang', code)
        await selected(dictionary.wholeDeployment)
        await expect(scope).toHaveAttribute('aria-expanded', 'false')
        await expect(page).toHaveURL(/\/overview$/)
        previous = dictionary
      }
      if (searchable) {
        await scope.fill('no-matching-choice')
        await expect(page.getByText(en.noChoiceMatches)).toBeVisible()
        await expect(scope).toHaveValue('no-matching-choice')
        await page.keyboard.press('Escape')
        await selected(en.wholeDeployment)
        await scope.fill(bucket.name)
      } else await scope.click()
      await page.getByRole('option').filter({ hasText: bucket.name }).first().click()
      await selected(bucket.name)
      const url = page.url()
      await page.getByRole('button', { name: en.language, exact: true }).click()
      await page.getByRole('menuitem', { name: ja.language_ja, exact: true }).click()
      await expect(page.locator('html')).toHaveAttribute('lang', 'ja')
      await selected(bucket.name)
      await expect(page).toHaveURL(url)
      await page.unroute('**/api/buckets')
    }
  } finally {
    await page.request.put('/api/me', { headers, data: original })
    await page.request.post('/api/logout', { headers })
  }
})
