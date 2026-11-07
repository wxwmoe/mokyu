import { readFileSync } from 'node:fs'
import { expect, test } from '@playwright/test'
import en from '../src/locales/i18n.en.js'

test('unlock an existing journal while adding a storage', async ({ page, request }) => {
  test.skip(!process.env.MOKYU_TEST_RECOVERY_SOURCE, 'requires a separately keyed source journal')
  const source = JSON.parse(readFileSync(process.env.MOKYU_TEST_RECOVERY_SOURCE!, 'utf8'))
  const password = process.env.MOKYU_TEST_PASSWORD!
  const origin = process.env.MOKYU_TEST_WEB!
  const login = await request.post('/api/v1/auth/login', {headers:{Origin:origin},data:{username:'tester',password}})
  const headers = {Origin:origin,'X-CSRF-Token':(await login.json()).csrf_token}
  await request.put('/api/v1/account',{headers,data:{display_name:'',locale:'en',theme:'light',avatar_email:'',avatar_enabled:false}})
  await page.goto('/login')
  await page.getByRole('textbox',{name:en.username,exact:true}).fill('tester')
  await page.getByRole('textbox',{name:en.password,exact:true}).fill(password)
  await page.getByRole('button',{name:en.signIn,exact:true}).click()
  await expect(page).toHaveURL(/\/overview$/)
  await page.goto('/storages')
  await page.getByRole('button',{name:en.addStore,exact:true}).click()
  const dialog=page.getByRole('dialog')
  for(const [label,value] of [[en.storeName,source.name],[en.storeEndpoint,source.endpoint],[en.storeBucket,source.bucket],[en.storeRegion,source.region],[en.storeAccessKey,source.access_key],[en.storeSecretKey,source.secret_key]]){
    await dialog.getByLabel(label,{exact:true}).fill(value)
  }
  await dialog.getByRole('button',{name:en.save,exact:true}).click()
  await expect(dialog.getByLabel(en.recoverySourceKey)).toBeVisible()
  await dialog.getByLabel(en.recoverySourceKey).fill(readFileSync(process.env.MOKYU_TEST_RECOVERY_KEY_FILE!,'utf8').trim())
  await dialog.getByRole('button',{name:en.recoveryUnlock}).click()
  await expect(dialog.getByLabel(en.recoveryTarget,{exact:true})).toBeVisible()
  await expect(dialog.getByRole('checkbox')).toHaveCount(2)
  await expect(dialog.getByRole('button',{name:en.recoverBuckets})).toBeDisabled()
  await page.setViewportSize({width:390,height:780})
  await dialog.getByRole('checkbox').first().check()
  await expect(dialog.getByLabel(en.recoveryBucketName)).toBeVisible()
  expect(await dialog.evaluate(el => el.scrollWidth <= el.clientWidth + 1)).toBe(true)
})
