import { expect, test } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test('maintain snapshots in the storage detail view', async ({page,request})=>{
  test.skip(!process.env.MOKYU_TEST_SNAPSHOT_STORAGE,'requires a published journal')
  test.setTimeout(90000)
  const origin=process.env.MOKYU_TEST_WEB!, password=process.env.MOKYU_TEST_PASSWORD!
  const login=await request.post('/api/v1/auth/login',{headers:{Origin:origin},data:{username:'tester',password}})
  const headers={Origin:origin,'X-CSRF-Token':(await login.json()).csrf_token}
  const storage=encodeURIComponent(process.env.MOKYU_TEST_SNAPSHOT_STORAGE!)
  const path='/api/v1/storages/'+storage+'/snapshots'
  async function preferences(locale:string,theme:string) {
    await request.put('/api/v1/account',{headers,data:{display_name:'',locale,theme,avatar_email:'',avatar_enabled:false}})
  }
  await preferences('en','light')
  await page.goto('/login')
  await page.getByRole('textbox',{name:en.username,exact:true}).fill('tester')
  await page.getByRole('textbox',{name:en.password,exact:true}).fill(password)
  await page.getByRole('button',{name:en.signIn,exact:true}).click()
  await expect(page).toHaveURL(/\/overview$/)
  for(const [lang,theme,labels,action] of [['en','light',en,'snapshotAppend'],['zh-CN','dark',zh,'snapshotCompact'],['ja','light',ja,'snapshotNew']] as const){
    await preferences(lang,theme)
    await page.setViewportSize({width:lang==='en'?1100:390,height:820})
    await page.goto('/storages/'+storage)
    const revision=Number((await (await request.get(path)).json()).revision)
    await expect(page.getByRole('heading',{name:labels.metadataSnapshots})).toBeVisible()
    const button=page.getByRole('button',{name:labels[action],exact:true})
    await button.click()
    await expect.poll(async()=>Number((await (await request.get(path)).json()).revision),{timeout:25000}).toBeGreaterThan(revision)
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth+1)).toBe(true)
  }
  await preferences('en','light')
})
