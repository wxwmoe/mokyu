import { readFileSync } from 'node:fs'
import { test, expect } from '@playwright/test'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'

test.skip(!process.env.MOKYU_TEST_REPLICATION_FIXTURE, 'Requires isolated buckets on two internal backends')

test('preview, save, inspect and drain a tiering policy',async({page,request})=>{
  test.setTimeout(150000)
  const f=JSON.parse(readFileSync(process.env.MOKYU_TEST_REPLICATION_FIXTURE!,'utf8'))
  const password=readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!,'utf8').trim()
  const origin=process.env.MOKYU_TEST_WEB!, base='/api/v1/buckets/'+f.source.id+'/tiering'
  const login=await request.post('/api/v1/auth/login',{headers:{Origin:origin},data:{username:'tester',password}})
  expect(login.status()).toBe(200)
  const headers={Origin:origin,'X-CSRF-Token':(await login.json()).csrf_token}
  const preferences=async(locale:string,theme:string)=>expect((await request.put('/api/v1/account',{headers,data:{display_name:'',locale,theme,avatar_email:'',avatar_enabled:false}})).ok()).toBeTruthy()
  const get=async()=>(await request.get(base)).json()
  const {storages}=await (await request.get('/api/v1/bucket-projects')).json()
  const coldName=storages.find((s:{id:string})=>s.id===f.target.storage_id).name
  await preferences('en','light')
  await page.goto('/login')
  await page.getByRole('textbox',{name:en.username,exact:true}).fill('tester')
  await page.getByRole('textbox',{name:en.password,exact:true}).fill(password)
  await page.getByRole('button',{name:en.signIn,exact:true}).click()
  await expect(page).toHaveURL(/\/overview$/)
  const panel=page.getByRole('tabpanel'), errors:string[]=[]
  page.on('pageerror',e=>errors.push(e.message))
  async function open(title:string){
    await page.goto('/buckets/'+f.source.id)
    await page.getByRole('tab',{name:title,exact:true}).click()
    await expect(panel.getByRole('heading',{name:title,exact:true})).toBeVisible()
  }
  for(const [locale,theme,width,tr] of [['en','light',1280,en],['zh-CN','dark',375,zh],['ja','light',768,ja]] as const){
    await preferences(locale,theme);await page.setViewportSize({width,height:900});await open(tr.tierTitle)
    const initial=await get()
    const cold=panel.getByRole('combobox',{name:tr.tierCold,exact:true})
    await cold.click()
    const choices=page.getByRole('option')
    await expect(choices).toHaveCount(storages.filter((s:{id:string;type:string})=>s.id!==f.source.storage_id && s.type==='s3').length)
    await page.getByRole('option',{name:coldName,exact:true}).click()
    await panel.getByRole('spinbutton',{name:tr.tier_promote_reads}).fill('5')
    await expect(panel.getByRole('button',{name:tr.save,exact:true})).toBeDisabled()
    await panel.getByRole('button',{name:tr.tierPreview}).click()
    await expect(panel.getByText(tr.tierReviewed)).toBeVisible()
    expect(await get()).toEqual(initial)
    await panel.getByRole('button',{name:tr.save,exact:true}).click()
    await expect.poll(async()=>(await get()).policy?.promote_reads).toBe(5)
    await open(tr.tierTitle)
    await expect(panel.getByRole('spinbutton',{name:tr.tier_promote_reads})).toHaveValue('5')
    await panel.getByRole('button',{name:tr.tierCalculate}).click()
    await expect(panel.getByText(tr.tierCounted,{exact:true})).toBeVisible()
    if(locale==='en'){
      await panel.getByRole('spinbutton',{name:tr.tier_promote_reads}).fill('7')
      await panel.getByRole('button',{name:tr.tierPreview}).click()
      await expect(panel.getByText(tr.tierReviewed)).toBeVisible()
      expect((await request.post(base+'/state',{headers,data:{revision:(await get()).revision,action:'pause'}})).status()).toBe(200)
      await panel.getByRole('button',{name:tr.save,exact:true}).click()
      await expect(panel.getByText(tr.tierStale,{exact:false})).toBeVisible()
      expect((await get()).policy.promote_reads).toBe(5)
      await panel.getByRole('button',{name:tr.refresh,exact:true}).click()
    }else await panel.getByRole('button',{name:tr.tierPause}).click()
    await panel.getByRole('button',{name:tr.tierResume}).click()
    await panel.getByRole('button',{name:tr.tierDrain}).click()
    await expect.poll(async()=>(await get()).state).toBe('draining')
    await panel.getByRole('button',{name:tr.tierDisable}).click()
    await expect.poll(async()=>(await get()).state).toBe('disabled')
    expect(await page.evaluate(()=>document.documentElement.scrollWidth)).toBeLessThanOrEqual(width)
  }
  expect(errors).toEqual([])
})
