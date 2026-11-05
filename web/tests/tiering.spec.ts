import { randomUUID } from 'node:crypto'
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
    await expect(panel.getByRole('heading',{name:tr.tierDecisions})).toBeVisible()
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

test('only global administrators can inspect tier decisions', async ({ page, request, playwright }) => {
  const { source, target } = JSON.parse(readFileSync(process.env.MOKYU_TEST_REPLICATION_FIXTURE!, 'utf8'))
  const origin = process.env.MOKYU_TEST_WEB!
  const login = await request.post('/api/v1/auth/login', { headers: { Origin: origin }, data: {
    username: 'tester', password: readFileSync(process.env.MOKYU_TEST_PASSWORD_FILE!, 'utf8').trim(),
  } })
  expect(login.status()).toBe(200)
  const headers = { Origin: origin, 'X-CSRF-Token': (await login.json()).csrf_token }
  const bucket = (await (await request.get('/api/v1/buckets')).json()).find((b: { id: string }) => b.id === source.id)
  const base = '/api/v1/buckets/' + bucket.id + '/tiering', units = base + '/units?unit=raw'
  const original = await (await request.get(base)).json()
  const username = 'tier-' + randomUUID().slice(0, 8), password = randomUUID()
  const grants = [{ bucket_id: bucket.id, actions: ['bucket.list', 'bucket.settings'] }]
  let user = '', revision = 0
  const tokens: string[] = []
  try {
    if (!original.policy) {
      const saved = await request.put(base, { headers, data: { revision: original.revision, policy: {
        cold_storage: target.storage_id, cool_after_seconds: 86400, observation_seconds: 60,
        promote_reads: 3, residency_seconds: 86400, parallelism: 1, bytes_per_second: 1048576,
      } } })
      expect(saved.status()).toBe(200)
      revision = (await saved.json()).revision
    }
    expect((await request.get(units)).status()).toBe(200)
    for (const system of [false, true]) {
      const issued = await request.post('/api/v1/api-tokens', { headers, data: { label: username, system, grants: system ? [] : grants } })
      expect(issued.status()).toBe(201)
      const { token, secret } = await issued.json()
      tokens.push(token.id)
      const client = await playwright.request.newContext({ baseURL: origin, extraHTTPHeaders: { Authorization: 'Bearer ' + secret } })
      try {
        expect((await client.get(base + '/usage?unit=raw')).status()).toBe(200)
        for (const method of ['GET', 'HEAD']) expect((await client.fetch(units, { method })).status()).toBe(system ? 200 : 403)
      } finally { await client.dispose() }
    }
    const created = await request.post('/api/v1/users', { headers, data: { username, password, role: 'member', must_change_password: false } })
    expect(created.status()).toBe(201)
    user = (await created.json()).id
    const membership = await request.put('/api/v1/projects/' + bucket.project_id + '/members/' + user, { headers, data: { role: 'maintainer', scope: 'selected', grants } })
    expect(membership.ok()).toBeTruthy()
    const member = page.request
    const signed = await member.post('/api/v1/auth/login', { headers: { Origin: origin }, data: { username, password } })
    expect(signed.status()).toBe(200)
    expect((await member.put('/api/v1/account', { headers: { Origin: origin, 'X-CSRF-Token': (await signed.json()).csrf_token }, data: {
      display_name: '', locale: 'en', theme: 'light', avatar_email: '', avatar_enabled: false,
    } })).ok()).toBeTruthy()
    for (const method of ['GET', 'HEAD']) expect((await member.fetch(units, { method })).status()).toBe(403)
    const calls: string[] = []
    page.on('request', r => { if (new URL(r.url()).pathname === base + '/units') calls.push(r.method()) })
    await page.goto('/buckets/' + bucket.id)
    await page.getByRole('tab', { name: en.tierTitle, exact: true }).click()
    await expect(page.getByRole('button', { name: en.tierDisable })).toBeVisible()
    await page.getByRole('button', { name: en.tierCalculate }).click()
    await expect(page.getByText(en.tierCounted, { exact: true })).toBeVisible()
    await expect(page.getByRole('heading', { name: en.tierDecisions })).toHaveCount(0)
    expect(calls).toEqual([])
  } finally {
    for (const token of tokens) await request.delete('/api/v1/api-tokens/' + token, { headers })
    if (user) await request.delete('/api/v1/users/' + user, { headers })
    if (revision) await request.post(base + '/state', { headers, data: { revision, action: 'disable' } })
  }
})
