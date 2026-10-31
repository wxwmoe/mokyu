<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Database, Layers, ChartColumn, ListChecks } from 'lucide-vue-next'
import { api, ApiError, bytes, params, write, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'
import UiSelect from './ui/UiSelect.vue'

type View = components['schemas']['BucketTiering']
type Policy = components['schemas']['TieringPolicy']
type Action = components['schemas']['TieringAction']
type Usage = components['schemas']['TierUnitUsage']
type Candidate = components['schemas']['TieringUnit']
const props = defineProps<{ bucket: Bucket }>()
const base = computed(() => '/api/v1/buckets/' + props.bucket.id + '/tiering')
const view = useQuery({queryKey:['bucket-tiering',base],queryFn:({signal})=>api<View>(base.value,{signal})})
const options = useQuery({queryKey:['bucket-projects'],queryFn:({signal})=>api<components['schemas']['BucketCreationOptions']>('/api/v1/bucket-projects',{signal})})
const targets = computed(() => (options.data.value?.storages || []).filter(s=>s.id!==props.bucket.storage_id && (!options.data.value?.shared_storage_only || s.type==='s3')).map(s=>({value:s.id,label:s.name})))
const hot = computed(()=>options.data.value?.storages.find(s=>s.id===props.bucket.storage_id)?.name || props.bucket.storage_id)
const defaults = (): Policy => ({cold_storage:'',cool_after_seconds:2592000,observation_seconds:86400,promote_reads:3,residency_seconds:86400,parallelism:1,bytes_per_second:8388608})
const form=ref<Policy>(defaults()), revision=ref(-1), previous=ref(''), reviewed=ref(''), busy=ref(false), error=ref<unknown>()
const dirty=computed(()=>JSON.stringify(form.value)!==previous.value)
const stale=computed(()=>revision.value>=0 && view.data.value?.revision!==revision.value)
const locked=computed(()=>busy.value || revision.value<0 || stale.value || props.bucket.status!=='active')
const periods=['cool_after_seconds','observation_seconds','residency_seconds'] as const
type Period=(typeof periods)[number]
const scales=[86400,3600,60,1] as const
const scale=ref<Record<Period,number>>({cool_after_seconds:86400,observation_seconds:3600,residency_seconds:3600})
const unitText: Record<number,string>={1:'tierSeconds',60:'tierMinutes',3600:'tierHours',86400:'tierDays'}
const unitChoices=computed(()=>scales.map(v=>({value:String(v),label:t(unitText[v])})))
function quantity(key:Period,value:string) { form.value[key]=Number(value)*scale.value[key] }
function setScale(key:Period,value:string) { const n=form.value[key]/scale.value[key];scale.value[key]=Number(value);form.value[key]=n*scale.value[key] }
function load(v:View) {
  form.value=v.policy ? {...v.policy} : defaults()
  for(const key of periods) scale.value[key]=scales.find(n=>form.value[key]%n===0)||1
  previous.value=JSON.stringify(form.value);reviewed.value='';revision.value=v.revision
}
watch(()=>view.data.value,v=>{if(v && (revision.value<0 || !dirty.value))load(v)},{immediate:true})
watch(form,()=>{reviewed.value=''}, {deep:true})
async function reset() { const r=await view.refetch();if(r.data)load(r.data) }
async function change(action:'preview'|'save'|Action) {
  if(locked.value)return
  busy.value=true;error.value=undefined
  try {
    if(action==='preview'||action==='save'){
      const payload={revision:revision.value,policy:form.value}
      if(action==='save' && reviewed.value!==JSON.stringify(payload)) return
      const result=await write<View>(base.value+(action==='preview'?'/preview':''),payload,action==='preview'?'POST':'PUT')
      if(action==='preview') reviewed.value=JSON.stringify(payload)
      else {load(result);await view.refetch()}
    } else {
      const result=await write<View>(base.value+'/state',{action,revision:revision.value})
      load(result);await view.refetch()
    }
  } catch(e){error.value=e;if(e instanceof ApiError && e.status===409)await view.refetch()} finally{busy.value=false}
}
const unit=ref('chunk'), after=ref(''), history=ref<string[]>([])
watch(unit,()=>{after.value='';history.value=[]})
const reasons=useQuery({queryKey:['tier-reasons',base,unit,after],enabled:computed(()=>!!view.data.value?.policy),queryFn:({signal})=>api<Candidate[]>(base.value+'/units?'+params({unit:unit.value,after:after.value||undefined,limit:'16'}),{signal})})
const reasonUnit=computed(()=>(['chunk','pack','raw'] as const).map(value=>({value,label:t('tierUnit_'+value)})))
const metrics=['hot_bytes','cold_bytes','duplicate_bytes','retiring_bytes'] as const
const empty=()=>({hot_bytes:0,cold_bytes:0,duplicate_bytes:0,retiring_bytes:0,unconfirmed:0,units:0,phases:{} as Record<string,number>})
const totals=ref<ReturnType<typeof empty>>(), scanning=ref(false), complete=ref(false)
let scan:AbortController|undefined
async function usage() {
  scan?.abort();const controller=new AbortController();scan=controller
  const signal=controller.signal, total=empty(), path=base.value
  scanning.value=true;complete.value=false;totals.value={...total};error.value=undefined
  try {
    for(const type of ['chunk','pack','raw']){
      let cursor=''
      for(;;){
        const page:Usage[]=await api<Usage[]>(path+'/usage?'+params({unit:type,after:cursor||undefined,limit:'128'}),{signal})
        if(signal.aborted)return
        for(const row of page){for(const k of metrics)total[k]+=row[k];total.units++;if(row.unconfirmed)total.unconfirmed++;if(row.phase)total.phases[row.phase]=(total.phases[row.phase]||0)+1}
        totals.value={...total}
        if(page.length<128)break
        const last=page[page.length-1].id;if(last<=cursor)throw new Error(t('tierCursorError'));cursor=last
      }
    }
    complete.value=true
  }catch(e){if(!signal.aborted)error.value=e}finally{if(scan===controller)scanning.value=false}
}
onBeforeUnmount(()=>scan?.abort())
</script>
<template>
  <section class="bucket-panel form-stack">
    <header class="bucket-section-head">
      <div class="bucket-heading"><span class="policy-icon sky"><Layers :size="22" aria-hidden="true" /></span><div><h2>{{ t('tierTitle') }}</h2><p class="field-help">{{ t('tierHint') }}</p></div></div>
      <span v-if="view.data.value" class="badge node-state" :data-tone="view.data.value.state==='active'?'mint':'gray'">{{ t('tierState_'+view.data.value.state) }}</span>
    </header>
    <ErrorNotice :error="error || view.error.value || options.error.value || reasons.error.value"/>
    <p v-if="stale" class="bucket-stale" role="status">{{ t('tierStale') }} <button @click="reset">{{ t('refresh') }}</button></p>
    <form class="bucket-subpanel form-stack" @submit.prevent="change('preview')">
      <p v-if="view.data.value" class="tier-backend"><Database :size="16" aria-hidden="true" />{{ t('tierHot') }} <strong>{{ hot }}</strong></p>
      <label class="field"><span>{{ t('tierCold') }}</span><UiSelect v-model="form.cold_storage" :options="targets" :label="t('tierCold')" :disabled="locked || !targets.length"/></label>
      <p v-if="!targets.length && options.data.value" class="field-help">{{ t('tierNoStorage') }}</p>
      <div class="form-pair">
        <div v-for="key in periods" :key="key" class="field"><label :for="'tier-'+key">{{ t('tier_'+key) }}</label><div class="tier-period">
          <input :id="'tier-'+key" type="number" required min="1" step="1" :value="form[key]/scale[key]" :disabled="locked" @input="quantity(key,($event.target as HTMLInputElement).value)">
          <UiSelect :model-value="String(scale[key])" :label="t('tier_'+key)+' · '+t('tierTimeUnit')" :options="unitChoices" :disabled="locked" @update:model-value="setScale(key,$event)"/>
        </div></div>
        <label class="field"><span>{{ t('tier_promote_reads') }}</span><input v-model.number="form.promote_reads" type="number" min="2" step="1" required :disabled="locked"></label>
        <label class="field"><span>{{ t('tier_parallelism') }}</span><input v-model.number="form.parallelism" type="number" min="1" max="32" step="1" required :disabled="locked"></label>
        <label class="field"><span>{{ t('tier_bytes_per_second') }}</span><input v-model.number="form.bytes_per_second" type="number" min="1" step="1" required :disabled="locked"><small>{{ bytes(form.bytes_per_second || 0) }}/s</small></label>
      </div>
      <div class="form-footer"><button type="submit" :disabled="locked || !form.cold_storage">{{ t('tierPreview') }}</button>
        <button type="button" class="primary" :disabled="locked || !reviewed" @click="change('save')">{{ t('save') }}</button>
        <button type="button" :disabled="busy || !dirty" @click="reset">{{ t('cancel') }}</button></div>
      <p v-if="reviewed" class="field-help" role="status">{{ t('tierReviewed') }}</p>
      <div v-if="view.data.value?.policy" class="actions">
        <button type="button" v-if="view.data.value.state==='active'" :disabled="locked || dirty" @click="change('pause')">{{ t('tierPause') }}</button>
        <button type="button" v-else :disabled="locked || dirty" @click="change('resume')">{{ t('tierResume') }}</button>
        <button type="button" :disabled="locked || dirty || view.data.value.state==='draining'" @click="change('drain')">{{ t('tierDrain') }}</button>
        <button type="button" :disabled="locked || dirty" @click="change('disable')">{{ t('tierDisable') }}</button>
      </div>
    </form>
    <section class="bucket-subpanel form-stack">
      <header class="bucket-section-head"><div class="bucket-heading"><span class="chunk-bead mint"><ChartColumn :size="18" aria-hidden="true" /></span><div><h3>{{ t('tierUsage') }}</h3><p class="field-help">{{ t('tierUsageHint') }}</p></div></div>
        <button v-if="!scanning" @click="usage">{{ t('tierCalculate') }}</button><button v-else @click="scan?.abort()">{{ t('cancel') }}</button>
      </header>
      <dl v-if="totals" class="bucket-metrics" aria-live="polite">
        <div v-for="key in metrics" :key="key"><dt>{{ t('tier_'+key) }}</dt><dd>{{ bytes(totals[key]) }}</dd></div>
        <div v-for="(count,phase) in totals.phases" :key="phase"><dt>{{ t('tierPhase_'+phase) }}</dt><dd>{{ count }}</dd></div>
        <div><dt>{{ t('tierUnconfirmed') }}</dt><dd>{{ totals.unconfirmed }}</dd></div><div><dt>{{ t(complete?'tierCounted':'tierPartial') }}</dt><dd>{{ totals.units }}</dd></div>
      </dl>
    </section>
    <section v-if="view.data.value?.policy" class="bucket-subpanel form-stack">
      <header class="bucket-section-head"><div class="bucket-heading"><span class="chunk-bead lilac"><ListChecks :size="18" aria-hidden="true" /></span><h3>{{ t('tierDecisions') }}</h3></div>
        <UiSelect v-model="unit" :options="reasonUnit" :label="t('tierDecisions')"/>
      </header>
      <p v-for="r in reasons.data.value" :key="r.id"><code>{{ r.id }}</code> · {{ bytes(r.bytes) }} · {{ t('tierReason_'+r.reason) }}<span v-if="r.last_read"> · {{ t('tierLastRead') }}: {{ new Date(r.last_read*1000).toLocaleString() }}</span></p>
      <div class="actions"><button :disabled="!history.length" @click="after=history.pop() || ''">{{ t('previous') }}</button><button :disabled="reasons.data.value?.length!==16" @click="history.push(after);after=reasons.data.value![15].id">{{ t('next') }}</button></div>
    </section>
  </section>
</template>
<style scoped>
.tier-period { display:grid;grid-template-columns:minmax(3.5rem,1fr) minmax(5.5rem,1fr);gap:.5rem }
.tier-period input { min-width:0 }
.tier-backend { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; color: var(--muted); }
.tier-backend strong { color: var(--ink); overflow-wrap: anywhere; }
.tier-backend svg { color: var(--sky); }
code { overflow-wrap:anywhere }
</style>
