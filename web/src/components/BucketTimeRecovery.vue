<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Clock3, RotateCcw } from 'lucide-vue-next'
import { api, bytes, params, write, type Bucket } from '../api/client'
import { date, t } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'
import UiSelect from './ui/UiSelect.vue'
import UiDialog from './ui/UiDialog.vue'
import UiListFooter from './ui/UiListFooter.vue'
import type { components } from '../api/schema'
type Window = components['schemas']['BucketRecoveryWindow']
type Plan = components['schemas']['TimeRestorePlan']
const props = defineProps<{bucket:Bucket; buckets:Bucket[]}>()
const base = computed(() => '/api/v1/buckets/' + props.bucket.id)
const window = useQuery({queryKey:['recovery-window',base],queryFn:({signal}) => api<Window>(base.value+'/time-window',{signal})})
const seconds=ref(0),busy=ref(false),error=ref<unknown>(),open=ref(false),confirm=ref(false),plan=ref<Plan>()
const at=ref(''),scope=ref('prefix'),key=ref(''),target=ref(props.bucket.id),prefix=ref(''),place=ref('copy')
const scopes=computed(()=>['object','prefix','bucket'].map(value=>({value,label:t('timeScope_'+value)})))
const modes=computed(()=>['copy','in_place'].map(value=>({value,label:t('timeDest_'+value)})))
const targets=computed(()=>props.buckets.filter(b=>b.status==='active').map(b=>({value:b.id,label:b.name})))
const valid=computed(()=>at.value && (scope.value==='bucket'||key.value) && (place.value==='in_place'||prefix.value||target.value!==props.bucket.id))
const pageAfter=ref<string>();
const stamp=(us:number)=>date(new Date(us/1000).toISOString())
let timer:ReturnType<typeof setTimeout>|undefined
watch(()=>window.data.value,v=>{if(v)seconds.value=v.seconds},{immediate:true})
watch([at,scope,key,target,prefix,place],()=>{ if(plan.value?.phase==='preview')plan.value=undefined })
onBeforeUnmount(()=>clearTimeout(timer))
async function save(){busy.value=true;error.value=undefined;try{await write(base.value+'/time-window',{seconds:seconds.value},'PUT');await window.refetch();confirm.value=false}catch(e){error.value=e}finally{busy.value=false}}
function begin(){clearTimeout(timer);pageAfter.value=undefined;error.value=undefined;plan.value=undefined;const now=new Date((window.data.value?.latest_us??Date.now()*1000)/1000);at.value=new Date(now.getTime()-now.getTimezoneOffset()*60000).toISOString().slice(0,23);open.value=true}
async function preview(){busy.value=true;error.value=undefined;try{plan.value=await write<Plan>(base.value+'/time-restore',{at:new Date(at.value).toISOString(),scope:scope.value,key:scope.value==='bucket'?'':key.value,target_bucket:place.value==='in_place'?props.bucket.id:target.value,target_prefix:place.value==='in_place'?'':prefix.value,in_place:place.value==='in_place'})}catch(e){error.value=e}finally{busy.value=false}}
async function show(after?:string){if(!plan.value)return;pageAfter.value=after;clearTimeout(timer);try{plan.value=await api<Plan>('/api/v1/time-restores/'+plan.value.id+'?'+params({after}));if(plan.value.phase==='running')timer=setTimeout(()=>show(after),1500)}catch(e){error.value=e}}
async function start(){if(!plan.value)return;busy.value=true;error.value=undefined;try{plan.value=await write<Plan>('/api/v1/time-restores/'+plan.value.id,{digest:plan.value.digest});await show()}catch(e){error.value=e}finally{busy.value=false}}
</script>
<template>
<section class="time-window settings-section"><header><Clock3 :size="19"/><h3>{{t('timeTitle')}}</h3></header><p class="field-help">{{t('timeHint')}}</p>
<ErrorNotice v-if="window.error.value || (error && !open)" :error="window.error.value || error"/>
<template v-if="window.data.value">
<div class="time-settings"><label class="field"><span>{{t('timeWindowSeconds')}}</span><input v-model.number="seconds" type="number" min="0" max="315360000" step="1"></label>
<button :disabled="busy || seconds===window.data.value.seconds || !Number.isInteger(seconds) || seconds<0 || seconds>315360000" @click="seconds===0?confirm=true:save()">{{t('save')}}</button></div>
<dl v-if="window.data.value.earliest_us"><div><dt>{{t('timeEarliest')}}</dt><dd>{{stamp(window.data.value.earliest_us)}}</dd></div><div><dt>{{t('timeRetained')}}</dt><dd>{{bytes(window.data.value.retained_bytes)}}</dd></div></dl>
<div v-if="window.data.value.gaps.length" class="info-callout"><strong>{{t('timeGap')}}</strong><p v-for="g in window.data.value.gaps" :key="g.after_us">{{stamp(g.after_us)}} – {{stamp(g.before_us)}}</p></div>
<button v-if="window.data.value.seconds" @click="begin"><RotateCcw :size="15"/>{{t('timePreview')}}</button>
<button v-if="plan && plan.phase!=='preview'" @click="open=true;show()">{{t('timeResult')}}</button>
</template></section>
<UiDialog v-model:open="confirm" :title="t('timeDisable')" :description="t('timeDisableHint')" :busy="busy"><template #footer><button @click="confirm=false">{{t('cancel')}}</button><button class="primary" :disabled="busy" @click="save">{{t('confirm')}}</button></template></UiDialog>
<UiDialog v-model:open="open" medium :title="t('timeTitle')" :description="t('timePreviewHint')" :busy="busy">
<div class="time-form" v-if="!plan || plan.phase==='preview'">
<label class="field"><span>{{t('timeAt')}}</span><input v-model="at" type="datetime-local" step=".001"></label>
<UiSelect v-model="scope" :options="scopes" :label="t('timeScope')"/>
<label v-if="scope!=='bucket'" class="field"><span>{{t(scope==='object'?'timeScope_object':'timeScope_prefix')}}</span><input v-model="key" maxlength="1024"></label>
<UiSelect v-model="place" :options="modes" :label="t('timeDestination')"/>
<template v-if="place==='copy'"><UiSelect v-model="target" :options="targets" :label="t('timeDestination')"/><label class="field"><span>{{t('timePrefix')}}</span><input v-model="prefix" maxlength="1024"></label></template>
<p v-else class="info-callout">{{t('timeConflictHint')}}</p></div>
<ErrorNotice v-if="error" :error="error"/><div v-if="plan" class="time-results"><h3>{{t('timeResult')}}</h3><span class="badge">{{t('timeState_'+plan.phase)}}</span>
<p class="field-help">{{t('timeWatermark')}} {{plan.watermark}}</p>
<div class="time-counts"><span v-for="(n,label) in plan.counts" :key="label">{{t('timeState_'+label)}} · {{n}}</span></div>
<div v-for="i in plan.items" :key="i.key" class="time-row"><code>{{i.target}}</code><span>{{t('timeState_'+i.state)}}</span></div>
<UiListFooter v-if="plan.items.length" :summary="t('pageItems',{count:plan.items.length})" :first="!!pageAfter" :next="!!plan.next" @first="show()" @next="show(plan.next || undefined)"/></div>
<template #footer><button @click="open=false">{{t('close')}}</button><button v-if="!plan" class="primary" :disabled="!valid || busy" @click="preview">{{t('timePreview')}}</button><button v-if="plan?.phase==='preview'" class="primary" :disabled="busy || !!plan.counts.missing" @click="start">{{t('timeApply')}}</button></template>
</UiDialog>
</template>
<style scoped>
.time-window{margin-top:1.8rem;padding-top:1.5rem;border-top:1px solid var(--line)}header,.time-settings,.time-counts{display:flex;align-items:center;gap:.75rem;flex-wrap:wrap}.time-settings{align-items:flex-end;margin:1rem 0}.time-settings .field{width:14rem}dl{display:flex;gap:2rem;flex-wrap:wrap;margin:1rem 0}dt,.time-row span{color:var(--muted)}dd{margin:.25rem 0}.time-form,.time-results{display:grid;gap:.75rem}.time-results{margin-top:1rem}.time-row{display:flex;justify-content:space-between;gap:1rem;padding:.45rem 0;border-bottom:1px solid var(--line)}code{overflow-wrap:anywhere}.time-counts{font-size:.85rem}
</style>
