<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ChevronLeft, ChevronRight, Download, Images, List, Pencil, Plus, Save, Search, Trash2, Upload } from 'lucide-vue-next'
import { api, ApiError, bytes, params, queries, write, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'
import UiCheckbox from './ui/UiCheckbox.vue'
import UiDialog from './ui/UiDialog.vue'
import ImageRuleEditor from './ImageRuleEditor.vue'
import ImageStyleImport from './ImageStyleImport.vue'
import ImageRulePreview from './ImageRulePreview.vue'
type Page=components['schemas']['ImageStylePage']
type Policy=components['schemas']['ImagePolicy']
type Style=components['schemas']['ImageStyle']
type Settings=components['schemas']['ImageSettings']
const props=defineProps<{bucket:Bucket}>(), emit=defineEmits<{dirty:[value:boolean]}>()
const base=computed(()=>'/api/v1/buckets/'+props.bucket.id+'/image-processing')
const after=ref(''), history=ref<string[]>([]), prefix=ref(''), filter=ref(''), selected=ref<string[]>([])
const pageSize=50
const page=useQuery({
  queryKey:['image-styles',base,after,filter],
  queryFn:({signal})=>api<Page>(base.value+'/styles?'+params({after:after.value,prefix:filter.value,limit:String(pageSize+1)}),{signal}),
  select:data=>({...data,styles:data.styles.slice(0,pageSize),hasNext:data.styles.length>pageSize}),
})
const policy=ref<Policy>(), baseline=ref(''), revision=ref(-1), busy=ref(false), error=ref<unknown>(), importing=ref(false), importDirty=ref(false)
const editor=ref<Style>(), editorOpen=ref(false), original=ref(''), editRevision=ref(-1), remove=ref<Style>()
const dirty=computed(()=>!!policy.value&&JSON.stringify(policy.value)!==baseline.value)
const stale=computed(()=>revision.value>=0&&page.data.value?.settings.revision!==revision.value)
const locked=computed(()=>busy.value||!policy.value||props.bucket.status!=='active')
const formats=['jxl','avif','webp','jpeg','png'] as const
const delimiters=['-','_','/','!']
const prefixes=computed(()=>Array.from(new Set((page.data.value?.styles||[]).map(s=>s.name.includes('.')?s.name.slice(0,s.name.indexOf('.')+1):'').filter(Boolean))))
const inputMB=computed({get:()=>(policy.value?.input_limit||100000000)/1000000,set:n=>{if(policy.value)policy.value.input_limit=Math.round(n*1000000)}})
const clone=<T,>(value:T):T=>JSON.parse(JSON.stringify(value))
const inputMP=computed({get:()=>(policy.value?.max_pixels??150000000)/1000000,set:n=>{if(policy.value)policy.value.max_pixels=Math.round(n*1000000)}})
function load(settings:Settings){policy.value=clone(settings.policy);baseline.value=JSON.stringify(policy.value);revision.value=settings.revision}
watch([page.data,page.isFetching],([v,fetching])=>{
  if(!v)return
  if(revision.value<0||!dirty.value)load(v.settings)
  if(!fetching&&!v.styles.length&&history.value.length)previous()
},{immediate:true})
watch([dirty,editorOpen,importDirty],()=>emit('dirty',dirty.value||editorOpen.value||importDirty.value))
watch([after,filter],()=>{selected.value=[]})
function mapped(enabled:boolean) {if(policy.value)policy.value.mapping=enabled?{auto:'auto',formats:{jpeg:'jpeg'}}:null}
function delimiter(value:string,enabled:boolean){if(policy.value)policy.value.delimiters=enabled?[...policy.value.delimiters,value]:policy.value.delimiters.filter(v=>v!==value)}
function mapping(format:string,value:string){const m=policy.value?.mapping;if(m){if(value.trim())m.formats[format]=value.trim();else delete m.formats[format]}}
async function reset(){const r=await page.refetch();if(r.data)load(r.data.settings)}
function refreshStyles(){return queries.invalidateQueries({queryKey:['image-styles',base.value]})}
async function savePolicy(){
  if(locked.value||!policy.value||stale.value)return
  busy.value=true;error.value=undefined
  try{load(await write<Settings>(base.value,{revision:revision.value,policy:policy.value,edits:[]},'PUT'));await refreshStyles()}
  catch(e){error.value=e;if(e instanceof ApiError&&e.status===409)await page.refetch()}finally{busy.value=false}
}
function edit(style?:Style){
  original.value=style?.name||'';editRevision.value=page.data.value?.settings.revision??-1
  editor.value=style?clone(style):{name:'',enabled:true,rule:{stages:[{operations:[{operation:'orient'}],format:'auto',quality:90,quality_mode:'cap',progressive:false}]}}
  editorOpen.value=true
}
async function saveStyle(){
  if(!editor.value||busy.value)return
  busy.value=true;error.value=undefined
  try{
    const style=editor.value
    if(style.name!==original.value){
      const check=await api<Page>(base.value+'/styles?'+params({prefix:style.name,limit:'1'}))
      if(check.styles.some(s=>s.name===style.name))throw new Error(t('imageNameExists'))
      if(check.settings.revision!==editRevision.value)throw new ApiError(409,'OperationAborted')
    }
    const edits:components['schemas']['ImageStyleEdit'][]=[]
    if(original.value&&original.value!==style.name)edits.push({action:'remove',name:original.value})
    edits.push({action:'put',style})
    await write(base.value,{revision:editRevision.value,edits},'PUT')
    editorOpen.value=false;await refreshStyles()
  }catch(e){error.value=e}finally{busy.value=false}
}
async function deleteStyle(){
  if(!remove.value||busy.value)return
  busy.value=true;error.value=undefined
  try{await write(base.value,{revision:page.data.value?.settings.revision,edits:[{action:'remove',name:remove.value.name}]},'PUT');remove.value=undefined;await refreshStyles()}
  catch(e){error.value=e}finally{busy.value=false}
}
function select(name:string,value:boolean){selected.value=value?[...selected.value,name]:selected.value.filter(n=>n!==name)}
function search(){after.value='';history.value=[];filter.value=prefix.value.trim()}
function next(){const data=page.data.value;if(data?.hasNext){history.value.push(after.value);after.value=data.styles.at(-1)!.name}}
function previous(){after.value=history.value.pop()||''}
const allExport=computed(()=>base.value+'/styles/export?'+params({revision:String(page.data.value?.settings.revision??0),prefix:filter.value}))
let downloadUrl=''
async function exportSelection(){
  if(!selected.value.length||busy.value)return
  busy.value=true;error.value=undefined
  try{
    const data=await write<components['schemas']['ImageExportPage']>(base.value+'/styles/export',{revision:page.data.value?.settings.revision,names:selected.value})
    if(downloadUrl)URL.revokeObjectURL(downloadUrl)
    downloadUrl=URL.createObjectURL(new Blob([data.text],{type:'text/plain;charset=utf-8'}))
    const link=document.createElement('a');link.href=downloadUrl;link.download='image-styles.txt';link.click()
  }catch(e){error.value=e}finally{busy.value=false}
}
onBeforeUnmount(()=>{if(downloadUrl)URL.revokeObjectURL(downloadUrl);emit('dirty',false)})
</script>
<template>
  <section class="bucket-panel form-stack">
    <header class="bucket-section-head">
      <div class="bucket-heading"><span class="policy-icon rose"><Images :size="22" aria-hidden="true" /></span><h2>{{ t('imageTitle') }}</h2></div>
      <UiCheckbox v-if="policy" v-model="policy.enabled" :label="t('imageEnabled')" :disabled="locked" />
    </header>
    <ErrorNotice :error="error || page.error.value" />
    <p v-if="page.isPending.value" role="status">{{ t('loading') }}</p>
    <p v-if="stale" class="bucket-stale" role="status">{{ t('imageStale') }} <button @click="reset">{{ t('refresh') }}</button></p>
    <form v-if="policy" class="form-stack" @submit.prevent="savePolicy">
      <fieldset :disabled="locked" class="form-stack">
        <section class="bucket-subpanel form-stack">
          <h3>{{ t('imageLimits') }}</h3>
          <div class="form-pair">
            <label class="field"><span>{{ t('imageInputLimit') }}</span><input v-model.number="inputMB" required type="number" min="0.000001" max="1000" step="0.000001"></label>
            <label class="field"><span>{{ t('imagePixelLimit') }}</span><input v-model.number="inputMP" required type="number" min="0.000001" max="150" step="0.000001"></label>
          </div>
          <p class="field-help">{{ t('imagePixelLimitHint') }}</p>
        </section>
        <section class="bucket-subpanel form-stack">
          <h3>{{ t('imageAccess') }}</h3>
          <div class="image-setting"><UiCheckbox v-model="policy.protect_original" :label="t('imageProtect')" :disabled="locked" /><p class="field-help">{{ t('imageProtectHint') }}</p></div>
          <div class="image-setting" role="group" :aria-label="t('imageDelimiters')"><strong>{{ t('imageDelimiters') }}</strong>
            <div class="actions image-delimiters"><UiCheckbox v-for="d in delimiters" :key="d" :model-value="policy.delimiters.includes(d)" :label="d" :disabled="locked" @update:model-value="delimiter(d,$event)" /></div>
          </div>
          <div class="image-setting"><UiCheckbox :model-value="!!policy.mapping" :label="t('imageMapping')" :disabled="locked" @update:model-value="mapped" /><p v-if="policy.mapping" class="field-help">{{ t('imageMappingHint') }}</p></div>
          <template v-if="policy.mapping">
            <label class="field"><span>{{ t('imageAutoTag') }}</span><input v-model="policy.mapping.auto" maxlength="128" required placeholder="auto"></label>
            <div class="form-pair"><label v-for="fmt in formats" :key="fmt" class="field"><span>{{ t('imageFormat_'+fmt) }}</span><input :value="policy.mapping.formats[fmt] || ''" maxlength="128" :required="fmt==='jpeg'" :placeholder="fmt==='jpeg'?'jpeg':t('imageMappingOff')" @input="mapping(fmt,($event.target as HTMLInputElement).value)"></label></div>
          </template>
          <p class="field-help">{{ t('imageDefaultHint') }}</p>
        </section>
      </fieldset>
      <div class="form-footer"><button type="button" :disabled="locked || !dirty" @click="reset">{{ t('cancel') }}</button><button class="primary" :disabled="locked || !dirty || stale"><Save :size="16" />{{ t('save') }}</button></div>
    </form>
    <section class="form-stack">
      <header class="bucket-section-head">
        <div class="bucket-heading"><span class="chunk-bead lilac"><List :size="18" aria-hidden="true" /></span><h3>{{ t('imageStyles') }}</h3></div>
        <div class="actions">
          <button class="primary" :disabled="locked || dirty" @click="edit()"><Plus :size="16" />{{ t('imageAddStyle') }}</button>
          <button :disabled="locked || dirty" :aria-expanded="importing" @click="importing=!importing"><Upload :size="16" />{{ t('imageImport') }}</button>
          <a v-if="page.data.value" class="button" :href="allExport" download><Download :size="16" />{{ t('imageExportAll') }}</a>
          <button :disabled="busy || !selected.length" @click="exportSelection">{{ t('imageExportSelected') }}</button>
        </div>
      </header>
      <p v-if="dirty" class="field-help">{{ t('imageSaveSettingsFirst') }}</p>
      <ImageStyleImport v-if="importing" :base="base" :prefixes="prefixes" @changed="refreshStyles" @dirty="importDirty=$event" />
      <form class="bucket-search" @submit.prevent="search"><label class="field"><span>{{ t('imageNamePrefix') }}</span><input v-model="prefix" maxlength="128"></label><button><Search :size="16" />{{ t('imageSearch') }}</button></form>
      <div v-if="page.data.value" class="table-scroll"><table class="style-table">
        <thead><tr><th>{{ t('imageName') }}</th><th>{{ t('imageOutput') }}</th><th>{{ t('imageStyleStatus') }}</th><th>{{ t('imageActions') }}</th></tr></thead>
        <tbody><tr v-for="style in page.data.value.styles" :key="style.name">
          <td><UiCheckbox :model-value="selected.includes(style.name)" :label="style.name" @update:model-value="select(style.name,$event)" /></td>
          <td>{{ t('imageFormat_'+style.rule.stages.at(-1)?.format) }}</td>
          <td><span class="badge node-state" :data-tone="style.enabled?'mint':'gray'">{{ t(style.enabled?'imageStyleEnabled':'disabled') }}</span></td>
          <td><div class="style-row-actions">
            <button class="icon-button" :disabled="locked || dirty" :aria-label="t('imageEditStyle',{name:style.name})" @click="edit(style)"><Pencil :size="16" /></button>
            <button class="icon-button" :disabled="locked || dirty" :aria-label="t('imageDeleteStyle',{name:style.name})" @click="remove=style"><Trash2 :size="16" /></button>
          </div></td>
        </tr></tbody>
      </table><p v-if="!page.data.value.styles.length" class="styles-empty">{{ t('imageNoStyles') }}</p></div>
      <div class="actions bucket-actions-end"><button :disabled="!history.length || page.isFetching.value" @click="previous"><ChevronLeft :size="15" />{{ t('previous') }}</button><button :disabled="!page.data.value?.hasNext || page.isFetching.value" @click="next">{{ t('next') }}<ChevronRight :size="15" /></button></div>
    </section>
    <UiDialog v-model:open="editorOpen" :title="t(original?'imageEditTitle':'imageAddStyle')" :busy="busy" medium>
      <form v-if="editor" id="image-style-editor" class="form-stack image-editor" @submit.prevent="saveStyle">
        <ErrorNotice :error="error" />
        <div class="image-style-identity">
          <label class="field"><span>{{ t('imageName') }}</span><input v-model="editor.name" required maxlength="128" pattern="[A-Za-z0-9][A-Za-z0-9._\x2D]*" :disabled="busy"></label>
          <UiCheckbox v-model="editor.enabled" :label="t('imageStyleEnabled')" :disabled="busy" />
        </div>
        <ImageRuleEditor v-model="editor.rule" :disabled="busy" />
        <ImageRulePreview :base="base" :revision="editRevision" :rule="editor.rule" :disabled="busy" />
      </form>
      <template #footer><button :disabled="busy" @click="editorOpen=false">{{ t('cancel') }}</button><button class="primary" form="image-style-editor" :disabled="busy"><Save :size="16" />{{ t('save') }}</button></template>
    </UiDialog>
    <UiDialog :open="!!remove" :title="t('imageDeleteStyle',{name:remove?.name||''})" :description="t('imageDeleteHint')" :busy="busy" @update:open="!$event && (remove=undefined)">
      <ErrorNotice :error="error" /><template #footer><button :disabled="busy" @click="remove=undefined">{{ t('cancel') }}</button><button class="danger" :disabled="busy" @click="deleteStyle">{{ t('imageRemove') }}</button></template>
    </UiDialog>
  </section>
</template>
<style scoped>
.image-setting { display: grid; gap: 8px; min-width: 0; }
.image-setting > strong { font-size: var(--small-size, 12px); }
.image-delimiters .check-field { min-width: 48px; padding: 6px 9px; background: var(--surface); border: 1px solid var(--line); border-radius: 10px; }
.style-row-actions { display: inline-flex; align-items: center; gap: 4px; }
.style-row-actions .icon-button:last-child:hover { background: var(--danger-soft); color: var(--danger); }
.styles-empty { text-align: center; padding: 24px; color: var(--muted); }
.image-style-identity { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 16px; align-items: end; }
.image-style-identity .check-field { min-height: 42px; }
.image-editor { gap: 20px; }
@media (max-width: 640px) {
  .style-table thead { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); }
  .style-table tbody { display: block; }
  .style-table tr { display: grid; grid-template-columns: auto minmax(0, 1fr) auto; grid-template-areas: "name name tools" "format status tools"; align-items: center; gap: 4px 8px; padding: 10px 2px; border-bottom: 1px solid var(--line-soft); }
  .style-table td { padding: 0; border: 0; }
  .style-table td:first-child { grid-area: name; min-width: 0; }
  .style-table td:nth-child(2) { grid-area: format; color: var(--muted); font-size: var(--small-size, 12px); }
  .style-table td:nth-child(3) { grid-area: status; }
  .style-table td:last-child { grid-area: tools; }
}
@media (max-width: 400px) { .image-style-identity { grid-template-columns: minmax(0, 1fr); gap: 8px; } }
</style>
