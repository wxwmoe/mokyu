<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { FileText, Upload } from 'lucide-vue-next'
import { write } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'
import UiSelect from './ui/UiSelect.vue'
import UiCheckbox from './ui/UiCheckbox.vue'
type Result = components['schemas']['StyleImportResult']
const props=defineProps<{ base:string; prefixes:string[] }>()
const emit=defineEmits<{ changed:[]; dirty:[value:boolean] }>()
const prefix=ref(''), conflict=ref('error'), busy=ref(false), error=ref<unknown>(), text=ref(''), offset=ref(0), applied=ref(false), ended=ref(false)
const preview=ref<Result>(), selected=ref<number[]>([]), processed=ref(0), filename=ref('')
let iterator:AsyncGenerator<string>|undefined
const policies=computed(()=>['error','skip','replace'].map(value=>({value,label:t('imageConflict_'+value)})))
watch([prefix,conflict],()=>{preview.value=undefined;applied.value=false})
watch([text,applied],()=>emit('dirty',!!text.value&&!applied.value))
async function* lines(file:File) {
  const reader=file.stream().getReader(), decoder=new TextDecoder('utf-8',{fatal:true});let pending=''
  try{
    for(;;){
      const part=await reader.read();pending+=decoder.decode(part.value,{stream:!part.done})
      for(let cut;(cut=pending.indexOf('\n'))>=0;){
        const row=pending.slice(0,cut).replace(/\r$/,'');pending=pending.slice(cut+1)
        if(new TextEncoder().encode(row).length>8352)throw new Error(t('imageLineTooLong'))
        yield row
      }
      if(new TextEncoder().encode(pending).length>8352)throw new Error(t('imageLineTooLong'))
      if(part.done){if(pending)yield pending.replace(/\r$/,'');return}
    }
  }finally{await reader.cancel();reader.releaseLock()}
}
async function next() {
  if(!iterator||busy.value)return
  busy.value=true;error.value=undefined
  try{
    const rows:string[]=[]
    for(let n=0;n<128;n++){const v=await iterator.next();if(v.done){ended.value=true;break}rows.push(v.value)}
    offset.value=processed.value;processed.value+=rows.length;text.value=rows.join('\n');preview.value=undefined;selected.value=[];applied.value=false
  }catch(e){error.value=e}finally{busy.value=false}
  if(text.value)await check()
}
async function pick(event:Event) {
  const file=(event.target as HTMLInputElement).files?.[0];if(!file)return
  await iterator?.return(undefined);iterator=lines(file);filename.value=file.name;processed.value=0;ended.value=false
  await next()
}
async function check() {
  if(!text.value||busy.value)return
  busy.value=true;error.value=undefined
  try{
    preview.value=await write<Result>(props.base+'/styles/import',{text:text.value,prefix:prefix.value,conflict:conflict.value,apply:false})
    selected.value=preview.value.rows.filter(r=>!r.error).map(r=>r.line)
  }catch(e){error.value=e}finally{busy.value=false}
}
async function apply() {
  if(!preview.value||!selected.value.length||busy.value||applied.value)return
  busy.value=true;error.value=undefined
  try{
    const result=await write<Result>(props.base+'/styles/import',{text:text.value,prefix:prefix.value,conflict:conflict.value,selected:selected.value,revision:preview.value.revision,apply:true})
    preview.value=result;applied.value=result.applied
    if(result.applied)emit('changed')
  }catch(e){error.value=e}finally{busy.value=false}
}
function select(line:number,value:boolean) {selected.value=value?[...selected.value,line]:selected.value.filter(n=>n!==line)}
onBeforeUnmount(()=>{void iterator?.return(undefined);emit('dirty',false)})
</script>
<template>
  <section class="bucket-subpanel form-stack">
    <header class="bucket-heading"><span class="chunk-bead lilac"><Upload :size="18" aria-hidden="true" /></span><div><h3>{{ t('imageImport') }}</h3><p class="field-help">{{ t('imageImportHint') }}</p></div></header>
    <ErrorNotice :error="error"/>
    <label class="field"><span>{{ t('imageStyleFile') }}</span>
      <span class="file-field">
        <span class="button"><FileText :size="16" aria-hidden="true" />{{ t('chooseFiles') }}</span><span v-if="filename">{{ filename }}</span>
        <input type="file" :aria-label="t('imageStyleFile')" accept=".txt,text/plain" :disabled="busy" @change="pick">
      </span>
    </label>
    <div class="form-pair"><label class="field"><span>{{ t('imagePrefix') }}</span><input v-model="prefix" maxlength="128" list="image-import-prefixes" :disabled="busy||applied"><datalist id="image-import-prefixes"><option v-for="p in prefixes" :key="p" :value="p"/></datalist></label><label class="field"><span>{{ t('imageConflict') }}</span><UiSelect v-model="conflict" :options="policies" :label="t('imageConflict')" :disabled="busy||applied"/></label></div>
    <p v-if="filename&&text">{{ t('imageBatchLines',{start:offset+1,end:processed}) }}</p>
    <div class="actions"><button type="button" :disabled="busy||!text||applied" @click="check">{{ t('imageCheckImport') }}</button><button type="button" class="primary" :disabled="busy||!preview||!selected.length||applied" @click="apply">{{ t(applied?'imageApplied':'imageApplySelected') }}</button><button type="button" :disabled="busy||!iterator||ended||(!applied&&!!text)" @click="next">{{ t('imageNextBatch') }}</button></div>
    <button v-if="text&&!applied&&!ended" type="button" :disabled="busy" @click="next">{{ t('imageSkipBatch') }}</button>
    <div v-if="preview" class="table-scroll"><table><thead><tr><th>{{ t('imageImportLine') }}</th><th>{{ t('imageName') }}</th><th>{{ t('imageImportResult') }}</th></tr></thead><tbody><tr v-for="row in preview.rows" :key="row.line"><td><UiCheckbox :model-value="selected.includes(row.line)" :label="String(offset+row.line)" :disabled="busy||applied||!!row.error" @update:model-value="select(row.line,$event)"/></td><td><code>{{ row.name }}</code></td><td><span :class="{error:!!row.error}">{{ row.error||t('imageImport_'+row.outcome) }}</span><details v-if="row.body"><summary>{{ t('imageCanonical') }}</summary><code class="image-body">{{ row.body }}</code></details></td></tr></tbody></table></div>
  </section>
</template>
<style scoped>
.file-field { display: flex; position: relative; align-items: center; flex-wrap: wrap; gap: 8px 12px; width: fit-content; max-width: 100%; overflow-wrap: anywhere; }
.file-field .button { color: var(--rose); }
.file-field input[type='file'] { position: absolute; inset: 0; width: 100%; height: 100%; padding: 0; opacity: 0; cursor: pointer; }
.file-field:focus-within .button { outline: 2px solid var(--rose); outline-offset: 2px; }
.file-field:hover .button { background: var(--rose-soft); border-color: var(--rose-line); }
.file-field:has(:disabled) { opacity: .5; }
.image-body { white-space: pre-wrap; overflow-wrap: anywhere; }
</style>
