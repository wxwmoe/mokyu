<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { Eye } from 'lucide-vue-next'
import { api, bytes, params } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'
type Rule=components['schemas']['Pipeline']
type Info={width:number;height:number;format:string;range:string}
const props=defineProps<{base:string;revision:number;rule:Rule;disabled?:boolean}>()
const key=ref(''), busy=ref(false), error=ref<unknown>(), result=ref<{source:Info;output:Info;original:string;url:string;elapsed:number;input:number;bytes:number}>(), unsupported=ref(false)
let controller:AbortController|undefined
function clear(){controller?.abort();if(result.value)URL.revokeObjectURL(result.value.url);result.value=undefined;unsupported.value=false}
watch(()=>[props.rule,props.revision,key.value],clear,{deep:true})
async function preview(){
  clear();const current=new AbortController();controller=current;busy.value=true;error.value=undefined
  let response:Response|undefined
  try{
    response=await api<Response>(props.base+'/preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:key.value,revision:props.revision,rule:props.rule}),signal:current.signal},'response')
    const source=JSON.parse(response.headers.get('x-mokyu-image-source')||'null') as Info
    const output=JSON.parse(response.headers.get('x-mokyu-image-output')||'null') as Info
    const version=response.headers.get('x-mokyu-image-version')
    if(!source?.width||!output?.width||!version)throw new Error(t('imagePreviewUnsupported'))
    const blob=await response.blob();if(current.signal.aborted)return
    result.value={source,output,original:props.base.replace(/\/image-processing$/,'')+'/object/content?'+params({key:key.value,version,preview:'true'}),url:URL.createObjectURL(blob),elapsed:Number(response.headers.get('x-mokyu-image-elapsed-ms')),input:Number(response.headers.get('x-mokyu-image-source-bytes')),bytes:blob.size}
  }catch(e){if(!current.signal.aborted)error.value=e;try{await response?.body?.cancel()}catch{/* a consumed body is already released */}}
  finally{if(controller===current)busy.value=false}
}
onBeforeUnmount(clear)
</script>
<template>
  <section class="bucket-subpanel form-stack image-preview">
    <header class="bucket-heading"><span class="chunk-bead sky"><Eye :size="18" aria-hidden="true" /></span><div><h3>{{ t('imagePreview') }}</h3><p class="field-help">{{ t('imagePreviewHint') }}</p></div></header>
    <ErrorNotice :error="error"/>
    <div class="bucket-search"><label class="field"><span>{{ t('imagePreviewKey') }}</span><input v-model="key" maxlength="1024" :disabled="busy||disabled"></label>
    <button type="button" :disabled="busy||disabled||!key" @click="preview"><Eye :size="16" />{{ t('imagePreview') }}</button></div>
    <div v-if="result" class="form-pair">
      <figure><figcaption>{{ t('imagePreviewOriginal') }} · {{ result.source.width }} × {{ result.source.height }} · {{ bytes(result.input) }}</figcaption><img :src="result.original" :alt="t('imagePreviewOriginal')" loading="lazy" decoding="async"></figure>
      <figure><figcaption>{{ t('imagePreviewOutput') }} · {{ result.output.width }} × {{ result.output.height }} · {{ result.output.format }} · {{ bytes(result.bytes) }}</figcaption><img v-if="!unsupported" :src="result.url" :alt="t('imagePreviewOutput')" decoding="async" @error="unsupported=true"><p v-else>{{ t('imagePreviewUnsupported') }}</p><a :href="result.url" :download="'preview.'+result.output.format">{{ t('imagePreviewDownload') }}</a></figure>
    </div>
    <p v-if="result">{{ t('imagePreviewTime') }}: {{ result.elapsed }} ms</p>
  </section>
</template>
<style scoped>
.image-preview { background: var(--sky-soft); border-color: transparent; }figure{margin:0;min-width:0}figcaption{margin-bottom:.5rem}img{display:block;max-width:100%;max-height:20rem;object-fit:contain}figure a{display:inline-block;margin-top:.5rem}
@media (max-width: 480px) { .bucket-search { flex-wrap: wrap; } .bucket-search .field { flex-basis: 100%; } }
</style>
