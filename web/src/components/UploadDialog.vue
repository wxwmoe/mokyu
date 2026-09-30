<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { UploadCloud, FolderHeart, X, ArrowRight } from 'lucide-vue-next'
import { bytes, type Bucket } from '../api/client'
import { enqueue } from '../app/uploads'
import { t } from '../app/i18n'
import UiDialog from './ui/UiDialog.vue'
import UiSelect from './ui/UiSelect.vue'
import UiCheckbox from './ui/UiCheckbox.vue'
import FileIcon from './FileIcon.vue'

const props = defineProps<{ buckets: Bucket[]; bucket?: string; prefix?: string }>()
const open = defineModel<boolean>('open', { required: true })
const router = useRouter(), picker = ref<HTMLInputElement>(), files = ref<File[]>([])
const target = ref(''), prefix = ref(''), publicRead = ref(false), overwrite = ref(false), dragging = ref(false)
const options = computed(() => props.buckets.filter(b => b.state === 'active' && b.actions.includes('object.write')).map(b => ({ value: b.id, label: b.name })))
const current = computed(() => props.buckets.find(b => b.id === target.value))
watch(open, value => { if (value) { files.value = []; target.value = props.bucket || options.value[0]?.value || ''; prefix.value = props.prefix || ''; publicRead.value = false; overwrite.value = false } })
watch(target, () => { if (!current.value?.actions.includes('object.acl')) publicRead.value = false })
function select(list?: FileList | null) {
  if (!list) return
  for (const file of list) {
    const existing = files.value.findIndex(item => item.name === file.name)
    if (existing >= 0) files.value.splice(existing, 1, file); else files.value.push(file)
  }
  if (picker.value) picker.value.value = ''
}
function drop(event: DragEvent) { dragging.value = false; select(event.dataTransfer?.files) }
function start() { enqueue(files.value, target.value, prefix.value, publicRead.value, overwrite.value); open.value = false; router.push('/transfers') }
</script>
<template>
  <UiDialog v-model:open="open" :title="t('uploadTitle')" :description="t('uploadHint')" wide>
    <div class="upload-layout"><div>
      <input ref="picker" type="file" multiple hidden :aria-label="t('chooseFiles')" @change="select(($event.target as HTMLInputElement).files)">
      <button class="upload-dropzone" :class="{ dragging }" @click="picker?.click()" @dragover.prevent="dragging = true" @dragleave="dragging = false" @drop.prevent="drop"><span class="upload-cloud"><UploadCloud :size="30" /></span><strong>{{ t('dropFiles') }}</strong><span>{{ t('chooseFiles') }}</span></button>
      <div class="upload-picks"><div v-for="(file, index) in files" :key="file.name" class="upload-pick"><FileIcon :name="file.name" /><div><strong>{{ file.name }}</strong><small>{{ bytes(file.size) }}</small></div><button class="icon-button" :aria-label="t('removeFile', { name: file.name })" @click="files.splice(index, 1)"><X :size="15" /></button></div></div>
    </div><div class="upload-options"><span class="project-symbol"><FolderHeart :size="25" /></span><h3>{{ t('uploadDestination') }}</h3>
      <label class="field"><span>{{ t('bucket') }}</span><UiSelect v-model="target" :label="t('bucket')" :options="options" /></label>
      <label class="field"><span>{{ t('uploadPrefix') }}</span><input v-model="prefix" :placeholder="t('uploadPrefixPlaceholder')" autocomplete="off"></label>
      <p class="field-help">{{ t('uploadPathHint') }}</p>
      <UiCheckbox v-if="current?.actions.includes('object.acl')" v-model="publicRead" :label="t('uploadPublic')" />
      <UiCheckbox v-model="overwrite" :label="t('uploadOverwrite')" /><p class="field-help">{{ t('uploadOverwriteHint') }}</p>
    </div></div>
    <template #footer><span class="upload-total">{{ t('uploadSelected', { count: files.length, size: bytes(files.reduce((n, f) => n + f.size, 0)) }) }}</span><button class="primary" :disabled="!files.length || !target" @click="start">{{ t('startUpload') }}<ArrowRight :size="16" /></button></template>
  </UiDialog>
</template>
