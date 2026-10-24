<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { api, byteSize, write } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { report } from '../app/feedback'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'

type Profile = components['schemas']['StorageProfile']
type Field = Exclude<keyof Profile, 'storage_id' | 'revision'>
type Draft = Record<Field, string | number | boolean | null>
type Catalog = components['schemas']['IoPresets']
type Preview = components['schemas']['ProfilePreview']
const props = defineProps<{ storageType: string; name?: string; storageId: string; revision: number; existingType?: string; disabled?: boolean }>()
const draft = defineModel<Draft>({ required: true })
const templates = useQuery({
  queryKey: computed(() => ['storage-io-presets', props.storageType, props.name]),
  queryFn: ({ signal }) => api<Catalog>(props.name && props.existingType === props.storageType
    ? `/api/v1/storages/${encodeURIComponent(props.name)}/profile/presets`
    : `/api/v1/storage-profile-presets?type=${encodeURIComponent(props.storageType)}`, { signal }),
})
const chosen = ref('reliable'), open = ref(false), busy = ref(false)
const trigger = ref<HTMLButtonElement>()
const proposed = ref<Draft | null>(null), verified = ref<Preview | null>(null)
const changes = ref<{ field: Field; before: Draft[Field]; after: Draft[Field] }[]>([])
const matched = computed(() => templates.data.value?.presets.find(preset => Object.entries(preset.values).every(([key, value]) => draft.value[key as Field] === value))?.name || 'custom')
const options = computed(() => (templates.data.value?.presets || []).map(preset => ({ value: preset.name, label: t('ioPreset_' + preset.name) })))
const constraints = computed(() => verified.value?.constraints || templates.data.value?.constraints)
watch(() => props.storageType, () => { open.value = false; chosen.value = 'reliable' })
function display(field: Field, value: Draft[Field]) {
  if (value === null) return t('storeOptionUnused')
  if (typeof value === 'boolean') return t(value ? 'enabled' : 'disabled')
  if (field.endsWith('_percent')) return `${value}%`
  if (field === 'compression_min_savings_bytes') return byteSize(value)
  if (field === 'integrity_bandwidth') return `${value}/s`
  if (field === 'integrity_rps') return `${value} ${t('unitRequestsPerSecond')}`
  const prefix: Partial<Record<Field, string>> = {
    public_cache_validation: 'storeReadValidation_', read_source_validation: 'storeReadValidation_', read_integrity: 'storeReadIntegrity_',
    write_processing: 'storeProcessing_', write_durability: 'storeDurability_', upload_cache_overflow: 'storeOverflow_', background_scheduling: 'storeScheduling_',
  }
  return prefix[field] ? t(prefix[field] + value) : String(value)
}
async function preview() {
  const preset = templates.data.value?.presets.find(preset => preset.name === chosen.value)
  if (!preset) return
  busy.value = true
  try {
    const next = { ...draft.value, ...preset.values } as Draft
    verified.value = props.name && props.existingType === props.storageType
      ? await write<Preview>(`/api/v1/storages/${encodeURIComponent(props.name)}/profile/preview`, { storage_id: props.storageId, revision: props.revision, ...next }) : null
    changes.value = Object.keys(preset.values).map(key => ({ field: key as Field, before: draft.value[key as Field], after: next[key as Field] })).filter(change => change.before !== change.after)
    proposed.value = next
    open.value = true
  } catch (error) { report(error) }
  finally { busy.value = false }
}
function apply() {
  if (proposed.value) draft.value = proposed.value
  open.value = false
}
const publicWindow = computed(() => proposed.value?.public_cache_validation === 'background' ? String(proposed.value.public_cache_max_age) : '')
const sourceWindow = computed(() => proposed.value?.read_source_validation === 'background' ? String(proposed.value.source_validation_max_age) : '')
const acknowledgement = computed(() => verified.value?.effects.write_ack || (props.storageType === 'local' ? 'local_' : 'cache_') + (constraints.value?.single ? String(proposed.value?.write_durability || 'durable') : 'durable'))
const warningCodes = computed(() => verified.value?.effects.warnings || [
  ...(proposed.value?.write_durability === 'volatile' && constraints.value?.write_memory_bytes === 0 ? ['write_memory_disabled'] : []),
  ...(constraints.value?.sqlite_synchronous && constraints.value.sqlite_synchronous.toLowerCase() !== 'full' ? ['sqlite_weak'] : []),
])
</script>

<template>
  <section class="store-form-wide storage-policy" :aria-label="t('storeIoPolicy')">
    <div class="section-heading"><div><h3>{{ t('storeIoPolicy') }}</h3><p class="field-help">{{ t('storeIoPolicyHint') }}</p></div><span class="badge node-state" :data-tone="matched === 'reliable' ? 'mint' : matched === 'throughput' ? 'cream' : matched === 'cached' ? 'sky' : 'gray'">{{ t('ioPreset_' + matched) }}</span></div>
    <div class="policy-picker"><label><span>{{ t('storeIoPreset') }}</span><UiSelect v-model="chosen" :options="options" :label="t('storeIoPreset')" :disabled="disabled || busy || templates.isPending.value" /></label><button ref="trigger" type="button" :disabled="disabled || busy || !templates.data.value" @click="preview">{{ t('storeIoPreview') }}</button></div>
    <p v-if="templates.isError.value" class="field-help danger">{{ t('storeIoLoadFailed') }} <button type="button" @click="templates.refetch()">{{ t('refresh') }}</button></p>
  </section>
  <UiDialog v-model:open="open" :title="t('storeIoPreview')" :description="t('storeIoDraftHint')" :trigger="trigger" medium>
    <div class="policy-effects">
      <p>{{ t('storeIoAck_' + acknowledgement) }}</p>
      <p>{{ publicWindow ? t('storeIoPublicWindow', { duration: publicWindow }) : t('storeIoPublicStrict') }}</p>
      <p>{{ sourceWindow ? t('storeIoSourceWindow', { duration: sourceWindow }) : t('storeIoSourceStrict') }}</p>
      <p>{{ t('profile_read_integrity') }} · {{ t('storeReadIntegrity_' + proposed?.read_integrity) }}</p>
      <p v-if="constraints?.sqlite_synchronous">SQLite · {{ t('storeIoDatabaseMode') }} {{ constraints.sqlite_synchronous.toUpperCase() }}</p>
      <p v-if="constraints?.write_memory_bytes !== null && constraints?.write_memory_bytes !== undefined">{{ t('storeIoMemoryBudget', { value: byteSize(constraints.write_memory_bytes) }) }}</p>
      <p v-for="warning in warningCodes" :key="warning" class="policy-warning">{{ t('storeIoWarning_' + warning) }}</p>
    </div>
    <p v-if="!changes.length" class="field-help">{{ t('storeIoNoChanges') }}</p>
    <dl v-else class="policy-changes"><div v-for="change in changes" :key="change.field"><dt>{{ t('profile_' + change.field) }}</dt><dd><span>{{ display(change.field, change.before) }}</span><span aria-hidden="true">→</span><strong>{{ display(change.field, change.after) }}</strong></dd></div></dl>
    <template #footer><button type="button" @click="open = false">{{ t('cancel') }}</button><button type="button" class="primary" @click="apply">{{ t('storeIoApplyDraft') }}</button></template>
  </UiDialog>
</template>
