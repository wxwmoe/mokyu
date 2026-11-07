<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { BookOpen, Settings2, Upload } from 'lucide-vue-next'
import { api, write } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { notify, report } from '../app/feedback'
import ErrorNotice from './ErrorNotice.vue'
import UiSelect from './ui/UiSelect.vue'
import UiDialog from './ui/UiDialog.vue'
import StorageSnapshots from './StorageSnapshots.vue'

type Status = components['schemas']['MetadataJournalStatus']
type Policy = Required<components['schemas']['MetadataJournalPolicy']>
type Quantity = Exclude<keyof Policy, 'mode'>
const props = defineProps<{ name: string }>()
const path = computed(() => `/api/v1/storages/${encodeURIComponent(props.name)}/journal`)
const status = useQuery({ queryKey: computed(() => ['storage-journal', props.name]), queryFn: ({ signal }) => api<Status>(path.value, { signal }) })
const open = ref(false), busy = ref(false), draft = ref<Policy>(), revision = ref('')
watch(() => props.name, () => { open.value = false })
function edit() {
  const value = status.data.value
  if (!value) return
  draft.value = { ...value.policy } as Policy
  revision.value = value.revision
  open.value = true
}
const modes = computed(() => [{ value: 'put', label: t('journalPut') }, { value: 'multipart', label: t('journalMultipart') }])
const fields = computed(() => {
  const result: { key: Quantity; label: string; unit: number; optional?: boolean }[] = []
  if (draft.value?.mode === 'put') {
    result.push({ key: 'put_target_bytes', label: t('journalPutTarget'), unit: 1024 }, { key: 'put_max_wait_seconds', label: t('journalWait'), unit: 1, optional: true })
  } else {
    result.push({ key: 'part_target_bytes', label: t('journalPartTarget'), unit: 1048576 }, { key: 'part_max_wait_seconds', label: t('journalWait'), unit: 1, optional: true }, { key: 'multipart_max_open_seconds', label: t('journalOpenFor'), unit: 1, optional: true })
  }
  return [...result, { key: 'segment_bytes' as Quantity, label: t('journalSegmentLimit'), unit: 1048576 },
    { key: 'snapshot_page_bytes' as Quantity, label: t('journalSnapshotPage'), unit: 1048576 },
    { key: 'snapshot_trigger_bytes' as Quantity, label: t('journalSnapshotTrigger'), unit: 1048576 }]
})
function change(key: Quantity, unit: number, event: Event) {
  const input = event.target as HTMLInputElement
  if (draft.value) Object.assign(draft.value, { [key]: input.value === '' ? null : input.valueAsNumber * unit })
}
async function save() {
  if (!draft.value || busy.value) return
  busy.value = true
  try {
    await write<Status>(path.value, { revision: revision.value, policy: draft.value }, 'PUT')
    open.value = false
    await status.refetch()
    notify(t('journalSaved'))
  } catch (error) { report(error) } finally { busy.value = false }
}
async function flush() {
  if (busy.value) return
  busy.value = true
  try { await write<Status>(path.value + '/flush', {}); await status.refetch(); notify(t('journalFlushQueued')) }
  catch (error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <div class="storage-detail-grid journal-panels">
    <section class="surface">
      <header class="section-heading"><h2><BookOpen :size="19" aria-hidden="true" />{{ t('metadataJournal') }}</h2></header>
      <ErrorNotice :error="status.error.value" />
      <div v-if="status.isPending.value" class="skeleton-row" />
      <dl v-if="status.data.value" class="facts">
        <dt>{{ t('journalWriteMode') }}</dt><dd>{{ t(status.data.value.policy.mode === 'multipart' ? 'journalMultipart' : 'journalPut') }}</dd>
        <dt>{{ t('journalSequence') }}</dt><dd>{{ status.data.value.emitted_sequence }}</dd>
      </dl>
      <p v-if="status.data.value?.pending_policy" class="field-help" role="status">{{ t('journalPolicyPending') }}</p>
      <p v-if="status.data.value?.error" class="error" role="alert">{{ status.data.value.error }}</p>
      <div class="actions">
        <button v-if="status.data.value" type="button" :disabled="busy" @click="flush"><Upload :size="16" />{{ t('journalFlush') }}</button>
        <button type="button" :disabled="!status.data.value || busy" @click="edit"><Settings2 :size="16" />{{ t('journalSettings') }}</button>
      </div>
    </section>
    <StorageSnapshots :name="name" :enabled="!!status.data.value && Number(status.data.value.emitted_sequence) > 0" />
  </div>
  <UiDialog v-model:open="open" medium :busy="busy" :title="t('journalSettings')">
    <form v-if="draft" class="form-stack" @submit.prevent="save">
      <label class="field"><span>{{ t('journalWriteMode') }}</span><UiSelect :model-value="draft.mode" :options="modes" :label="t('journalWriteMode')" @update:model-value="draft.mode = $event === 'multipart' ? 'multipart' : 'put'" /></label>
      <div class="form-pair">
        <label v-for="field in fields" :key="field.key" class="field"><span>{{ field.label }}</span>
          <input type="number" :step="1 / field.unit" :min="1 / field.unit" :required="!field.optional"
            :placeholder="field.optional ? t('journalUnlimited') : undefined" :value="draft[field.key] == null ? '' : Number(draft[field.key]) / field.unit"
            @input="change(field.key, field.unit, $event)">
        </label>
      </div>
      <div class="form-footer"><button type="button" :disabled="busy" @click="open = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy">{{ t('save') }}</button></div>
    </form>
  </UiDialog>
</template>
