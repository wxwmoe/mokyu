<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Layers3 } from 'lucide-vue-next'
import { api, bytes, write } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { notify, report } from '../app/feedback'
import ErrorNotice from './ErrorNotice.vue'

type Status = components['schemas']['MetadataSnapshotStatus']
type Action = components['schemas']['MetadataSnapshotAction']
const props = defineProps<{ name: string; enabled: boolean }>()
const path = computed(() => `/api/v1/storages/${encodeURIComponent(props.name)}/snapshots`)
const status = useQuery({queryKey:computed(() => ['metadata-snapshots',props.name]),queryFn:({signal})=>api<Status>(path.value,{signal}),refetchInterval:q=>q.state.data?.task ? 3000 : false})
const busy = ref(false)
const operationLabel = computed(() => t(actions.find(a => a.value === current.value?.operation)?.label || 'snapshotAppend'))
const actions: {value: Action; label: string}[] = [{value:'append',label:'snapshotAppend'},{value:'compact',label:'snapshotCompact'},{value:'new',label:'snapshotNew'}]
const current = computed(()=>status.data.value)
async function run(action: Action) {
  busy.value = true
  try { await write<Status>(path.value, {action}); await status.refetch(); notify(t('snapshotRequested')) }
  catch(error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <section class="surface">
    <header class="section-heading"><h2><Layers3 :size="19" aria-hidden="true" />{{ t('metadataSnapshots') }}</h2></header>
    <ErrorNotice :error="status.error.value" />
    <div v-if="status.isPending.value" class="skeleton-row" />
    <template v-if="current">
      <dl class="facts">
        <dt>{{ t('snapshotRevision') }}</dt><dd>{{ current.current ? current.revision : t('snapshotNone') }}</dd>
        <dt>{{ t('snapshotChanges') }}</dt><dd>{{ bytes(Number(current.pending_log_bytes)) }}</dd>
        <template v-if="current.current"><dt>{{ t('snapshotPages') }}</dt><dd>{{ current.pages }}</dd></template>
        <template v-if="Number(current.invalid_bytes)"><dt>{{ t('snapshotWaste') }}</dt><dd>{{ bytes(Number(current.invalid_bytes)) }}</dd></template>
        <template v-if="Number(current.delete_log_bytes)"><dt>{{ t('snapshotDeletes') }}</dt><dd>{{ bytes(Number(current.delete_log_bytes)) }}</dd></template>
        <template v-if="Number(current.gc_bytes)"><dt>{{ t('snapshotPendingGc') }}</dt><dd>{{ bytes(Number(current.gc_bytes)) }}</dd></template>
      </dl>
      <p v-if="current.error" class="error" role="alert">{{ current.error }}</p>
      <p v-else-if="current.task" class="badge node-state" data-tone="lilac" role="status">{{ t('snapshotWorking',{operation:operationLabel}) }}</p>
      <p v-if="Number(current.delete_log_bytes) >= 1073741824" class="field-help">{{ t('snapshotRebuildHint') }}</p>
      <div class="actions"><button v-for="a in actions" :key="a.value" type="button" :disabled="busy || !enabled || (!!current.task && current.task_state !== 'failed') || (a.value === 'compact' && !current.current)" @click="run(a.value)">{{ t(a.label) }}</button></div>
    </template>
  </section>
</template>
