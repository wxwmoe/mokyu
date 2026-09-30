<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { UploadCloud, CloudCheck, Pause, Play, X, RefreshCw, ArrowUpRight, CircleCheck, Clock, FileCheck2, ChevronRight } from 'lucide-vue-next'
import { api, ApiError, params, bytes, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { uploads, pause, resume, reselect, cancel, type Transfer, type UploadJob } from '../app/uploads'
import { t, date } from '../app/i18n'
import { report, errorText } from '../app/feedback'
import UploadDialog from '../components/UploadDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import FileIcon from '../components/FileIcon.vue'
import EmptyState from '../components/EmptyState.vue'

const state = ref('active'), bucket = ref('all'), cursor = ref(''), add = ref(false), visible = ref(!document.hidden)
document.addEventListener('visibilitychange', visibility)
function visibility() { visible.value = !document.hidden }
import { onUnmounted } from 'vue'
onUnmounted(() => document.removeEventListener('visibilitychange', visibility))
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const options = computed(() => [{ value: 'all', label: t('allBuckets') }, ...(buckets.data.value || []).map(b => ({ value: b.id, label: b.name }))])
const writable = computed(() => buckets.data.value?.some(b => b.state === 'active' && b.actions.includes('object.write')))
const transfers = useQuery({ queryKey: ['transfers', state, bucket, cursor],
  queryFn: ({ signal }) => api<components['schemas']['TransferPage']>('/api/uploads?' + params({ state: state.value === 'active' ? undefined : state.value, bucket: bucket.value === 'all' ? undefined : bucket.value, after: cursor.value || undefined }), { signal }),
  refetchInterval: computed(() => visible.value ? 5000 : false) })
interface Row { id: string; job?: UploadJob; transfer?: Transfer }
const rows = computed(() => {
  const values = new Map<string, Row>()
  for (const transfer of transfers.data.value?.uploads || []) values.set(transfer.id, { id: transfer.id, transfer })
  for (const job of uploads) {
    if (bucket.value !== 'all' && job.bucket !== bucket.value || cursor.value || state.value === 'active' && ['completed', 'aborted'].includes(job.state) || state.value === 'completed' && job.state !== 'completed') continue
    const id = job.transfer?.id || job.client
    const old = values.get(id)
    values.set(id, { id, job, transfer: old?.transfer || job.transfer })
  }
  return [...values.values()].reverse()
})
watch([state, bucket], () => { cursor.value = '' })
const active = computed(() => uploads.filter(j => ['queued', 'checking', 'uploading', 'completing'].includes(j.state)).length)
const pending = computed(() => rows.value.filter(r => r.transfer?.state === 'completed' && r.transfer.remote_state === 'pending').length)
const picker = ref<HTMLInputElement>(), selected = ref<Transfer>(), removing = ref<Row>(), busy = ref(false)
const confirming = computed({ get: () => !!removing.value, set: v => { if (!v) removing.value = undefined } })
function name(row: Row) { return row.transfer?.object_key || row.job?.input.key || '' }
function status(row: Row) { return row.job?.state || row.transfer?.state || 'active' }
function needsFile(job: UploadJob) { return job.error instanceof ApiError && ['ResumeMismatch', 'FileReadFailed'].includes(job.error.code) }
function progress(row: Row) { const size = Number(row.transfer?.expected_size || row.job?.input.size); const sent = row.job ? row.job.accepted + row.job.sending : Number(row.transfer?.received_bytes); return size > 0 ? Math.min(100, 100 * sent / size) : (status(row) === 'completed' ? 100 : 0) }
function choose(transfer: Transfer) { selected.value = transfer; picker.value?.click() }
function picked(event: Event) { const input = event.target as HTMLInputElement; try { if (input.files?.[0] && selected.value) reselect(selected.value, input.files[0]) } catch (error) { report(error) } finally { input.value = '' } }
async function remove() {
  if (!removing.value) return; busy.value = true
  try { await cancel(removing.value.job, removing.value.transfer); removing.value = undefined; await transfers.refetch() } catch (error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('uploadEyebrow') }}</p><h1>{{ t('transfers') }}</h1><p>{{ t('transferHint') }}</p></div><div class="heading-actions"><button :disabled="transfers.isFetching.value" :aria-label="t('refresh')" @click="transfers.refetch()"><RefreshCw :size="16" /></button><button v-if="writable" class="primary" @click="add = true"><UploadCloud :size="16" />{{ t('uploadTitle') }}</button></div></div>
  <div class="transfer-banner"><div class="transfer-mascot"><img src="/assets/mokyu-mochi.svg" alt="" width="108" height="98"></div><div><h2>{{ t('transferStory') }}</h2><p>{{ t('transferSafe') }}</p></div><div class="transfer-count"><strong>{{ active }}</strong><span>{{ t('transferSending') }}</span></div><div class="transfer-count remote"><strong>{{ pending }}</strong><span>{{ t('transferWaiting') }}</span></div></div>
  <div class="transfer-toolbar"><UiSelect v-model="state" :label="t('transferFilter')" :options="[{ value: 'active', label: t('transferActive') }, { value: 'completed', label: t('transferFinished') }, { value: 'all', label: t('transferAll') }]" /><UiSelect v-model="bucket" :label="t('bucket')" :options="options" /><span>{{ t('transferScope') }}</span></div>
  <ErrorNotice v-if="transfers.error.value" :error="transfers.error.value" />
  <section class="surface transfer-list">
    <div v-if="transfers.isPending.value" class="skeleton-row" />
    <article v-for="row in rows" :key="row.id" class="transfer-row" :data-state="status(row)">
      <FileIcon :name="name(row)" /><div class="transfer-main"><div class="transfer-heading"><strong>{{ name(row).split('/').pop() }}</strong><span class="badge"><CircleCheck v-if="status(row) === 'completed'" :size="12" /><Clock v-else :size="12" />{{ t('transfer_' + status(row)) }}</span></div>
        <div class="transfer-context"><RouterLink :to="'/media/' + (row.transfer?.bucket_id || row.job?.bucket)">{{ row.transfer?.bucket_name || buckets.data.value?.find(b => b.id === row.job?.bucket)?.name }}<ArrowUpRight :size="11" /></RouterLink><span v-if="row.transfer">{{ row.transfer.owner }} · {{ row.transfer.source === 'web' ? t('browserUpload') : 'S3' }}</span></div>
        <template v-if="!['completed', 'aborted'].includes(status(row))"><div class="transfer-progress" role="progressbar" :aria-label="name(row)" :aria-valuenow="Math.round(progress(row))" :aria-valuemin="0" :aria-valuemax="100"><span :style="{ width: progress(row) + '%' }" /></div><div class="transfer-detail"><span>{{ bytes(row.job ? row.job.accepted + row.job.sending : Number(row.transfer?.received_bytes || 0)) }}<template v-if="row.transfer?.expected_size || row.job"> / {{ bytes(Number(row.transfer?.expected_size || row.job?.input.size)) }}</template></span><span v-if="row.job?.state === 'checking'">{{ t('verifiedParts', { count: row.job.verified }) }}</span><span v-else-if="row.job?.speed">~ {{ bytes(row.job.speed) }}/s</span><span v-else-if="row.transfer?.expires_at">{{ t('transferExpires', { date: date(row.transfer.expires_at) }) }}</span></div></template>
        <div v-else-if="status(row) === 'completed'" class="transfer-durability"><CloudCheck v-if="row.transfer?.remote_state === 'stored'" :size="14" /><Clock v-else :size="14" /><span>{{ t('remote_' + (row.transfer?.remote_state || 'pending')) }}</span><span>{{ bytes(Number(row.transfer?.received_bytes || row.job?.input.size || 0)) }}</span></div>
        <p v-if="row.job?.error" class="transfer-error" role="alert">{{ errorText(row.job.error) }}</p>
      </div><div class="transfer-actions">
        <button v-if="row.job && ['queued', 'checking', 'uploading'].includes(row.job.state)" class="icon-button" :disabled="row.job.paused" :aria-label="t(row.job.paused ? 'pausingUpload' : 'pauseUpload')" @click="pause(row.job)"><Pause :size="17" /></button>
        <button v-else-if="row.job?.file && ['paused', 'failed'].includes(row.job.state) && !needsFile(row.job)" class="icon-button" :disabled="row.job.running" :aria-label="t('resumeUpload')" @click="resume(row.job)"><Play :size="17" /></button>
        <button v-if="row.transfer?.can_resume && !row.job?.running && (!row.job?.file || row.job?.state === 'failed')" :aria-label="t('reselectFile')" @click="choose(row.transfer)"><FileCheck2 :size="16" /><span>{{ t('reselectFile') }}</span></button>
        <button v-if="!['completed', 'completing', 'aborted'].includes(status(row))" class="icon-button" :aria-label="t('cancelUpload')" @click="removing = row"><X :size="17" /></button>
      </div>
    </article>
    <EmptyState v-if="!transfers.isPending.value && !transfers.error.value && !rows.length" :title="t('transfersEmpty')" :description="t('transfersEmptyHint')" />
    <div v-if="transfers.data.value?.next || cursor" class="table-footer"><button v-if="cursor" @click="cursor = ''">{{ t('transferNewest') }}</button><button v-if="transfers.data.value?.next" @click="cursor = transfers.data.value.next">{{ t('nextPage') }}<ChevronRight :size="16" /></button></div>
  </section>
  <p class="field-help transfer-help">{{ t('resumeHint') }}</p>
  <input ref="picker" type="file" hidden :aria-label="t('reselectFile')" @change="picked">
  <UploadDialog v-model:open="add" :buckets="buckets.data.value || []" />
  <UiDialog v-model:open="confirming" :title="t('cancelUpload')" :description="t('cancelUploadHint')" :busy="busy"><p class="object-path">{{ removing && name(removing) }}</p><template #footer><button :disabled="busy" @click="confirming = false">{{ t('keepUpload') }}</button><button class="danger-button" :disabled="busy" @click="remove">{{ t('cancelUpload') }}</button></template></UiDialog>
</template>
