<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Pause, Play, ArrowRightLeft, Trash2, RefreshCw } from 'lucide-vue-next'
import { api, write, queries, bytes, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { secure } from '../app/security'
import { t } from '../app/i18n'
import { notify, report, errorText } from '../app/feedback'
import UiDialog from './ui/UiDialog.vue'
import UiSelect from './ui/UiSelect.vue'

const props = defineProps<{ bucket: Bucket; dirty: boolean }>()
const emit = defineEmits<{ changed: []; removed: [] }>()
type Impact = components['schemas']['BucketImpact']
type Transfer = components['schemas']['TransferPreview']
const open = ref(false), busy = ref(false), operation = ref<'transfer' | 'purge' | 'delete'>('transfer')
const target = ref(''), name = ref(''), error = ref<unknown>(), preview = ref<components['schemas']['BucketPreview'] | Transfer>()
const projects = useQuery({ queryKey: ['projects'], queryFn: ({ signal }) => api<components['schemas']['Project'][]>('/api/projects', { signal }) })
const choices = computed(() => projects.data.value?.filter(p => p.id !== props.bucket.project_id).map(p => ({ value: p.id, label: p.name })) || [])
const transfer = computed(() => preview.value && 'target' in preview.value ? preview.value : undefined)
const impact = computed<Impact | undefined>(() => preview.value?.bucket)
const drained = computed(() => impact.value?.uploads_paused && ['reserved_bytes', 'inflight_bytes', 'active_uploads', 'writing_streams'].every(k => impact.value?.[k as keyof Impact] === '0'))
const path = computed(() => `/api/buckets/${props.bucket.id}`)
watch(target, () => { preview.value = undefined })
async function refresh() {
  busy.value = true; error.value = undefined; preview.value = undefined
  try { preview.value = operation.value === 'transfer' ? await write<Transfer>(path.value + '/transfer/preview', { target_project: target.value }) : await api<components['schemas']['BucketPreview']>(path.value + '/purge/preview', { method: 'POST' }) }
  catch (failure) { error.value = failure } finally { busy.value = false }
}
function begin(kind: typeof operation.value) { operation.value = kind; name.value = ''; preview.value = undefined; error.value = undefined; target.value = choices.value[0]?.value || ''; open.value = true; if (kind !== 'transfer' || target.value) refresh() }
async function pause() {
  busy.value = true
  try {
    const value = await api<components['schemas']['BucketSettings']>(path.value + '/settings')
    const b = value.bucket
    await write(path.value + '/settings', { revision: b.revision, cors: b.cors, website_enabled: b.website_enabled, index_document: b.index_document, error_document: b.error_document, public_base_url: b.public_base_url, uploads_paused: !b.uploads_paused }, 'PUT')
    emit('changed'); await queries.invalidateQueries({ queryKey: ['buckets'] }); notify(t(b.uploads_paused ? 'uploadsResumed' : 'uploadsPaused'))
    if (open.value) await refresh()
  } catch (failure) { report(failure) } finally { busy.value = false }
}
async function execute() {
  if (!preview.value) return
  busy.value = true; error.value = undefined
  try {
    const input = { confirm_name: name.value, confirmation: preview.value.confirmation }
    if (operation.value === 'transfer') { await secure(() => write(path.value + '/transfer', { ...input, target_project: target.value })); emit('changed'); notify(t('bucketTransferred')) }
    else if (operation.value === 'purge') { const task = await secure(() => write<components['schemas']['BucketTask']>(path.value + '/purge', input)); notify(t('bucketPurgeStarted'), task.task_id); emit('removed') }
    else { await secure(() => write(path.value, input, 'DELETE')); notify(t('bucketDeleted')); emit('removed') }
    open.value = false; await queries.invalidateQueries({ queryKey: ['buckets'] }); await queries.invalidateQueries({ queryKey: ['quota'] })
  } catch (failure) { error.value = failure; report(failure) } finally { busy.value = false }
}
</script>
<template>
  <section class="surface settings-card lifecycle-card"><h2><ArrowRightLeft :size="20" />{{ t('bucketLifecycle') }}</h2><p class="field-help">{{ t('bucketLifecycleHint') }}</p>
    <p v-if="dirty" class="info-callout">{{ t('saveBeforeLifecycle') }}</p>
    <div class="lifecycle-row"><div><strong>{{ t(bucket.uploads_paused ? 'uploadsPaused' : 'newUploads') }}</strong><p>{{ t('pauseUploadsHint') }}</p></div><button :disabled="busy || dirty || bucket.state !== 'active'" @click="pause"><Play v-if="bucket.uploads_paused" :size="16" /><Pause v-else :size="16" />{{ t(bucket.uploads_paused ? 'resumeUploads' : 'pauseUploads') }}</button></div>
    <div class="lifecycle-row"><div><strong>{{ t('transferBucket') }}</strong><p>{{ t('transferBucketHint') }}</p></div><button :disabled="busy || dirty || !choices.length || bucket.state !== 'active'" @click="begin('transfer')"><ArrowRightLeft :size="16" />{{ t('transferBucket') }}</button></div>
    <div class="lifecycle-row lifecycle-danger"><div><strong>{{ t('removeBucket') }}</strong><p>{{ t('removeBucketHint') }}</p></div><div class="actions"><button :disabled="busy || dirty" @click="begin('delete')">{{ t('deleteEmptyBucket') }}</button><button class="danger-button" :disabled="busy || dirty" @click="begin('purge')"><Trash2 :size="16" />{{ t('purgeBucket') }}</button></div></div>
  </section>
  <UiDialog v-model:open="open" :title="t(operation === 'transfer' ? 'transferBucket' : operation === 'purge' ? 'purgeBucket' : 'deleteEmptyBucket')" :busy="busy">
    <div v-if="operation === 'transfer'"><label class="field"><span>{{ t('destinationProject') }}</span><UiSelect v-model="target" :label="t('destinationProject')" :options="choices" :disabled="busy" /></label><button :disabled="busy || !target" @click="refresh"><RefreshCw :size="15" />{{ t('reviewTransfer') }}</button><p class="field-help">{{ t('transferDrainHint') }}</p></div>
    <p v-if="error" class="error" role="alert">{{ errorText(error) }} <button :disabled="busy" @click="refresh">{{ t('refresh') }}</button></p>
    <div v-if="impact" class="bucket-impact"><dl class="facts"><dt>{{ t('bucket') }}</dt><dd>{{ impact.name }}</dd><dt>{{ t('objects') }}</dt><dd>{{ impact.objects }}</dd><dt>{{ t('size') }}</dt><dd>{{ bytes(Number(impact.logical_bytes)) }}</dd><dt>{{ t('activeUploads') }}</dt><dd>{{ impact.active_uploads }}</dd><dt>{{ t('writingStreams') }}</dt><dd>{{ impact.writing_streams }}</dd></dl>
      <template v-if="transfer"><p class="info-callout">{{ t('transferAccessHint', { members: impact.member_grants, keys: impact.service_grants, tokens: impact.token_grants }) }}</p><p class="field-help">{{ t('transferTargetHint', { name: transfer.target.name, members: transfer.target.members, used: bytes(Number(transfer.target.used_bytes)), limit: transfer.target.byte_limit == null ? t('quotaUnlimited') : bytes(Number(transfer.target.byte_limit)) }) }}</p><div v-if="!drained" class="info-callout"><p>{{ t(impact.uploads_paused ? 'waitingForDrain' : 'pauseToTransfer') }}</p><button v-if="!impact.uploads_paused" :disabled="busy" @click="pause">{{ t('pauseUploads') }}</button><button v-else :disabled="busy" @click="refresh">{{ t('refresh') }}</button></div></template>
      <p v-else class="info-callout" :class="{ 'danger-callout': operation === 'purge' }">{{ t(operation === 'purge' ? 'purgeBucketWarning' : 'deleteEmptyHint') }}</p>
      <label class="field"><span>{{ t('typeBucketName', { name: bucket.name }) }}</span><input v-model="name" autocomplete="off" :disabled="busy"></label>
    </div>
    <template #footer><button :disabled="busy" @click="open = false">{{ t('cancel') }}</button><button :class="operation === 'transfer' ? 'primary' : 'danger-button'" :disabled="busy || !preview || name !== bucket.name || (operation === 'transfer' && !drained)" @click="execute">{{ t(busy ? 'saving' : operation === 'transfer' ? 'confirmTransfer' : operation === 'purge' ? 'confirmPurge' : 'deleteEmptyBucket') }}</button></template>
  </UiDialog>
</template>
