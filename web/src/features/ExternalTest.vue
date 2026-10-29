<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Download, Pause, Play, RefreshCw } from 'lucide-vue-next'
import { api, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { report } from '../app/feedback'
import { secure } from '../app/security'
import ErrorNotice from '../components/ErrorNotice.vue'
import UiDialog from '../components/ui/UiDialog.vue'

const props = defineProps<{ bucket: components['schemas']['ExternalBucket'] }>()
type Result = components['schemas']['ExternalTest']
const probe = useQuery({ queryKey: ['external-test', () => props.bucket.id], queryFn: ({ signal }) => api<Result | null>(`/api/v1/external-buckets/${props.bucket.id}/test`, { signal }), refetchInterval: q => ['queued', 'running'].includes(q.state.data?.state || '') ? 2000 : false })
const busy = ref(false), error = ref<unknown>(null), stopping = ref(false), confirmation = ref('')
const current = computed(() => probe.data.value)
const active = computed(() => ['queued', 'running'].includes(current.value?.state || ''))
const canStart = computed(() => props.bucket.enabled && probe.isSuccess.value && (!current.value || current.value.stopped || current.value.report.phase === 'done'))
const canStop = computed(() => current.value && !current.value.stopped && current.value.report.phase !== 'done' && ['queued', 'paused', 'failed'].includes(current.value.state))
const namespace = computed(() => current.value ? [current.value.report.connection.prefix, '.mokyu-tests', current.value.id].filter(Boolean).join('/') + '/' : '')
async function change(action: 'start' | 'pause' | 'resume' | 'stop') {
  const id = props.bucket.id, revision = props.bucket.revision, task = current.value?.id
  busy.value = true; error.value = null
  try {
    if (action === 'start') await secure(() => write(`/api/v1/external-buckets/${id}/test`, { revision }))
    else if (action === 'stop') await secure(() => write(`/api/v1/external-buckets/${id}/test/stop`, { test_id: task }))
    else await secure(() => write(`/api/v1/tasks/${task}/actions`, { action }))
    stopping.value = false
    await queries.invalidateQueries({ queryKey: ['external-test', id] })
    await queries.invalidateQueries({ queryKey: ['jobs'] })
  } catch (value) { error.value = value; report(value) } finally { busy.value = false }
}
</script>

<template>
  <section class="detail-section">
    <div class="section-heading"><h3>{{ t('externalTest') }}</h3><button :disabled="busy || !canStart" @click="change('start')"><Play :size="15" />{{ t('externalTestStart') }}</button></div>
    <p class="field-help">{{ t('externalTestHint') }}</p>
    <ErrorNotice :error="error || probe.error.value" />
    <button v-if="probe.isError.value" :disabled="probe.isFetching.value" @click="probe.refetch()"><RefreshCw :size="15" />{{ t('externalRetry') }}</button>
    <p v-if="probe.isPending.value" role="status">{{ t('loading') }}</p>
    <template v-if="current">
      <dl class="key-facts"><dt>{{ t('externalCheckState') }}</dt><dd><span class="badge" :class="{ 'error-badge': current.state === 'failed' }">{{ t(current.stopped ? 'externalTestStopped' : current.state) }}</span></dd><dt>{{ t('externalTestPhase') }}</dt><dd>{{ t('externalPhase_' + current.report.phase) }}</dd><dt>{{ t('externalTestNode') }}</dt><dd class="object-path">{{ current.node_id || t(current.attempts ? 'externalTestLocal' : 'externalTestAwaitingNode') }}</dd><dt>{{ t('updated') }}</dt><dd>{{ date(current.updated_at) }}</dd></dl>
      <p v-if="current.revision !== bucket.revision" class="info-callout">{{ t('externalTestOutdated') }}</p>
      <p v-if="current.report.blocked" class="info-callout">{{ t('externalProblem_' + current.report.blocked) }}</p>
      <div class="actions"><button v-if="active" :disabled="busy" @click="change('pause')"><Pause :size="15" />{{ t('pauseTask') }}</button><button v-if="!current.stopped && ['failed', 'paused'].includes(current.state)" :disabled="busy" @click="change('resume')"><Play :size="15" />{{ t('resumeTask') }}</button><a class="button" :href="`/api/v1/external-tests/${current.id}`" download><Download :size="15" />{{ t('downloadReport') }}</a><button v-if="canStop" class="danger-button" :disabled="busy" @click="confirmation = ''; stopping = true">{{ t('externalTestStop') }}</button></div>
      <div class="table-scroll"><table><thead><tr><th scope="col">{{ t('externalCapability') }}</th><th scope="col">{{ t('externalCheckState') }}</th></tr></thead><tbody><tr v-for="(check, capability) in current.report.checks" :key="capability"><td>{{ t('externalCapability_' + capability) }}</td><td>{{ t('externalCheck_' + check.state) }}<small v-if="check.status"> · {{ check.status }}</small></td></tr></tbody></table></div>
      <details v-if="current.cleanup_required" class="task-technical"><summary>{{ t('externalTestArtifacts') }}</summary><p class="field-help">{{ t('externalTestArtifactsHint') }}</p><code class="object-path">{{ current.report.connection.endpoint_url }} · {{ current.report.connection.bucket }} / {{ namespace }}</code><div v-for="(item, index) in current.report.artifacts.filter(value => !value.cleaned)" :key="index"><code class="object-path">{{ namespace + item.file }}</code><dl class="key-facts"><template v-if="item.version"><dt>Version ID</dt><dd class="object-path">{{ item.version }}</dd></template><template v-if="item.upload"><dt>Upload ID</dt><dd class="object-path">{{ item.upload }}</dd></template><template v-if="item.etag"><dt>ETag</dt><dd class="object-path">{{ item.etag }}</dd></template></dl></div></details>
    </template>
  </section>
  <UiDialog v-model:open="stopping" :title="t('externalTestStop')" :description="t('externalTestStopHint')" :busy="busy"><form @submit.prevent="change('stop')"><ErrorNotice :error="error" /><label>{{ t('confirmIdentity', { name: bucket.name }) }}<input v-model="confirmation" autocomplete="off" :disabled="busy"></label><div class="form-footer"><button type="button" :disabled="busy" @click="stopping = false">{{ t('cancel') }}</button><button class="danger-button" :disabled="busy || confirmation !== bucket.name">{{ t('externalTestStop') }}</button></div></form></UiDialog>
</template>
