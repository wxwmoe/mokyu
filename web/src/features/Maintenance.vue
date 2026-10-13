<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRouter } from 'vue-router'
import { Flower2, Play, Pause, ScanHeart, ShieldCheck, CloudUpload, ArrowUpRight, RefreshCw } from 'lucide-vue-next'
import { api, write, queries, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { secure } from '../app/security'
import { notify, report } from '../app/feedback'
import { useMaintenance, policies, jobIcon } from '../app/maintenance'
import { taskTone } from '../app/insights'
import UiSelect from '../components/ui/UiSelect.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UnpackTools from '../components/UnpackTools.vue'
const router = useRouter(), state = useMaintenance(), busy = ref('')
const ordered = computed(() => policies.map(kind => state.data.value?.controls.find(c => c.kind === kind)).filter(c => !!c))
const tool = ref(''), mode = ref('metadata'), bucket = ref(''), key = ref(''), age = ref('48h')
const toolOpen = computed({ get: () => !!tool.value, set: v => { if (!v) tool.value = '' } })
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/v1/buckets', { signal }) })
const bucketOptions = computed(() => [{ value: '', label: t('allBuckets') }, ...(buckets.data.value || []).map(b => ({ value: b.name, label: b.name }))])
const modes = computed(() => ['metadata', 'head', 'full'].map(value => ({ value, label: t('integrity_' + value), description: t('integrity_' + value + '_hint') })))
async function change(kind: string, action: string) {
  const active = state.data.value?.controls.find(c => c.kind === kind)?.active
  if (action === 'run' && active) { await router.push({ path: '/tasks', query: { task: active.id } }); return }
  busy.value = kind
  try { const result = await write<components['schemas']['MaintenanceResult']>(`/api/v1/maintenance/${kind}/actions`, { action }); await state.refetch(); if (result.task_id) await router.push({ path: '/tasks', query: { task: result.task_id } }); else notify(t(action === 'pause' ? 'policyPausedNotice' : action === 'resume' ? 'policyResumedNotice' : 'maintenanceUpdated')) }
  catch (e) { report(e) } finally { busy.value = '' }
}
async function start() {
  busy.value = tool.value
  try {
    let result: components['schemas']['TaskStarted']
    if (tool.value === 'integrity') result = await write('/api/v1/integrity', { mode: mode.value, bucket: bucket.value || null, key: bucket.value ? key.value || null : null })
    else if (tool.value === 'sweep') result = await write('/api/v1/maintenance/sweep', { older_than: age.value })
    else result = await write('/api/v1/maintenance/flush', {})
    tool.value = ''; await queries.invalidateQueries({ queryKey: ['maintenance'] }); await router.push({ path: '/tasks', query: { task: result.task_id } })
  } catch (e) { report(e) } finally { busy.value = '' }
}
async function toggle(which: 'mode' | 'pack-creation', enabled: boolean) {
  busy.value = which
  try {
    await secure(() => write(`/api/v1/maintenance/${which}`, { enabled }))
    await state.refetch()
    await queries.refetchQueries({ queryKey: [which === 'pack-creation' ? 'service-config' : 'service-status'] })
    const message = which === 'mode' ? (enabled ? 'maintenanceEntered' : 'maintenanceExited') : (enabled ? 'packCreationEnabledNotice' : 'packCreationDisabledNotice')
    notify(t(message))
  }
  catch (e) { report(e) } finally { busy.value = '' }
}
</script>
<template><div class="page-heading"><div><p class="eyebrow">{{ t('maintenanceEyebrow') }}</p><h1>{{ t('maintenance') }} <Flower2 :size="25" class="heading-heart" /></h1><p>{{ t('maintenanceHint') }}</p></div><div class="heading-actions"><RouterLink to="/tasks" class="button">{{ t('tasks') }}<ArrowUpRight :size="16" /></RouterLink><button class="icon-button" :aria-label="t('refresh')" @click="state.refetch()"><RefreshCw :size="17" /></button></div></div>
  <ErrorNotice v-if="state.error.value" :error="state.error.value" /><div v-if="!state.data.value && state.isPending.value" class="skeleton-row" />
  <div v-if="state.data.value?.maintenance" class="info-callout"><ShieldCheck :size="24" /><p>{{ t('maintenanceActiveHint') }}</p><button :disabled="!!busy" @click="toggle('mode', false)">{{ t('leaveMaintenance') }}</button></div>
  <div class="policy-grid"><article v-for="policy in ordered" :key="policy.kind" class="surface policy-card" :class="taskTone(policy.kind)"><header><span class="policy-icon"><component :is="jobIcon(policy.kind)" :size="23" /></span><span class="badge">{{ t(!policy.enabled ? 'disabled' : policy.paused ? 'paused' : 'scheduled') }}</span></header><h2>{{ t('task_' + policy.kind) }}</h2><p>{{ t('policy_' + policy.kind) }}</p><dl><dt>{{ t('scheduleEvery') }}</dt><dd>{{ policy.interval_seconds >= 3600 ? t('hoursShort', { value: policy.interval_seconds / 3600 }) : t('secondsShort', { value: policy.interval_seconds }) }}</dd><dt>{{ t(policy.active ? 'currentTask' : 'nextCheck') }}</dt><dd><RouterLink v-if="policy.active" :to="{ path: '/tasks', query: { task: policy.active.id } }">{{ t(policy.active.status) }} · {{ policy.active.processed }}</RouterLink><span v-else>{{ policy.next_run_at ? date(policy.next_run_at) : t('notScheduled') }}</span></dd></dl><RouterLink v-if="policy.latest && !policy.active" class="policy-last" :to="{ path: '/tasks', query: { task: policy.latest.id } }">{{ t('lastRun') }} · {{ t(policy.latest.status) }}<ArrowUpRight :size="13" /></RouterLink><footer><button :disabled="!!busy || !policy.enabled" @click="change(policy.kind, policy.paused ? 'resume' : 'pause')"><Play v-if="policy.paused" :size="15" /><Pause v-else :size="15" />{{ t(policy.paused ? 'resumePolicy' : 'pausePolicy') }}</button><button :disabled="!!busy || !policy.enabled || policy.paused || (state.data.value?.maintenance && policy.kind !== 'cleanup')" @click="change(policy.kind, 'run')">{{ t(policy.active ? 'viewTask' : 'runNow') }}</button></footer></article></div>
  <div class="section-heading maintenance-section-title"><h2>{{ t('maintenanceTools') }}</h2><span class="field-help">{{ t('toolsHint') }}</span></div><div class="maintenance-tools"><button class="surface tool-card" @click="tool = 'integrity'"><ScanHeart :size="25" /><strong>{{ t('integrityCheck') }}</strong><span>{{ t('integrityToolHint') }}</span></button><button class="surface tool-card" @click="tool = 'flush'"><CloudUpload :size="25" /><strong>{{ t('flushUploadCache') }}</strong><span>{{ t('flushHint') }}</span></button><button class="surface tool-card" @click="tool = 'sweep'"><ShieldCheck :size="25" /><strong>{{ t('backendSweep') }}</strong><span>{{ t('sweepHint') }}</span></button></div>
  <UnpackTools :status="state.data.value" :busy="!!busy" @change="toggle('pack-creation', $event)" />
  <section class="surface recovery-card"><div class="section-heading"><h2><ShieldCheck :size="21" />{{ t('recoveryMode') }}</h2><button :disabled="!!busy" @click="toggle('mode', !state.data.value?.maintenance)">{{ t(state.data.value?.maintenance ? 'leaveMaintenance' : 'enterMaintenance') }}</button></div><p class="field-help">{{ t('recoveryModeHint') }}</p></section>
  <UiDialog v-model:open="toolOpen" :title="t(tool === 'integrity' ? 'integrityCheck' : tool === 'sweep' ? 'backendSweep' : 'flushUploadCache')" :description="t(tool === 'integrity' ? 'integrityToolHint' : tool === 'sweep' ? 'sweepHint' : 'flushHint')" :busy="!!busy"><form @submit.prevent="start"><template v-if="tool === 'integrity'"><label>{{ t('inspectionDepth') }}<UiSelect v-model="mode" :options="modes" :label="t('inspectionDepth')" /></label><label>{{ t('buckets') }}<UiSelect v-model="bucket" :options="bucketOptions" :label="t('buckets')" /></label><label>{{ t('optionalObjectKey') }}<input v-model="key" :disabled="!bucket" maxlength="1024"></label><p class="field-help">{{ t('integrity_' + mode + '_hint') }}</p></template><label v-else-if="tool === 'sweep'">{{ t('sweepAge') }}<UiSelect v-model="age" :options="['24h', '48h', '7d', '30d'].map(value => ({ value, label: value }))" :label="t('sweepAge')" /></label><div class="form-footer"><button type="button" :disabled="!!busy" @click="tool = ''">{{ t('cancel') }}</button><button class="primary" :disabled="!!busy">{{ t(tool === 'sweep' ? 'scanOnly' : 'startTask') }}</button></div></form></UiDialog>
</template>
