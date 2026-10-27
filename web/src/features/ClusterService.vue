<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { Network, HeartPulse, Settings2, ArrowUpRight, Copy, RefreshCw, Cloud } from 'lucide-vue-next'
import { api, bytes, params } from '../api/client'
import type { components } from '../api/schema'
import { useDeployment, nodeTone } from '../app/cluster'
import { t, date } from '../app/i18n'
import { copy } from '../app/feedback'
import UiSelect from '../components/ui/UiSelect.vue'
import UiListFooter from '../components/ui/UiListFooter.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import ErrorNotice from '../components/ErrorNotice.vue'
import EmptyState from '../components/EmptyState.vue'
import RuntimeCoverage from '../components/RuntimeCoverage.vue'
import TrendChart from '../components/TrendChart.vue'
import { requestHistory, type Runtime } from '../app/insights'

const { status, overview } = useDeployment(10_000)
const route = useRoute(), router = useRouter(), after = ref(''), role = ref(''), contact = ref(''), work = ref(''), name = ref('')
const configuration = computed(() => typeof route.query.configuration === 'string' ? route.query.configuration : '')
const section = computed(() => typeof route.query.section === 'string' ? route.query.section : '')
const query = computed(() => params({ name: name.value || undefined, work: work.value || undefined, role: role.value || undefined, contact: contact.value || undefined, configuration: configuration.value || undefined, section: section.value || undefined, after: after.value || undefined, limit: '50' }))
const nodes = useQuery({ queryKey: ['cluster-nodes', query], queryFn: ({ signal }) => api<components['schemas']['NodePage']>('/api/v1/cluster/nodes?' + query.value, { signal }), refetchInterval: 10_000 })
watch([role, contact, work, name, configuration, section], () => { after.value = '' })
const selected = computed(() => typeof route.query.node === 'string' ? route.query.node : '')
const open = computed({ get: () => !!selected.value, set: value => { if (!value) void router.replace({ query: { ...route.query, node: undefined } }) } })
const detail = useQuery({ queryKey: ['cluster-node-status', selected], enabled: computed(() => !!selected.value), queryFn: ({ signal }) => api<components['schemas']['NodeStatus']>(`/api/v1/cluster/nodes/${encodeURIComponent(selected.value)}/status`, { signal }), refetchInterval: 10_000 })
const runtime = useQuery({ queryKey: ['runtime-insights'], queryFn: ({ signal }) => api<Runtime>('/api/v1/usage/runtime', { signal }), refetchInterval: 15_000 })
const history = useQuery({ queryKey: ['cluster-node-runtime', selected], enabled: computed(() => !!selected.value), queryFn: ({ signal }) => api<components['schemas']['NodeRuntime']>(`/api/v1/cluster/nodes/${encodeURIComponent(selected.value)}/runtime`, { signal }), refetchInterval: () => selected.value ? 30_000 : false })
const nodeRequests = computed(() => (history.data.value?.history || []).map((p, i, all) => { const before = all[i - 1], seconds = before ? p.uptime_seconds - before.uptime_seconds : 0; return { at: p.received_at, epoch: p.boot_id, value: before?.boot_id === p.boot_id && seconds > 0 && seconds <= p.interval_seconds * 3 && Number(p.point.requests) >= Number(before.point.requests) ? (Number(p.point.requests) - Number(before.point.requests)) / seconds : null } }))
type Snapshot = { work?: string; runtime?: { started_at: string; uptime_seconds: number; http: Record<string, { active: number; started: number; failed: number }> }; process_memory?: { rss_bytes: number | null }; db_pool_idle?: number; db_pool_size?: number; cache?: Record<string, number>; response_cache?: Record<string, unknown>; compression?: Record<string, unknown>; read_replica?: Record<string, unknown> | null; io?: { backend_queues: Record<string, { running: number; limit: number; queued: Record<string, number>; oldest_wait_seconds: number }> } }
const snapshot = computed(() => detail.data.value?.status as Snapshot | null)
const connection = computed(() => detail.data.value?.connection)
const connectionPhase = computed(() => ['connecting', 'ready', 'recovering', 'draining', 'fenced', 'stopped'].includes(connection.value?.phase || '') ? t('nodePhase_' + connection.value!.phase) : t('nodeState_unknown'))
const milliseconds = (value: number | null | undefined) => value == null ? '—' : `${value} ms`
const counts = computed(() => overview.data.value?.nodes || [])
const total = computed(() => counts.value.filter(row => row.contact !== 'released').reduce((sum, row) => sum + Number(row.count), 0))
const online = computed(() => counts.value.filter(row => row.contact === 'online').reduce((sum, row) => sum + Number(row.count), 0))
const pending = computed(() => counts.value.filter(row => row.contact !== 'released' && row.configuration === 'restart_required').reduce((sum, row) => sum + Number(row.count), 0))
const unknown = computed(() => counts.value.filter(row => row.contact !== 'released' && row.configuration === 'unknown').reduce((sum, row) => sum + Number(row.count), 0))
const roles = computed(() => ['control', 'data', 'worker', 'node'].map(role => ({ role, count: counts.value.filter(row => row.role === role && row.contact !== 'released').reduce((sum, row) => sum + Number(row.count), 0) })))
const roleOptions = computed(() => [{ value: '', label: t('clusterAllRoles') }, ...roles.value.map(row => ({ value: row.role, label: t('nodeRole_' + row.role) }))])
const contactOptions = computed(() => [{ value: '', label: t('clusterAllContacts') }, ...['online', 'unresponsive', 'not_reported', 'released'].map(value => ({ value, label: t('nodeState_' + value) }))])
const workOptions = computed(() => [{ value: '', label: t('clusterAllWork') }, ...['normal', 'starting', 'maintenance', 'restricted', 'recovery', 'unknown'].map(value => ({ value, label: t('nodeState_' + value) }))])
const available = computed(() => counts.value.filter(row => row.contact === 'online' && row.work === 'normal').reduce((sum, row) => sum + Number(row.count), 0))
const queues = computed(() => Object.entries(snapshot.value?.io?.backend_queues || {}))
function diagnostic() { void copy(JSON.stringify(selected.value ? detail.data.value : overview.data.value, null, 2), t('diagnosticCopied')) }
function refresh() { void overview.refetch(); void nodes.refetch(); void runtime.refetch(); if (selected.value) void history.refetch(); if (selected.value) void detail.refetch() }
</script>

<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('serviceEyebrow') }}</p><h1>{{ t('service') }}</h1><p>{{ t('clusterServiceHint') }}</p></div><div class="heading-actions"><button @click="diagnostic"><Copy :size="16" />{{ t('copyDiagnostics') }}</button><button class="icon-button" :aria-label="t('refresh')" @click="refresh"><RefreshCw :size="17" /></button></div></div>
  <ErrorNotice v-if="overview.error.value || nodes.error.value" :error="overview.error.value || nodes.error.value" />
  <div class="service-metrics cluster-summary">
    <section class="surface service-metric sky"><Network :size="25" /><span>{{ t('clusterNodesOnline') }}</span><strong>{{ online }} / {{ total }}</strong><small>{{ t('clusterAvailable', { count: available }) }}</small><small>{{ t('clusterContactHint') }}</small></section>
    <section class="surface service-metric lilac"><HeartPulse :size="25" /><span>{{ t('clusterRoles') }}</span><div class="cluster-role-counts"><span v-for="item in roles" :key="item.role">{{ t('nodeRole_' + item.role) }} <strong>{{ item.count }}</strong></span></div><small>{{ t('clusterWorkersHint') }}</small></section>
    <section class="surface service-metric mint"><Settings2 :size="25" /><span>{{ t('clusterConfiguration') }}</span><strong>{{ !overview.data.value || !total ? t('nodeState_unknown') : pending ? t('clusterPending', { count: pending }) : unknown ? t('clusterUnknown', { count: unknown }) : t('clusterValuesApplied') }}</strong><RouterLink class="text-button" to="/runtime-config">{{ t('clusterDeploymentSettings') }}<ArrowUpRight :size="15" /></RouterLink></section>
  </div>
  <section class="surface cluster-board">
    <div class="section-heading"><div><h2><Network :size="19" />{{ t('clusterNodes') }}</h2><p class="field-help">{{ t('clusterReportHint') }}</p></div><span v-if="overview.data.value" class="field-help">{{ date(overview.data.value.observed_at) }}</span></div>
    <p v-if="status.data.value?.maintenance" class="info-callout">{{ t('maintenanceBannerHint') }}</p>
    <div class="task-filters"><label class="inline-search"><input v-model.lazy="name" :placeholder="t('clusterSearchNodes')" :aria-label="t('clusterSearchNodes')"></label><UiSelect v-model="role" :options="roleOptions" :label="t('clusterRole')" /><UiSelect v-model="contact" :options="contactOptions" :label="t('clusterContact')" /><UiSelect v-model="work" :options="workOptions" :label="t('clusterWork')" /><button v-if="configuration || section" @click="router.replace({ query: { ...route.query, configuration: undefined, section: undefined } })">{{ t('clusterClearFilter') }}</button></div>
    <div class="node-list"><div class="node-list-heading"><span>{{ t('clusterNode') }}</span><span>{{ t('clusterContact') }}</span><span>{{ t('clusterWork') }}</span><span>{{ t('clusterConfiguration') }}</span><span>{{ t('clusterLastReport') }}</span></div>
      <button v-for="node in nodes.data.value?.items" :key="node.id" class="node-row" @click="router.push({ query: { ...route.query, node: node.id } })"><span class="node-identity"><strong>{{ node.name }}</strong><small>{{ t('nodeRole_' + node.role) }}</small></span><span><small class="node-mobile-label">{{ t('clusterContact') }}</small><span class="badge node-state" :data-tone="nodeTone(node.contact)">{{ t('nodeState_' + node.contact) }}</span></span><span><small class="node-mobile-label">{{ t('clusterWork') }}</small><span class="badge node-state" :data-tone="nodeTone(node.work)">{{ t('nodeState_' + node.work) }}</span></span><span><small class="node-mobile-label">{{ t('clusterConfiguration') }}</small><span class="badge node-state" :data-tone="nodeTone(node.configuration)">{{ t('nodeState_' + node.configuration) }}</span></span><span class="node-time"><small class="node-mobile-label">{{ t('clusterLastReport') }}</small>{{ node.sampled_at ? date(node.sampled_at) : t('nodeState_not_reported') }}<small v-if="!node.report_fresh">{{ t('clusterReportStale') }}</small></span></button>
    </div><EmptyState v-if="nodes.data.value && !nodes.data.value.items.length" compact :title="t('clusterNoNodes')" :description="t('clusterNoNodesHint')" /><UiListFooter :first="!!after" :next="!!nodes.data.value?.next_cursor" @first="after = ''" @next="after = nodes.data.value?.next_cursor || ''" />
  </section>
  <section class="surface cluster-runtime"><div class="section-heading"><h2>{{ t('requestsOverTime') }}</h2><span class="field-help">{{ t('clusterSampleWindow') }}</span></div><ErrorNotice v-if="runtime.error.value" :error="runtime.error.value" /><TrendChart :points="requestHistory(runtime.data.value)" :label="t('requestsPerSecond')" unit="rate" /></section>
  <RuntimeCoverage :runtime="runtime.data.value?.cluster" />
  <UiDialog v-model:open="open" drawer :title="detail.data.value?.node.name || t('clusterNode')" :description="t('clusterNodeDetailHint')">
    <ErrorNotice v-if="detail.error.value" :error="detail.error.value" /><template v-if="detail.data.value"><div class="node-detail-status"><span class="badge">{{ t('nodeRole_' + detail.data.value.node.role) }}</span><span v-for="(state, index) in [detail.data.value.node.contact, detail.data.value.node.work, detail.data.value.node.configuration]" :key="index" class="badge node-state" :data-tone="nodeTone(state)">{{ t('nodeState_' + state) }}</span></div><p v-if="!detail.data.value.node.report_fresh" class="info-callout">{{ t('clusterReportStaleHint') }}</p>
      <dl class="facts"><dt>{{ t('clusterNodeId') }}</dt><dd class="technical-key">{{ detail.data.value.node.id }}</dd><dt>{{ t('startedAt') }}</dt><dd>{{ detail.data.value.started_at ? date(detail.data.value.started_at) : '—' }}</dd><dt>{{ t('clusterLastReport') }}</dt><dd>{{ detail.data.value.node.sampled_at ? date(detail.data.value.node.sampled_at) : '—' }}</dd><dt>{{ t('clusterReceivedAt') }}</dt><dd>{{ detail.data.value.node.received_at ? date(detail.data.value.node.received_at) : '—' }}</dd><dt>{{ t('processMemory') }}</dt><dd>{{ snapshot?.process_memory?.rss_bytes == null ? '—' : bytes(snapshot.process_memory.rss_bytes) }}</dd><dt>{{ t('databasePool') }}</dt><dd>{{ snapshot?.db_pool_idle ?? '—' }} / {{ snapshot?.db_pool_size ?? '—' }}</dd></dl>
      <section v-if="connection" class="node-detail-section node-connection"><h3>{{ t('nodeConnection') }}</h3>
        <dl class="facts">
          <dt>{{ t('nodeConnectionState') }}</dt><dd><span class="badge node-state" :data-tone="nodeTone(connection.phase)">{{ connectionPhase }}</span></dd>
          <dt>{{ t('nodeControlAddress') }}</dt><dd>{{ connection.control.url }}</dd>
          <dt>{{ t('nodeControlInstance') }}</dt><dd>{{ connection.control.id ?? '—' }}</dd>
          <dt>{{ t('nodeControlPeer') }}</dt><dd>{{ connection.control.peer ?? '—' }}</dd>
          <dt>{{ t('nodeRpcLatency') }}</dt><dd>{{ milliseconds(connection.rpc.p50_ms) }} / {{ milliseconds(connection.rpc.p95_ms) }} / {{ milliseconds(connection.rpc.p99_ms) }}</dd>
          <template v-if="connection.control.last_error"><dt>{{ t('nodeLastError') }}</dt><dd>{{ connection.control.last_error }}</dd></template>
        </dl><p class="field-help">{{ t('nodeRpcCounts', { requests: connection.rpc.requests, retries: connection.rpc.retries, failures: connection.rpc.failures }) }}</p>
      </section>
      <section v-for="[name, queue] in queues" :key="name" class="node-detail-section"><h3><Cloud :size="16" />{{ t('queue_' + name) }} <span class="badge">{{ queue.running }} / {{ queue.limit }}</span></h3><dl class="facts"><dt>{{ t('foregroundQueue') }}</dt><dd>{{ queue.queued.foreground }}</dd><dt>{{ t('uploadQueue') }}</dt><dd>{{ queue.queued.upload }}</dd><dt>{{ t('maintenanceQueue') }}</dt><dd>{{ queue.queued.maintenance }}</dd></dl></section>
      <section v-if="snapshot?.runtime" class="node-detail-section"><h3>{{ t('clusterRequests') }}</h3><dl class="facts"><template v-for="(listener, name) in snapshot.runtime.http" :key="name"><dt>{{ name }}</dt><dd>{{ t('clusterRequestCounts', { active: listener.active, started: listener.started, failed: listener.failed }) }}</dd></template></dl></section>
      <section v-if="snapshot?.cache" class="node-detail-section"><h3>{{ t('localPantry') }}</h3><dl class="facts"><dt>{{ t('chunkCache') }}</dt><dd>{{ snapshot.cache.bytes == null ? '—' : bytes(snapshot.cache.bytes) }} / {{ snapshot.cache.limit == null ? '—' : bytes(snapshot.cache.limit) }}</dd><dt>{{ t('uploadCache') }}</dt><dd>{{ snapshot.cache.upload_bytes == null ? '—' : bytes(snapshot.cache.upload_bytes) }} / {{ snapshot.cache.upload_limit == null ? '—' : bytes(snapshot.cache.upload_limit) }}</dd></dl></section>
      <section v-if="snapshot?.read_replica" class="node-detail-section"><h3>{{ t('configSection_replica_database') }}</h3><span class="badge node-state" :data-tone="snapshot.read_replica.state === 'available' ? 'mint' : 'cream'">{{ t('replicaState_' + String(snapshot.read_replica.state)) }}</span><p class="field-help">{{ t('clusterReplicaHint') }}</p></section>
      <section class="node-detail-section"><h3>{{ t('clusterCurrentTasks') }}</h3><p v-if="!detail.data.value.tasks.length" class="field-help">{{ t('clusterNoTasks') }}</p><RouterLink v-for="task in detail.data.value.tasks" :key="task.id" class="node-task-link" :to="{ path: '/tasks', query: { task: task.id } }">{{ t('task_' + task.task_type) }}<span>{{ t(task.status) }}</span><ArrowUpRight :size="15" /></RouterLink><p v-if="detail.data.value.more_tasks" class="field-help">{{ t('clusterMoreTasks') }}</p></section>
      <section class="node-detail-section"><h3>{{ t('requestsOverTime') }}</h3><ErrorNotice v-if="history.error.value" :error="history.error.value" /><TrendChart :points="nodeRequests" :label="t('requestsPerSecond')" unit="rate" /></section>
      <details class="node-detail-section"><summary>{{ t('runtimeCounters') }}</summary><pre class="node-diagnostics">{{ JSON.stringify({ response_cache: snapshot?.response_cache, compression: snapshot?.compression, io: snapshot?.io, connection: connection }, null, 2) }}</pre></details>
    </template><template #footer><button @click="diagnostic"><Copy :size="16" />{{ t('copyDiagnostics') }}</button><RouterLink class="button primary" :to="{ path: '/runtime-config', query: { view: 'node', node: selected } }">{{ t('clusterNodeConfig') }}<ArrowUpRight :size="16" /></RouterLink></template>
  </UiDialog>
</template>
