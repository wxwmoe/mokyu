<script setup lang="ts">
import { computed } from 'vue'
import { HeartPulse, Database, Cpu, Cloud, Copy, RefreshCw } from 'lucide-vue-next'
import ErrorNotice from '../components/ErrorNotice.vue'
import { bytes } from '../api/client'
import { useDeployment } from '../app/cluster'
import ClusterService from './ClusterService.vue'
import { t, date } from '../app/i18n'
import { copy } from '../app/feedback'

const { status, cluster } = useDeployment(5000)
const counter = computed(() => Object.fromEntries((status.data.value?.counters || []).map(row => [row.key, row.value])))
const queues = computed(() => Object.entries((status.data.value?.backend_queues || {}) as Record<string, { limit: number; running: number; queued: Record<string, number>; oldest_wait_seconds: number }>))
function diagnostic() { void copy(JSON.stringify({ service: status.data.value }, null, 2), t('diagnosticCopied')) }
</script>

<template>
  <ClusterService v-if="cluster" />
  <template v-else>
  <div class="page-heading"><div><p class="eyebrow">{{ t('serviceEyebrow') }}</p><h1>{{ t('service') }}</h1><p>{{ t('serviceHint') }}</p></div><div class="heading-actions"><button @click="diagnostic"><Copy :size="16" />{{ t('copyDiagnostics') }}</button><button class="icon-button" :aria-label="t('refresh')" @click="status.refetch()"><RefreshCw :size="17" /></button></div></div>
  <ErrorNotice v-if="status.error.value" :error="status.error.value" />
  <div>
      <div class="service-metrics"><section class="surface service-metric rose"><HeartPulse :size="25" /><span>{{ t('serviceMode') }}</span><strong>{{ status.data.value ? t(status.data.value.maintenance ? 'recoveryMode' : 'serviceOnline') : '—' }}</strong><small v-if="status.data.value">Mokyu {{ status.data.value.version }}</small></section><section class="surface service-metric lilac"><Cpu :size="25" /><span>{{ t('processMemory') }}</span><strong>{{ counter.rss_bytes && counter.rss_bytes !== 'null' ? bytes(Number(counter.rss_bytes)) : '—' }}</strong><small>{{ t('activeStreams', { count: counter.active_streams || '0' }) }}</small></section><section class="surface service-metric mint"><Database :size="25" /><span>{{ t('databasePool') }}</span><strong>{{ counter.db_pool_idle || '—' }} / {{ counter.db_pool_size || '—' }}</strong><small>{{ t('idleConnections') }}</small></section></div>
      <div class="service-queues"><section v-for="[queueName, queue] in queues" :key="queueName" class="surface queue-card"><div class="section-heading"><h2><Cloud :size="19" />{{ t('queue_' + queueName) }}</h2><span class="badge">{{ queue.running }} / {{ queue.limit }}</span></div><p class="field-help">{{ t('queueHint') }}</p><dl class="facts"><dt>{{ t('foregroundQueue') }}</dt><dd>{{ queue.queued.foreground }}</dd><dt>{{ t('uploadQueue') }}</dt><dd>{{ queue.queued.upload }}</dd><dt>{{ t('maintenanceQueue') }}</dt><dd>{{ queue.queued.maintenance }}</dd><dt>{{ t('oldestWait') }}</dt><dd>{{ t('secondsShort', { value: queue.oldest_wait_seconds }) }}</dd></dl></section></div>
      <section class="surface recovery-card"><div class="section-heading"><h2>{{ t('runtimeGroupMetrics') }}</h2></div><dl class="facts"><dt>{{ t('inventoryAsOf') }}</dt><dd>{{ status.data.value?.inventory_as_of ? date(status.data.value.inventory_as_of) : t('collectingInsights') }}</dd><dt>{{ t('startedAt') }}</dt><dd>{{ status.data.value?.started_at ? date(status.data.value.started_at) : '—' }}</dd></dl><p v-if="status.data.value?.inventory_error" class="error">{{ status.data.value?.inventory_error }}</p><p v-if="status.data.value?.inventory_stale" class="field-help">{{ t('snapshotStale') }}</p></section>
      <section class="surface recovery-card"><details><summary>{{ t('runtimeCounters') }}</summary><dl class="runtime-counters"><template v-for="row in status.data.value?.counters" :key="row.key"><dt>{{ row.key }}</dt><dd>{{ row.value }}</dd></template></dl></details></section>
  </div>
  </template>
</template>
