<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import RuntimeCoverage from '../components/RuntimeCoverage.vue'
import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Sparkles, HardDrive, Archive, RefreshCw, ArrowUpRight } from 'lucide-vue-next'
import { api, bytes, session } from '../api/client'
import { useInsights, type Runtime } from '../app/insights'
import { t, date } from '../app/i18n'
import EmptyState from '../components/EmptyState.vue'
import ScopePicker from '../components/ScopePicker.vue'
import SpaceStory from '../components/SpaceStory.vue'
const admin = computed(() => session.value?.role === 'admin')
const { statistics } = useInsights(), usage = computed(() => statistics.data.value?.usage)
const runtime = useQuery({ queryKey: ['runtime-insights'], enabled: admin, queryFn: ({ signal }) => api<Runtime>('/api/v1/usage/runtime', { signal }), refetchInterval: 15_000 })
const bridge = computed(() => usage.value?.physical)
const rows = computed(() => bridge.value ? [
  { label: 'selectedBackend', value: bridge.value.selected_bytes, tone: 'rose' },
  { label: 'retainedBackend', value: bridge.value.retained_bytes, tone: 'lilac' },
  { label: 'waitingForGc', value: bridge.value.gc_bytes, tone: 'mint' },
] : [])
const cluster = computed(() => runtime.data.value?.scope === 'cluster')
const now = computed(() => cluster.value ? runtime.data.value?.cluster?.current?.gauges : runtime.data.value?.current)
</script>
<template><div class="page-heading"><div><p class="eyebrow">{{ t('storageEyebrow') }}</p><h1>{{ t('storage') }}</h1><p>{{ t('storageHint') }}</p></div><div class="heading-actions"><ScopePicker /><button class="icon-button" :aria-label="t('refresh')" @click="statistics.refetch()"><RefreshCw :size="17" /></button></div></div>
  <ErrorNotice v-if="statistics.error.value" :error="statistics.error.value" /><p v-if="statistics.data.value?.stale && usage" class="snapshot-note">{{ t('snapshotStale') }}</p>
  <section class="surface insight-story"><div class="section-heading"><h2><Sparkles :size="19" />{{ t('roomForMore') }}</h2><span v-if="statistics.data.value?.as_of" class="field-help">{{ t('measuredAt', { time: date(statistics.data.value.as_of) }) }}</span></div><SpaceStory v-if="usage" :usage="usage" /><EmptyState v-else compact :title="t('collectingInsights')" :description="t('insightsWait')" /></section>
  <div v-if="bridge || now" class="storage-grid"><section v-if="bridge" class="surface overview-panel lilac-panel"><div class="section-heading"><h2><HardDrive :size="19" />{{ t('backendBridge') }}</h2></div><div class="bridge-total"><strong>{{ bytes(Number(bridge.indexed_bytes)) }}</strong><span>{{ t('indexedBackend') }}</span></div><div class="bridge-bar" :aria-label="t('backendBridge')" role="img"><span v-for="row in rows" :key="row.label" :class="row.tone" :style="{ width: Number(row.value) / Math.max(1, Number(bridge.indexed_bytes)) * 100 + '%' }" /></div><dl class="bridge-legend"><template v-for="row in rows" :key="row.label"><dt><i :class="row.tone" />{{ t(row.label) }}</dt><dd>{{ bytes(Number(row.value)) }}</dd></template><dt>{{ t('unconfirmedBackend') }}</dt><dd>{{ bytes(Number(bridge.unconfirmed_bytes)) }}</dd></dl><p class="field-help">{{ t('backendBridgeHint') }}</p></section>
    <section v-if="now" class="surface overview-panel mint-panel"><div class="section-heading"><h2><Archive :size="19" />{{ t(cluster ? 'clusterCacheCopies' : 'localPantry') }}</h2><span class="field-help">{{ t('deploymentOnly') }}</span></div><dl class="facts pantry-facts"><dt>{{ t('chunkCache') }}</dt><dd>{{ bytes(Number(now.cache_bytes)) }} / {{ bytes(Number(now.cache_limit)) }}</dd><dt>{{ t('uploadCache') }}</dt><dd>{{ bytes(Number(now.upload_bytes)) }} / {{ bytes(Number(now.upload_limit)) }}</dd><dt>{{ t('multipartStaging') }}</dt><dd>{{ bytes(Number(now.multipart_bytes)) }}</dd><dt>{{ t('imageCache') }}</dt><dd>{{ bytes(Number(now.image_bytes)) }}</dd><dt>{{ t('processMemory') }}</dt><dd>{{ now.rss_bytes == null ? '—' : bytes(Number(now.rss_bytes)) }}</dd></dl><p class="field-help">{{ t(cluster ? 'clusterCacheCopiesHint' : 'cacheOverlapHint') }}</p></section></div>
  <RuntimeCoverage v-if="cluster" :runtime="runtime.data.value?.cluster" />
</template>
