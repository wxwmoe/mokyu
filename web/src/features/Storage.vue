<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { Package, Sparkles, HardDrive, Archive, RefreshCw, ArrowUpRight, ChevronRight, Layers3 } from 'lucide-vue-next'
import { api, bytes, params, session } from '../api/client'
import type { components } from '../api/schema'
import { useInsights, type Runtime } from '../app/insights'
import { t, date } from '../app/i18n'
import { errorText } from '../app/feedback'
import ScopePicker from '../components/ScopePicker.vue'
import SpaceStory from '../components/SpaceStory.vue'
import EmptyState from '../components/EmptyState.vue'
import PackDetails from '../components/PackDetails.vue'
import UiSelect from '../components/ui/UiSelect.vue'
const route = useRoute(), router = useRouter(), admin = computed(() => session.value?.role === 'admin')
const { scope, query, statistics } = useInsights(), usage = computed(() => statistics.data.value?.usage)
const runtime = useQuery({ queryKey: ['runtime-insights'], enabled: admin, queryFn: ({ signal }) => api<Runtime>('/api/insights/runtime', { signal }), refetchInterval: 15_000 })
const after = ref(''), state = ref('ready')
const states = computed(() => ['all', 'ready', 'preparing', 'retired', 'deleting', 'deleted'].map(value => ({ value, label: t(value === 'all' ? 'allPackStates' : value) })))
const packQuery = computed(() => params({ ...scope.value, after: after.value || undefined, state: admin.value && state.value !== 'all' ? state.value : undefined }))
const packs = useQuery({ queryKey: ['packs', packQuery], queryFn: ({ signal }) => api<components['schemas']['PackPage']>('/api/storage/packs?' + packQuery.value, { signal }) })
watch([query, state], () => { after.value = '' })
const open = computed({ get: () => !!route.query.pack, set: value => { if (!value) router.replace({ query: { ...route.query, pack: undefined } }) } })
const bridge = computed(() => usage.value?.physical)
const rows = computed(() => bridge.value ? [
  { label: 'selectedBackend', value: bridge.value.selected_bytes, tone: 'rose' },
  { label: 'retainedBackend', value: bridge.value.retained_bytes, tone: 'lilac' },
  { label: 'waitingForGc', value: bridge.value.gc_bytes, tone: 'mint' },
] : [])
const now = computed(() => runtime.data.value?.current)
</script>
<template><div class="page-heading"><div><p class="eyebrow">{{ t('storageEyebrow') }}</p><h1>{{ t('storage') }}</h1><p>{{ t('storageHint') }}</p></div><div class="heading-actions"><ScopePicker /><button class="icon-button" :aria-label="t('refresh')" @click="statistics.refetch(); packs.refetch()"><RefreshCw :size="17" /></button></div></div>
  <p v-if="statistics.error.value" class="error" role="alert">{{ errorText(statistics.error.value) }}</p><p v-if="statistics.data.value?.stale && usage" class="snapshot-note">{{ t('snapshotStale') }}</p>
  <section class="surface insight-story"><div class="section-heading"><h2><Sparkles :size="19" />{{ t('roomForMore') }}</h2><span v-if="statistics.data.value?.as_of" class="field-help">{{ t('measuredAt', { time: date(statistics.data.value.as_of) }) }}</span></div><SpaceStory v-if="usage" :usage="usage" /><EmptyState v-else compact :title="t('collectingInsights')" :description="t('insightsWait')" /></section>
  <div v-if="bridge || now" class="storage-grid"><section v-if="bridge" class="surface overview-panel lilac-panel"><div class="section-heading"><h2><HardDrive :size="19" />{{ t('backendBridge') }}</h2></div><div class="bridge-total"><strong>{{ bytes(Number(bridge.indexed_bytes)) }}</strong><span>{{ t('indexedBackend') }}</span></div><div class="bridge-bar" :aria-label="t('backendBridge')" role="img"><span v-for="row in rows" :key="row.label" :class="row.tone" :style="{ width: Number(row.value) / Math.max(1, Number(bridge.indexed_bytes)) * 100 + '%' }" /></div><dl class="bridge-legend"><template v-for="row in rows" :key="row.label"><dt><i :class="row.tone" />{{ t(row.label) }}</dt><dd>{{ bytes(Number(row.value)) }}</dd></template><dt>{{ t('unconfirmedBackend') }}</dt><dd>{{ bytes(Number(bridge.unconfirmed_bytes)) }}</dd></dl><p class="field-help">{{ t('backendBridgeHint') }}</p></section>
    <section v-if="now" class="surface overview-panel mint-panel"><div class="section-heading"><h2><Archive :size="19" />{{ t('localPantry') }}</h2><span class="field-help">{{ t('deploymentOnly') }}</span></div><dl class="facts pantry-facts"><dt>{{ t('chunkCache') }}</dt><dd>{{ bytes(Number(now.cache_bytes)) }} / {{ bytes(Number(now.cache_limit)) }}</dd><dt>{{ t('uploadCache') }}</dt><dd>{{ bytes(Number(now.upload_bytes)) }} / {{ bytes(Number(now.upload_limit)) }}</dd><dt>{{ t('multipartStaging') }}</dt><dd>{{ bytes(Number(now.multipart_bytes)) }}</dd><dt>{{ t('thumbnailCache') }}</dt><dd>{{ bytes(Number(now.thumbnail_bytes)) }}</dd><dt>{{ t('processMemory') }}</dt><dd>{{ now.rss_bytes == null ? '—' : bytes(Number(now.rss_bytes)) }}</dd></dl><p class="field-help">{{ t('cacheOverlapHint') }}</p></section></div>
  <section class="surface pack-library"><div class="section-heading"><h2><Package :size="20" />{{ t('packLibrary') }}</h2><UiSelect v-if="admin" v-model="state" :options="states" :label="t('packState')" /></div><div v-if="usage" class="pack-library-intro"><span class="badge"><Layers3 :size="14" />{{ t('chunksInScope', { count: usage.chunks }) }}</span><span class="badge"><Package :size="14" />{{ t('packsInScope', { count: usage.packs }) }}</span><p>{{ t(admin ? 'packLibraryHint' : 'packScopedHint') }}</p></div>
    <p v-if="packs.error.value" class="error" role="alert">{{ errorText(packs.error.value) }}</p><div v-if="packs.isPending.value" class="skeleton-row" />
    <div v-else class="pack-grid"><button v-for="pack in packs.data.value?.packs" :key="pack.id" class="pack-card" @click="router.push({ query: { ...route.query, pack: pack.id } })"><img src="/assets/mokyu-pack.svg" alt="" width="74" height="74"><span class="badge">{{ t(pack.state) }}</span><strong>{{ t('packNumber', { id: pack.id }) }}</strong><span>{{ pack.stored_size == null ? t('visiblePack') : bytes(Number(pack.stored_size)) }} · {{ t(pack.compressed ? 'zstdPacked' : 'uncompressedPack') }}</span><ArrowUpRight :size="16" class="pack-open" /></button></div>
    <EmptyState v-if="packs.data.value && !packs.data.value.packs.length" compact :title="t('noPacksHere')" :description="t('noPacksHint')" /><div class="table-footer"><button v-if="after" @click="after = ''">{{ t('firstPage') }}</button><button v-if="packs.data.value?.next" @click="after = packs.data.value.next">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div>
  </section><PackDetails v-model:open="open" :id="String(route.query.pack || '')" :bucket="scope.bucket" :project="scope.project" />
</template>
