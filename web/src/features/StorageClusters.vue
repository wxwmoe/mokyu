<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { ChevronRight, Network, Layers, Pencil, Plus, RefreshCw, Server } from 'lucide-vue-next'
import { api, bytes, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { useDeployment } from '../app/cluster'
import { t, date } from '../app/i18n'
import { report } from '../app/feedback'
import ErrorNotice from '../components/ErrorNotice.vue'
import EmptyState from '../components/EmptyState.vue'
import StorageDiskChoices from '../components/StorageDiskChoices.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'

type Catalog = components['schemas']['StorageClusterOverview']
type Cluster = components['schemas']['StorageClusterCluster']
type Policy = components['schemas']['StorageClusterPolicy']
const route = useRoute(), router = useRouter(), { cluster, status } = useDeployment()
const catalog = useQuery({ queryKey: ['storage-clusters'], enabled: cluster, queryFn: ({ signal }) => api<Catalog>('/api/v1/storage-clusters', { signal }), refetchInterval: 5000 })
watch(() => status.data.value?.deployment_mode, mode => { if (mode === 'single') void router.replace('/storages') }, { immediate: true })
const current = computed(() => {
  const rows = catalog.data.value?.clusters
  return rows?.find(row => row.id === route.query.cluster) || rows?.[0]
})
const selected = computed({
  get: () => current.value?.id || '',
  set(id: string) { void router.replace({ query: { ...route.query, cluster: id } }) },
})
const busy = ref(false), open = ref(false), name = ref(''), policy = ref<Policy>(), editing = ref<Cluster>(), operation = ref<'add' | 'enable' | 'bind'>(), media = ref<string[]>([])
const space = useQuery({ queryKey: ['storage-cluster-space', selected], enabled: computed(() => !!current.value), queryFn: ({ signal }) => api<components['schemas']['StorageClusterSpace']>(`/api/v1/storage-clusters/${selected.value}/space`, { signal }), refetchInterval: 5000 })
const backupGroups = computed(() => catalog.data.value?.backups.filter(row => row.cluster === selected.value) || [])
const movingFrom = ref(''), moveTarget = ref(''), moveRate = ref(16 * 1024 * 1024), moveRevision = ref(0)
const memberMoves = computed(() => catalog.data.value?.member_moves.filter(j => j.cluster === selected.value) || [])
const moveOptions = computed(() => {
  const members = current.value?.manifest.members || [], source = members.find(m => m.media_id === movingFrom.value)
  const backup = source?.topology.purpose === 'backup'
  const eligible = members.filter(m => m.media_id !== movingFrom.value && !grouped(m.media_id)
    && (backup ? m.topology.purpose === 'backup' && m.topology.storage_group === source?.topology.storage_group && m.state === 'available'
      : m.topology.purpose === 'primary' && ['available', 'spare'].includes(m.state))
    && !memberMoves.value.some(j => !['cancelled', 'completed'].includes(j.state) && [j.source, j.target].includes(m.media_id)))
  return [{ value: '', label: t(backup ? 'scBackupPeer' : 'scAutoSpare') }, ...eligible.map(m => ({ value: m.media_id, label: m.topology.storage_group, description: m.topology.url }))]
})
const runBackup = ref(''), runMedia = ref(''), runKind = ref(''), runRevision = ref(0), runRate = ref(16 * 1024 * 1024)
const clusterRuns = computed(() => catalog.data.value?.runs.filter(job => job.cluster === selected.value) || [])
const runActive = computed(() => clusterRuns.value.some(job => ['objects', 'snapshots'].includes(job.state)))
const rebuildSources = computed(() => [{ value: '', label: t('scPrimarySource') }, ...[...new Set(current.value?.manifest.members.filter(m => m.topology.purpose === 'backup').map(m => m.topology.storage_group))].sort().map(group => ({ value: group, label: t('scBackupSource', { group }) }))])
function selectRun(kind: string, media = '') { runBackup.value = ''; runMedia.value = media; runKind.value = kind; runRevision.value = current.value!.revision }
async function changeRun(job?: string) {
  const row = current.value
  if (!row) return
  busy.value = true
  try {
    const base = '/api/v1/storage-clusters/' + row.id + '/runs'
    if (job) await write(base + '/' + job + '/cancel', { revision: row.revision })
    else await write(base, { revision: runRevision.value, kind: runKind.value, backup_group: runKind.value === 'rebuild' && runBackup.value || null, media_id: runMedia.value || null, bytes_per_second: runRate.value })
    runKind.value = ''; await refresh()
  } catch (error) { report(error) } finally { busy.value = false }
}
function replaceMember(id: string) { movingFrom.value = id; moveTarget.value = ''; moveRevision.value = current.value!.revision }
function memberName(id: string) { return current.value?.manifest.members.find(m => m.media_id === id)?.topology.storage_group || id }
async function moveAction(job?: string, action?: string) {
  const row = current.value
  if (!row) return
  busy.value = true
  try {
    const base = `/api/v1/storage-clusters/${row.id}/moves`
    if (job) await write(base + '/' + job, { revision: row.revision, action }, 'PUT')
    else await write(base, { revision: moveRevision.value, source: movingFrom.value, target: moveTarget.value || null, bytes_per_second: moveRate.value })
    movingFrom.value = ''; await refresh()
  } catch (error) { report(error) } finally { busy.value = false }
}
const showPoints = ref(false), pages = ref<string[]>([''])
const cursor = computed(() => pages.value.at(-1) || '')
const points = useQuery({ queryKey: ['storage-backup-points', selected, cursor], enabled: computed(() => !!current.value && showPoints.value), queryFn: ({ signal }) => api<components['schemas']['StorageClusterBackupPoints']>(`/api/v1/storage-clusters/${selected.value}/backups${cursor.value ? '?after=' + cursor.value : ''}`, { signal }), refetchInterval: 10000 })
watch(selected, () => { showPoints.value = false; pages.value = [''] })
const operationRevision = ref(0)
const canApply = computed(() => {
  if (operation.value === 'bind') return !!name.value.trim()
  return media.value.length > 0 && (operation.value !== 'enable' || media.value.length % 2 === 0)
})
const candidates = computed(() => {
  if (operation.value === 'add') return catalog.data.value?.devices.filter(row => !row.cluster_id && !row.announcement.manifest && !row.announcement.pending_manifest) || []
  const row = current.value
  return catalog.data.value?.devices.filter(device => row?.manifest.members.some(member => member.media_id === device.media_id && member.state === 'available' && member.topology.purpose === 'primary') && !row.manifest.groups.some(group => group.members.includes(device.media_id))) || []
})
const operationTitle = computed(() => t(operation.value === 'add' ? 'scAdd' : operation.value === 'enable' ? 'scEnable' : 'scBind'))
const operationHint = computed(() => t(operation.value === 'enable' ? 'scEnableHint' : operation.value === 'bind' ? 'scBindingHint' : 'scAddHint'))
function jobTone(state: string) {
  if (state === 'completed') return 'mint'
  if (state === 'paused') return 'lilac'
  return state === 'cancelled' ? 'gray' : 'sky'
}
const choices = (values: string[]) => values.map(value => ({ value, label: t('sc_' + value) }))
const acknowledgements = computed(() => choices(['all', 'data_and_parity', 'data']))
const degraded = computed(() => [{ value: '', label: t('scInherit') }, ...acknowledgements.value])
function edit(row?: Cluster) {
  editing.value = row
  name.value = row?.name || ''
  const source = row?.policy || catalog.data.value?.defaults
  policy.value = source ? { ...source, layout: { ...source.layout } } : undefined
  open.value = true
}
async function verifyBackups(row: Cluster) {
  busy.value = true
  try { await write(`/api/v1/storage-clusters/${row.id}/verify-backups`, { revision: row.revision }); await refresh() }
  catch (error) { report(error) }
  finally { busy.value = false }
}
async function refresh() { await queries.invalidateQueries({ queryKey: ['storage-clusters'] }) }
async function save() {
  if (!policy.value) return
  busy.value = true
  try {
    const row = editing.value
    const saved = await write<Cluster>(row ? `/api/v1/storage-clusters/${row.id}` : '/api/v1/storage-clusters', { name: name.value, policy: policy.value, ...(row ? { revision: row.revision } : {}) }, row ? 'PUT' : 'POST')
    selected.value = saved.id; open.value = false; await refresh()
  } catch (error) { report(error) } finally { busy.value = false }
}
function choose(value: 'add' | 'enable' | 'bind') { operationRevision.value = current.value?.revision || 0; operation.value = value; media.value = []; name.value = '' }
async function apply() {
  const row = current.value
  if (!row || !operation.value) return
  busy.value = true
  try {
    if (operation.value === 'bind') {
      await write('/api/v1/storages', { name: name.value, type: 'mokyu', cluster_id: row.id, initialize: true })
      await queries.invalidateQueries({ queryKey: ['storages'] })
    } else await write(`/api/v1/storage-clusters/${row.id}/${operation.value === 'add' ? 'members' : 'groups'}`, { revision: operationRevision.value, media_ids: media.value })
    operation.value = undefined; await refresh()
  } catch (error) { report(error) } finally { busy.value = false }
}
async function memberState(id: string, state: string) {
  const row = current.value
  if (!row) return
  busy.value = true
  try { await write(`/api/v1/storage-clusters/${row.id}/members/${id}`, { revision: row.revision, state }, 'PUT'); await refresh() }
  catch (error) { report(error) } finally { busy.value = false }
}
function observed(id: string) { return catalog.data.value?.devices.find(row => row.media_id === id) }
function grouped(id: string) { return current.value?.manifest.groups.some(group => group.members.includes(id)) }
function available(id: string) {
  const capacity = observed(id)?.announcement.capacity
  return capacity?.accounting_ready ? Math.min(Math.max(0, capacity.available_bytes - capacity.min_free_space - capacity.reserved_bytes), capacity.limit == null ? Infinity : Math.max(0, capacity.limit - capacity.used_bytes - capacity.reserved_bytes)) : null
}
</script>

<template>
  <div class="page-heading">
    <div><p class="eyebrow">{{ t('administration') }}</p><h1>{{ t('storageClusters') }}</h1><p>{{ t('scHint') }}</p></div>
    <div class="heading-actions"><button class="icon-button" :aria-label="t('refresh')" @click="refresh"><RefreshCw :size="17" /></button><button class="primary" :disabled="!catalog.data.value" @click="edit()"><Plus :size="17" />{{ t('scCreate') }}</button></div>
  </div>
  <ErrorNotice :error="catalog.error.value" />
  <template v-if="catalog.data.value">
    <EmptyState v-if="!catalog.data.value.clusters.length" :title="t('scEmpty')" :description="t('scEmptyHint')" />
    <div v-else class="sc-layout">
      <nav class="sc-list" :aria-label="t('storageClusters')">
        <button v-for="row in catalog.data.value.clusters" :key="row.id" :class="{ selected: selected === row.id }" @click="selected = row.id">
          <Network :size="20" /><span><strong>{{ row.name }}</strong><small>{{ t('sc_' + row.state) }}</small></span>
        </button>
      </nav>
      <section v-if="current" class="surface sc-detail">
        <div class="sc-heading"><div><h2>{{ current.name }}</h2><span class="badge node-state" data-tone="cream">{{ t('sc_' + current.state) }}</span></div><button class="icon-button" :aria-label="t('scEdit')" @click="edit(current)"><Pencil :size="17" /></button></div>
        <p class="muted">{{ t('scHint_' + current.state) }}</p>
        <dl v-if="space.data.value" class="sc-facts sc-capacity"><div><dt>{{ t('scWritableSpace') }}</dt><dd>{{ bytes(space.data.value.logical_bytes) }}</dd></div><div><dt>{{ t('scWritableGroups') }}</dt><dd>{{ space.data.value.writable_groups }} / {{ space.data.value.groups }}</dd></div><div><dt>{{ t('scPrimaryUsed') }}</dt><dd>{{ bytes(space.data.value.primary_used_bytes) }}</dd></div></dl>
        <ErrorNotice :error="space.error.value" />
        <section v-if="current.policy.backup_copies || backupGroups.length" class="sc-backups">
          <div class="sc-heading"><h3>{{ t('scBackups') }}</h3><button v-if="backupGroups.length" class="button" :disabled="busy" @click="verifyBackups(current)"><RefreshCw :size="15" />{{ t('scVerifyBackups') }}</button></div>
          <p v-if="!backupGroups.length" class="muted">{{ t('scBackupEmpty') }}</p>
          <details v-if="backupGroups.length" class="sc-points" :open="showPoints" @toggle="showPoints = ($event.target as HTMLDetailsElement).open">
            <summary><ChevronRight :size="15" />{{ t('scRecoveryPoints') }}</summary>
            <ErrorNotice v-if="points.error.value" :error="points.error.value" />
            <p v-else-if="points.isLoading.value" class="sc-empty" role="status">{{ t('sc_checking') }}</p>
            <p v-else-if="points.data.value && !points.data.value.entries.length" class="sc-empty">{{ t('scNoPoints') }}</p>
            <div v-for="point in points.data.value?.entries" :key="point.storage + point.group" class="sc-backup-group">
              <strong>{{ point.name }} · {{ point.group }}</strong>
              <dl class="sc-facts">
                <div><dt>{{ t('scBackupWatermark') }}</dt><dd>{{ point.recoverable_through ?? '—' }}</dd></div>
                <div><dt>{{ t('scBackupEvidence') }}</dt><dd>{{ point.evidence_revision }} / {{ point.published_revision }}</dd></div>
                <div><dt>{{ t('scMissingBodies') }}</dt><dd>{{ point.missing_bodies }}</dd></div>
                <div><dt>{{ t('scMissingMetadata') }}</dt><dd>{{ point.missing_metadata }}</dd></div>
              </dl>
            </div>
            <div v-if="pages.length > 1 || points.data.value?.after" class="sc-actions"><button v-if="pages.length > 1" class="button" @click="pages.pop()">{{ t('previousPage') }}</button><button v-if="points.data.value?.after" class="button" @click="pages.push(points.data.value.after)">{{ t('nextPage') }}</button></div>
          </details>
          <div v-for="row in backupGroups" :key="row.storage_group || ''" class="sc-backup-group">
            <strong>{{ row.storage_group || t('scBackupUnassigned') }}</strong>
            <dl class="sc-facts"><div><dt>{{ t('scBackupPending') }}</dt><dd>{{ row.pending }} · {{ bytes(row.pending_bytes) }}</dd></div><div><dt>{{ t('scBackupVerified') }}</dt><dd>{{ row.verified }}</dd></div><div v-if="row.oldest_pending"><dt>{{ t('scBackupOldest') }}</dt><dd>{{ date(new Date(row.oldest_pending / 1000).toISOString()) }}</dd></div><div v-if="row.last_verified"><dt>{{ t('scBackupChecked') }}</dt><dd>{{ date(new Date(row.last_verified / 1000).toISOString()) }}</dd></div></dl>
          </div>
        </section>
        <dl class="sc-facts"><div><dt>{{ t('scLayout') }}</dt><dd>{{ t('scLayoutValue', { data: current.policy.layout.data_shards, parity: current.policy.layout.parity_shards, copies: current.policy.layout.extra_copies }) }}</dd></div><div><dt>{{ t('scGroups') }}</dt><dd>{{ current.manifest.groups.length }}</dd></div><div><dt>{{ t('scRevision') }}</dt><dd>{{ current.revision }}</dd></div></dl>
        <div class="sc-section-heading"><h3><Server :size="18" />{{ t('scMembers') }}</h3><button :disabled="busy" @click="choose('add')"><Plus :size="16" />{{ t('scAdd') }}</button></div>
        <p v-if="!current.manifest.members.length" class="sc-empty muted">{{ t('scNoMembers') }}</p>
        <article v-for="member in current.manifest.members" :key="member.media_id" class="sc-member">
          <div class="sc-heading"><strong>{{ member.topology.storage_group }}</strong><span class="badge node-state" :data-tone="member.state === 'paused' ? 'lilac' : grouped(member.media_id) ? 'sky' : 'cream'">{{ t('sc_' + (member.state === 'available' && !grouped(member.media_id) ? 'unassigned' : member.state)) }}</span></div>
          <p class="sc-address">{{ member.topology.url }}</p>
          <button v-if="(grouped(member.media_id) || member.topology.purpose === 'backup') && !memberMoves.some(j => j.source === member.media_id && !['cancelled','completed'].includes(j.state))" @click="replaceMember(member.media_id)">{{ t('scReplace') }}</button>
          <dl class="sc-facts"><div><dt>{{ t('scRegion') }}</dt><dd>{{ member.topology.region || t('scUnset') }}</dd></div><div><dt>{{ t('scPurpose') }}</dt><dd>{{ t('sc_' + member.topology.purpose) }}</dd></div><div><dt>{{ t('scAvailable') }}</dt><dd>{{ observed(member.media_id)?.announcement.capacity.accounting_ready === false ? t('scMeasuring') : available(member.media_id) == null ? '—' : bytes(available(member.media_id)!) }}</dd></div></dl>
          <div v-if="member.topology.purpose === 'primary' && member.state === 'available' && grouped(member.media_id)" class="sc-actions"><button :disabled="busy || runActive" @click="selectRun('sync', member.media_id)">{{ t('scRun_sync') }}</button></div>
          <div class="sc-member-footer"><small class="muted">{{ t('scObserved') }} · {{ observed(member.media_id) ? date(new Date(observed(member.media_id)!.observed_at / 1000).toISOString()) : '—' }}</small><UiSelect :model-value="member.state" :options="choices(grouped(member.media_id) || member.topology.purpose === 'backup' ? ['available', 'paused'] : ['available', 'paused', 'spare'])" :label="t('scMemberState')" :disabled="busy" @update:model-value="value => memberState(member.media_id, value)" /></div>
          <details><summary>{{ t('technicalDetails') }}</summary><dl class="sc-identifiers"><dt>{{ t('scMedia') }}</dt><dd>{{ member.media_id }}</dd><dt>{{ t('scNode') }}</dt><dd>{{ member.node_id }}</dd></dl></details>
        </article>
        <section class="sc-backups" :aria-label="t('scRunTitle')">
          <div class="sc-heading"><h3>{{ t('scRunTitle') }}</h3><div class="sc-actions"><button v-for="kind in ['scrub', 'rebuild']" :key="kind" :disabled="busy || runActive || !current.manifest.groups.length" @click="selectRun(kind)">{{ t('scRun_' + kind) }}</button></div></div>
          <article v-for="job in clusterRuns" :key="job.id" class="sc-backup-group">
            <div class="sc-heading"><strong>{{ t('scRun_' + job.kind) }}{{ job.media_id ? ' · ' + memberName(job.media_id) : '' }}</strong><span class="badge node-state" :data-tone="jobTone(job.state)">{{ t('scRun_' + job.state) }}</span></div>
            <p class="muted">{{ t('scRunProgress') }} · {{ job.checked }} · {{ bytes(job.byte_count) }}</p>
            <p v-if="job.last_error" role="status">{{ job.last_error }}</p>
            <div v-if="['objects', 'snapshots'].includes(job.state)" class="sc-actions"><button :disabled="busy" @click="changeRun(job.id)">{{ t('scRunCancel') }}</button></div>
          </article>
        </section>
        <section v-if="memberMoves.length" class="sc-backups">
          <h3>{{ t('scMaintenance') }}</h3>
          <article v-for="job in memberMoves" :key="job.id" class="sc-backup-group">
            <div class="sc-heading"><strong>{{ memberName(job.source) }} → {{ memberName(job.target) }}</strong><span class="badge node-state" :data-tone="jobTone(job.state)">{{ t('scMove_' + job.state) }}</span></div>
            <div v-if="['paused','moving'].includes(job.state)" class="sc-actions"><button :disabled="busy" @click="moveAction(job.id, job.state === 'paused' ? 'resume' : 'pause')">{{ t(job.state === 'paused' ? 'scResumeMove' : 'scPauseMove') }}</button><button :disabled="busy" @click="moveAction(job.id, 'cancel')">{{ t('scCancelMove') }}</button></div>
          </article>
        </section>
        <div class="sc-actions"><button :disabled="busy || !current.manifest.members.length" @click="choose('enable')"><Layers :size="16" />{{ t('scEnable') }}</button><button :disabled="busy" @click="choose('bind')">{{ t('scBind') }}</button></div>
      </section>
    </div>
    <section class="surface sc-discovery"><h2>{{ t('scDiscovered') }} <span class="muted">{{ catalog.data.value.devices.filter(row => !row.cluster_id).length }}</span></h2><p class="muted">{{ t('scDiscoveryHint') }}</p><div v-for="device in catalog.data.value.devices.filter(row => !row.cluster_id)" :key="device.media_id" class="sc-discovered"><div><strong>{{ device.announcement.topology.storage_group }}</strong><p class="sc-address">{{ device.announcement.topology.url }}</p></div><span class="badge node-state" data-tone="cream">{{ t(device.announcement.manifest || device.announcement.pending_manifest ? 'scTakeover' : 'scUnjoined') }}</span></div></section>
  </template>
  <UiDialog :open="!!runKind" :title="t('scRun_' + runKind)" :description="t('scRunHint_' + runKind)" :busy="busy" @update:open="value => { if (!value) runKind = '' }">
    <form id="sc-run" class="sc-form" @submit.prevent="changeRun()">
      <dl v-if="runMedia" class="sc-context"><dt>{{ t('scMember') }}</dt><dd>{{ memberName(runMedia) }}</dd></dl>
      <label v-if="runKind === 'rebuild' && rebuildSources.length > 1" class="field"><span>{{ t('scRebuildSource') }}</span><UiSelect v-model="runBackup" :options="rebuildSources" :label="t('scRebuildSource')" :disabled="busy" /><small v-if="runBackup">{{ t('scBackupSourceHint') }}</small></label>
      <label class="field"><span>{{ t('scMoveRate') }}</span><input v-model.number="runRate" type="number" min="1" max="1099511627776" required :disabled="busy" /><small>{{ bytes(runRate || 0) }}/s</small></label>
    </form>
    <template #footer><button :disabled="busy" @click="runKind = ''">{{ t('cancel') }}</button><button class="primary" form="sc-run" type="submit" :disabled="busy">{{ t('scRun_' + runKind) }}</button></template>
  </UiDialog>
  <UiDialog :open="!!movingFrom" :title="t('scReplace')" :description="t('scMoveHint')" :busy="busy" @update:open="value => { if (!value) movingFrom = '' }">
    <form id="sc-replace" class="sc-form" @submit.prevent="moveAction()">
      <dl class="sc-context"><dt>{{ t('scMember') }}</dt><dd>{{ memberName(movingFrom) }}</dd></dl>
      <label class="field"><span>{{ t('scMoveTarget') }}</span><UiSelect v-model="moveTarget" :label="t('scMoveTarget')" :options="moveOptions" :disabled="busy" /></label>
      <label class="field"><span>{{ t('scMoveRate') }}</span><input v-model.number="moveRate" type="number" min="1" max="1099511627776" required :disabled="busy" /><small>{{ bytes(moveRate || 0) }}/s</small></label>
    </form>
    <template #footer><button :disabled="busy" @click="movingFrom = ''">{{ t('cancel') }}</button><button class="primary" form="sc-replace" type="submit" :disabled="busy">{{ t('scStartMove') }}</button></template>
  </UiDialog>
  <UiDialog v-model:open="open" :title="t(editing ? 'scEdit' : 'scCreate')" :description="t('scPolicyHint')" :busy="busy" medium>
    <form v-if="policy" id="storage-cluster-policy" class="sc-form" @submit.prevent="save">
      <label class="field"><span>{{ t('name') }}</span><input v-model="name" required maxlength="128" :disabled="busy" /></label>
      <fieldset class="sc-policy-section"><legend>{{ t('scWriteSettings') }}</legend><div class="sc-form-grid">
        <label class="field"><span>{{ t('scAck') }}</span><UiSelect v-model="policy.acknowledgement" :label="t('scAck')" :options="acknowledgements" :disabled="busy" /></label>
        <label class="field"><span>{{ t('scDegradedAck') }}</span><UiSelect :model-value="policy.degraded_acknowledgement || ''" :options="degraded" :label="t('scDegradedAck')" :disabled="busy" @update:model-value="value => policy && (policy.degraded_acknowledgement = value as Policy['degraded_acknowledgement'] || null)" /></label>
      </div></fieldset>
      <fieldset class="sc-policy-section"><legend>{{ t('scPlacement') }}</legend><div class="sc-form-grid">
        <label class="field"><span>{{ t('scDataRegions') }}</span><UiSelect v-model="policy.data_regions" :label="t('scDataRegions')" :options="choices(['same_region','any'])" :disabled="busy" /></label>
        <label class="field"><span>{{ t('scParityRegions') }}</span><UiSelect v-model="policy.parity_regions" :label="t('scParityRegions')" :options="choices(['spread','data_region','any'])" :disabled="busy" /></label>
        <label class="field"><span>{{ t('scReplicaRegions') }}</span><UiSelect v-model="policy.replica_regions" :label="t('scReplicaRegions')" :options="choices(['different_region','same_region','original','any'])" :disabled="busy" /></label>
        <label class="field"><span>{{ t('scNodePriority') }}</span><UiSelect v-model="policy.node_priority" :label="t('scNodePriority')" :options="choices(['none','idle','free','fill'])" :disabled="busy" /></label>
        <label class="field"><span>{{ t('scOrder') }}</span><UiSelect v-model="policy.placement_order" :label="t('scOrder')" :options="choices(['region_first','node_first'])" :disabled="busy" /></label>
        <label class="field"><span>{{ t('scReadParallelism') }}</span><input v-model.number="policy.read_parallelism" type="number" min="1" max="256" required :disabled="busy" /></label>
      </div></fieldset>
      <fieldset class="sc-policy-section"><legend>{{ t('scBackups') }}</legend><div class="sc-form-grid">
        <label class="field"><span>{{ t('scBackupCopies') }}</span><input v-model.number="policy.backup_copies" type="number" min="0" max="256" required :disabled="busy" /><small>{{ t('scBackupCopiesHint') }}</small></label>
        <label v-if="policy.backup_copies" class="field"><span>{{ t('scBackupRate') }}</span><input v-model.number="policy.backup_bytes_per_second" type="number" min="1" max="1099511627776" required :disabled="busy" /><small>{{ bytes(policy.backup_bytes_per_second || 0) }}/s</small></label>
      </div></fieldset>
    </form>
    <template #footer><button type="button" :disabled="busy" @click="open = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy || !policy || !name.trim()" type="submit" form="storage-cluster-policy">{{ t(editing ? 'save' : 'scCreate') }}</button></template>
  </UiDialog>
  <UiDialog :open="!!operation" :title="operationTitle" :description="operationHint" :busy="busy" @update:open="value => { if (!value) operation = undefined }">
    <form id="storage-cluster-operation" class="sc-form" @submit.prevent="apply">
      <label v-if="operation === 'bind'" class="field"><span>{{ t('name') }}</span><input v-model="name" required maxlength="128" :disabled="busy" /></label>
      <p v-else-if="!candidates.length" class="sc-empty">{{ t('scNoCandidates') }}</p>
      <StorageDiskChoices v-else v-model="media" :devices="candidates" :disabled="busy" />
    </form>
    <template #footer><button type="button" :disabled="busy" @click="operation = undefined">{{ t('cancel') }}</button><button class="primary" type="submit" form="storage-cluster-operation" :disabled="busy || !canApply">{{ operationTitle }}</button></template>
  </UiDialog>
</template>

<style scoped>
.sc-backups { display:grid; gap:12px; padding-block:16px; border-block:1px solid var(--line); }
.sc-backups h3, .sc-backups p, .sc-backup-group .sc-facts { margin:0; }
.sc-backup-group { display:grid; gap:8px; min-width:0; overflow-wrap:anywhere; }
.sc-layout { display: grid; grid-template-columns: minmax(180px, 240px) minmax(0, 1fr); gap: 24px; align-items: start; }
.sc-list { display: grid; gap: 8px; }
.sc-list button { text-align: start; justify-content: start; padding: 16px; height: auto; }
.sc-list button > span { display: grid; gap: 6px; min-width: 0; overflow-wrap: anywhere; }
.sc-list small { font-weight: normal; color: var(--muted); }
.sc-list .selected { background: var(--rose-soft); border-color: var(--rose-line); }
.sc-detail, .sc-discovery { padding: 24px; min-width: 0; }
.sc-heading, .sc-section-heading, .sc-member-footer, .sc-discovered { display: flex; align-items: center; justify-content: space-between; gap: 16px; }
.sc-heading h2 { margin: 0 0 8px; }
.sc-section-heading { margin-top: 28px; }
.sc-section-heading h3 { display: flex; align-items: center; gap: 8px; margin: 0; }
.sc-facts { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin: 20px 0; }
.sc-facts dt { color: var(--muted); font-size: .85rem; }
.sc-facts dd { margin: 6px 0 0; overflow-wrap: anywhere; }
.sc-member { border-bottom: 1px solid var(--line-soft); padding: 24px 0; }
.sc-member:last-of-type { border-bottom: 0; }
.sc-address { display: block; color: var(--muted); overflow-wrap: anywhere; margin: 8px 0; }
.sc-member-footer small { min-width: 0; }
.sc-member-footer :deep(button) { min-width: 120px; }
.sc-member details { margin-top: 16px; }
.sc-identifiers { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 12px; font-size: .85rem; }
.sc-identifiers dd { margin: 0; text-align: end; overflow-wrap: anywhere; }
.sc-heading { display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap; }
.sc-backup-group + .sc-backup-group { border-top: 1px solid var(--line-soft); padding-top: 12px; }
.sc-points { padding: 10px 14px; background: var(--surface-soft); border: 1px solid var(--line-soft); border-radius: 12px; }
.sc-points > summary { display: flex; align-items: center; gap: 8px; list-style: none; cursor: pointer; font-weight: var(--ui-weight, 700); }
.sc-points > :not(summary) { margin-top: 12px; }
.sc-points summary::-webkit-details-marker { display: none; }
.sc-points[open] summary svg { transform: rotate(90deg); }
.sc-actions { display: flex; justify-content: flex-end; flex-wrap: wrap; gap: 12px; margin-top: 24px; }
.sc-empty { margin: 0; padding: 12px 0; color: var(--muted); }
.sc-points > .sc-empty { padding: 0; }
.sc-discovery { margin-top: 24px; }
.sc-discovered { padding: 16px 0; border-top: 1px solid var(--line-soft); }
.sc-discovered > div { min-width: 0; overflow-wrap: anywhere; }
.sc-discovered .badge { flex-shrink: 0; }
.sc-form { display: grid; gap: 18px; }
.sc-form .field { align-content: start; min-width: 0; margin: 0; }
.sc-form .field small { color: var(--muted); font-size: var(--small-size, 12px); font-weight: 400; }
.sc-form .field input, .sc-form :deep(.select-trigger) { width: 100%; min-width: 0; min-height: 44px; }
.sc-form :deep(.select-trigger) { text-align: start; overflow-wrap: anywhere; }
.sc-form :deep(.select-trigger svg) { flex: none; }
.sc-form-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); align-items: start; gap: 14px 20px; }
.sc-policy-section { min-width: 0; margin: 0; padding: 9px 0 0; border: 0; border-top: 1px solid var(--line); }
.sc-policy-section legend { padding: 0 10px 0 0; font-weight: 700; }
.sc-context { display: grid; gap: 3px; padding: 10px 14px; margin: 0; background: var(--surface-soft); border: 1px solid var(--line-soft); border-radius: 12px; }
.sc-context dt { color: var(--muted); font-size: var(--small-size, 12px); }
.sc-context dd { margin: 0; overflow-wrap: anywhere; }
@media (max-width: 720px) {
  .sc-layout { grid-template-columns: minmax(0, 1fr); }
  .sc-list { grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }
  .sc-detail, .sc-discovery { padding: 20px; }
  .sc-facts { grid-template-columns: minmax(0, 1fr); gap: 14px; }
  .sc-member-footer, .sc-discovered { align-items: flex-start; flex-direction: column; }
  .sc-form-grid { grid-template-columns: minmax(0, 1fr); }
}
</style>
