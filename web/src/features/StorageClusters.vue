<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeftRight, ChevronRight, Clock3, Database, HardDrive, Layers, Link2, Network, Pencil, Plus, RefreshCw, Server, ShieldCheck, Wrench } from 'lucide-vue-next'
import { api, bytes, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { useDeployment } from '../app/cluster'
import { t, date } from '../app/i18n'
import { report } from '../app/feedback'
import ErrorNotice from '../components/ErrorNotice.vue'
import EmptyState from '../components/EmptyState.vue'
import LayoutPicker from './StorageLayoutPicker.vue'
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
const picker = ref(false)
const space = useQuery({ queryKey: ['storage-cluster-space', selected], enabled: computed(() => !!current.value), queryFn: ({ signal }) => api<components['schemas']['StorageClusterSpace']>(`/api/v1/storage-clusters/${selected.value}/space`, { signal }), refetchInterval: 5000 })
const health = useQuery({ queryKey: ['storage-cluster-health', selected], enabled: computed(() => !!current.value), queryFn: ({ signal }) => api<components['schemas']['StorageClusterHealth']>(`/api/v1/storage-clusters/${selected.value}/health`,{signal}), refetchInterval:30000 })
const healthState = computed(() => health.data.value?.state || 'checking')
const healthTone = computed(() => {
  if (healthState.value === 'healthy') return 'mint'
  if (healthState.value === 'incomplete') return 'rose'
  return healthState.value === 'checking' ? 'gray' : 'cream'
})
const recovery = ref(''), recoveryMedia = ref<string[]>([]), recoveryName = ref(''), inspectMedia = ref(''), sourceAfter = ref('')
const returning = computed(() => catalog.data.value?.devices.filter(d => !d.cluster_id && !!d.announcement.manifest) || [])
const related = computed(() => returning.value.filter(d => d.announcement.manifest!.cluster_id === recovery.value))
const sources = useQuery({ queryKey: ['storage-recovery-sources', selected,inspectMedia,sourceAfter], enabled: computed(() => !!inspectMedia.value),
  queryFn: ({signal}) => api<components['schemas']['StorageRecoverySources']>(`/api/v1/storage-clusters/${selected.value}/recovery-sources?media=${inspectMedia.value}${sourceAfter.value ? '&after=' + sourceAfter.value : ''}`,{signal}), retry:false })
function inspect(id:string) { sourceAfter.value=''; inspectMedia.value=id }
watch(selected, () => inspect(''))
function reviewTakeover(id:string) {
  recovery.value=id; recoveryMedia.value=[];recoveryName.value=catalog.data.value?.clusters.find(c=>c.id===id)?.name || ''
}
async function takeover() {
  busy.value=true
  try { const result=await write<Cluster>('/api/v1/storage-clusters/takeover',{name:recoveryName.value,media_ids:recoveryMedia.value})
    selected.value=result.id; recovery.value=''; await refresh()
  } catch(error) { report(error) } finally {busy.value=false}
}
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
const preview = useQuery({
  queryKey: computed(() => ['storage-group-preview', selected.value, operationRevision.value, media.value.join(',')]),
  enabled: computed(() => operation.value === 'enable' && !!media.value.length),
  queryFn: ({ signal }) => api<components['schemas']['StorageGroupPreview']>(`/api/v1/storage-clusters/${selected.value}/groups/preview`, {
    method: 'POST', signal, headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ revision: operationRevision.value, media_ids: media.value }),
  }),
  retry: false,
})
const canApply = computed(() => {
  if (operation.value === 'bind') return !!name.value.trim()
  return media.value.length > 0 && (operation.value !== 'enable' ||
    (preview.isSuccess.value && !preview.isFetching.value && !preview.data.value?.reason))
})
const candidates = computed(() => {
  if (operation.value === 'add') return catalog.data.value?.devices.filter(row => !row.cluster_id && !row.announcement.manifest && !row.announcement.pending_manifest) || []
  const row = current.value
  return catalog.data.value?.devices.filter(device => row?.manifest.members.some(member => member.media_id === device.media_id && member.state === 'available' && member.topology.purpose === 'primary') && !row.manifest.groups.some(group => group.members.includes(device.media_id))) || []
})
const operationTitle = computed(() => t(operation.value === 'add' ? 'scAdd' : operation.value === 'enable' ? 'scEnable' : 'scBind'))
const operationHint = computed(() => {
  const layout = current.value?.policy.layout
  if (operation.value !== 'enable' || !layout) return t(operation.value === 'bind' ? 'scBindingHint' : 'scAddHint')
  const disks = (layout.data_shards + layout.parity_shards) * (1 + layout.extra_copies)
  return t(layout.parity_shards || layout.extra_copies ? 'scEnableProtectedHint' : 'scEnableHint', { disks })
})
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
        <button v-for="row in catalog.data.value.clusters" :key="row.id" :class="{ selected: selected === row.id }" :aria-current="selected === row.id ? 'true' : undefined" @click="selected = row.id">
          <Network :size="19" aria-hidden="true" /><span><strong>{{ row.name }}</strong><small>{{ row.manifest.groups.length ? t('scGroups') + ' · ' + row.manifest.groups.length : t('sc_' + row.state) }}</small></span>
        </button>
      </nav>
      <section v-if="current" class="surface sc-detail">
        <header class="sc-overview">
          <div class="sc-heading">
            <div class="sc-title"><span class="sc-emblem"><Network :size="23" /></span><h2>{{ current.name }}</h2><span class="badge node-state" :data-tone="healthTone">{{ t('sc_' + healthState) }}</span></div>
            <div class="sc-actions"><button :disabled="busy" @click="choose('bind')"><Link2 :size="16" />{{ t('scBind') }}</button><button class="icon-button" :aria-label="t('scEdit')" @click="edit(current)"><Pencil :size="17" /></button></div>
          </div>
          <p class="sc-health-hint">{{ t('scHint_' + healthState) }}</p>
          <ErrorNotice :error="health.error.value" /><ErrorNotice :error="space.error.value" />
          <dl class="sc-metrics">
            <div><dt><Database :size="16" />{{ t('scWritableSpace') }}</dt><dd>{{ space.data.value ? bytes(space.data.value.logical_bytes) : '—' }}</dd></div>
            <div><dt><ShieldCheck :size="16" />{{ t('scCompleteCopies') }}</dt><dd>{{ health.data.value?.complete_copies ?? '—' }}</dd></div>
            <div><dt><Layers :size="16" />{{ t('scProtectionDebt') }}</dt><dd>{{ health.data.value ? bytes(health.data.value.pending_bytes) : '—' }}</dd></div>
          </dl>
          <dl class="sc-facts sc-overview-facts">
            <div><dt>{{ t('scLayout') }}</dt><dd>{{ t('scLayoutValue', { data: current.policy.layout.data_shards, parity: current.policy.layout.parity_shards, copies: current.policy.layout.extra_copies }) }}</dd></div>
            <template v-if="health.data.value">
              <div><dt>{{ t('scCurrentAck') }}</dt><dd>{{ t(health.data.value.effective_acknowledgement ? 'sc_' + health.data.value.effective_acknowledgement : 'scNewWritesBlocked') }}</dd></div>
              <div><dt>{{ t('scPrimaryDomains') }}</dt><dd>{{ health.data.value.online_primary }} / {{ health.data.value.primary_domains }}</dd></div>
              <div><dt>{{ t('scReadable') }}</dt><dd>{{ health.data.value.readable }} / {{ health.data.value.tracked }}</dd></div>
            </template>
            <template v-if="space.data.value">
              <div><dt>{{ t('scWritableGroups') }}</dt><dd>{{ space.data.value.writable_groups }} / {{ space.data.value.groups }}</dd></div>
              <div><dt>{{ t('scPrimaryUsed') }}</dt><dd>{{ bytes(space.data.value.primary_used_bytes) }}</dd></div>
            </template>
          </dl>
          <div class="sc-overview-meta">
            <small v-if="health.data.value" class="sc-report"><Clock3 :size="13" />{{ t('scObserved') }} · {{ date(new Date(health.data.value.checked_at / 1000).toISOString()) }}</small>
            <small>{{ t('scRevision') }} {{ current.revision }}</small>
          </div>
        </header>
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
        <section class="sc-members" :aria-label="t('scMembers')">
          <div class="sc-section-heading">
            <h3><Server :size="18" />{{ t('scMembers') }}</h3>
            <div class="sc-actions">
              <button :disabled="busy || !current.manifest.members.length" @click="choose('enable')"><Layers :size="16" />{{ t('scEnable') }}</button>
              <button :disabled="busy" @click="choose('add')"><Plus :size="16" />{{ t('scAdd') }}</button>
            </div>
          </div>
          <p v-if="!current.manifest.members.length" class="sc-empty muted">{{ t('scNoMembers') }}</p>
          <div v-else class="sc-member-grid">
            <article v-for="member in current.manifest.members" :key="member.media_id" class="sc-member">
              <header class="sc-member-heading">
                <span class="sc-disk"><HardDrive :size="19" /></span>
                <div><div class="sc-member-title"><h4>{{ member.topology.storage_group }}</h4>
                  <span v-if="member.topology.purpose === 'primary' && member.state === 'available' && !grouped(member.media_id)" class="badge node-state" data-tone="cream">{{ t('sc_unassigned') }}</span>
                </div><p class="sc-address">{{ member.topology.url }}</p></div>
              </header>
              <dl class="sc-facts sc-member-facts">
                <div><dt>{{ t('scRegion') }}</dt><dd>{{ member.topology.region || t('scUnset') }}</dd></div>
                <div><dt>{{ t('scPurpose') }}</dt><dd>{{ t('sc_' + member.topology.purpose) }}</dd></div>
                <div><dt>{{ t('scAvailable') }}</dt><dd>{{ observed(member.media_id)?.announcement.capacity.accounting_ready === false ? t('scMeasuring') : available(member.media_id) == null ? '—' : bytes(available(member.media_id)!) }}</dd></div>
              </dl>
              <div class="sc-member-tools" role="group" :aria-label="member.topology.storage_group">
                <UiSelect class="sc-state node-state" :data-tone="member.state === 'paused' ? 'lilac' : member.state === 'spare' ? 'sky' : 'mint'" :model-value="member.state" :options="choices(grouped(member.media_id) || member.topology.purpose === 'backup' ? ['available', 'paused'] : ['available', 'paused', 'spare'])" :label="t('scMemberState')" :disabled="busy" @update:model-value="value => memberState(member.media_id, value)" />
                <button v-if="member.topology.purpose === 'primary' && member.state === 'available' && grouped(member.media_id)" :disabled="busy || runActive" @click="selectRun('sync', member.media_id)"><RefreshCw :size="15" />{{ t('scRun_sync') }}</button>
                <button v-if="(grouped(member.media_id) || member.topology.purpose === 'backup') && !memberMoves.some(j => j.source === member.media_id && !['cancelled','completed'].includes(j.state))" :disabled="busy" @click="replaceMember(member.media_id)"><ArrowLeftRight :size="15" />{{ t('scReplace') }}</button>
                <button v-if="current.manifest.takeover_epoch > 1 && observed(member.media_id)" @click="inspect(member.media_id)">{{ t('scInspectSources') }}</button>
              </div>
              <details class="sc-member-details">
                <summary><span><ChevronRight :size="14" />{{ t('technicalDetails') }}</span><small>{{ t('scObserved') }} · {{ observed(member.media_id) ? date(new Date(observed(member.media_id)!.observed_at / 1000).toISOString()) : '—' }}</small></summary>
                <dl class="sc-identifiers"><dt>{{ t('scMedia') }}</dt><dd>{{ member.media_id }}</dd><dt>{{ t('scNode') }}</dt><dd>{{ member.node_id }}</dd></dl>
              </details>
            </article>
          </div>
        </section>
        <section class="sc-backups" :aria-label="t('scRunTitle')">
          <div class="sc-heading"><h3><Wrench :size="17" />{{ t('scRunTitle') }}</h3><div class="sc-actions"><button v-for="kind in ['scrub', 'rebuild']" :key="kind" :disabled="busy || runActive || !current.manifest.groups.length" @click="selectRun(kind)">{{ t('scRun_' + kind) }}</button></div></div>
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
      </section>
    </div>
    <section class="surface sc-discovery"><h2>{{ t('scDiscovered') }} <span class="muted">{{ catalog.data.value.devices.filter(row => !row.cluster_id).length }}</span></h2><p class="muted">{{ t('scDiscoveryHint') }}</p><div v-for="device in catalog.data.value.devices.filter(row => !row.cluster_id)" :key="device.media_id" class="sc-discovered"><div><strong>{{ device.announcement.topology.storage_group }}</strong><p class="sc-address">{{ device.announcement.topology.url }}</p></div><span class="badge node-state" data-tone="cream">{{ t(device.announcement.manifest || device.announcement.pending_manifest ? 'scTakeover' : 'scUnjoined') }}</span><button v-if="device.announcement.manifest" @click="reviewTakeover(device.announcement.manifest.cluster_id)">{{ t('scReviewTakeover') }}</button></div></section>
  </template>
  <UiDialog :open="!!recovery" :title="t('scReviewTakeover')" :description="t('scTakeoverGuide')" :busy="busy" @update:open="v=>{ if (!v) recovery='' }">
    <form id="cluster-takeover" class="sc-form" @submit.prevent="takeover">
      <dl class="sc-context"><dt>{{ t('scSourceCluster') }}</dt><dd><code>{{ recovery }}</code></dd></dl>
      <label class="field"><span>{{ t('name') }}</span><input v-model.trim="recoveryName" required maxlength="128" :disabled="busy" /></label>
      <StorageDiskChoices v-model="recoveryMedia" :devices="related" :disabled="busy" />
    </form><template #footer><button :disabled="busy" @click="recovery=''">{{ t('cancel') }}</button><button class="primary" type="submit" form="cluster-takeover" :disabled="busy || !recoveryName || !recoveryMedia.length">{{ t('scAdopt') }}</button></template>
  </UiDialog>
  <UiDialog :open="!!inspectMedia" :title="t('scInspectSources')" :description="memberName(inspectMedia)" @update:open="v=>{ if (!v) inspect('') }">
    <ErrorNotice v-if="sources.error.value" :error="sources.error.value" />
    <p v-else-if="sources.isLoading.value" class="sc-empty" role="status">{{ t('sc_checking') }}</p>
    <p v-else-if="!sources.data.value?.entries.length" class="sc-empty">{{ t('scNoSources') }}</p>
    <ul v-else class="sc-sources">
      <li v-for="source in sources.data.value.entries" :key="source.id"><code>{{ source.prefix || source.id }}</code><span class="badge node-state" :data-tone="source.available ? 'mint' : 'cream'">{{ source.available ? t('scRevision') + ' ' + (source.revision ?? '—') : t('scNeedsEvidence') }}</span></li>
    </ul>
    <template #footer><button v-if="sourceAfter" @click="sourceAfter=''">{{ t('firstPage') }}</button><button v-if="sources.data.value?.next" @click="sourceAfter=sources.data.value!.next!">{{ t('nextPage') }}</button><button @click="inspect('')">{{ t('close') }}</button></template>
  </UiDialog>
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
  <LayoutPicker v-if="policy" v-model:open="picker" :rows="catalog.data.value?.layouts || []" :layout="policy.layout" @select="row => { if (policy) Object.assign(policy.layout, { data_shards: row.data, parity_shards: row.parity, extra_copies: row.copies }) }" />
  <UiDialog v-model:open="open" :title="t(editing ? 'scEdit' : 'scCreate')" :description="t('scPolicyHint')" :busy="busy" medium>
    <form v-if="policy" id="storage-cluster-policy" class="sc-form" @submit.prevent="save">
      <label class="field"><span>{{ t('name') }}</span><input v-model="name" required maxlength="128" :disabled="busy" /></label>
      <div class="sc-picker-field"><div><strong>{{ t('scLayout') }}</strong><p>{{ t('scLayoutValue', { data: policy.layout.data_shards, parity: policy.layout.parity_shards, copies: policy.layout.extra_copies }) }}</p></div><button type="button" :disabled="busy" @click="picker = true"><Layers :size="16" />{{ t('scPickLayout') }}</button></div>
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
      <template v-if="operation === 'enable' && media.length">
        <ErrorNotice v-if="preview.error.value" :error="preview.error.value" />
        <p v-else class="sc-preview node-state" :data-tone="preview.isFetching.value ? 'gray' : preview.data.value?.reason ? 'cream' : 'mint'" role="status">
          <template v-if="preview.isFetching.value">{{ t('sc_checking') }}</template>
          <template v-else-if="preview.data.value?.reason">{{ t('scPreview_' + preview.data.value.reason) }}<span v-if="preview.data.value.media"> · {{ memberName(preview.data.value.media) }}</span></template>
          <template v-else-if="preview.data.value">{{ t('scCanEnable', { groups: preview.data.value.groups, size: bytes(preview.data.value.logical_bytes) }) }}</template>
        </p>
      </template>
    </form>
    <template #footer><button type="button" :disabled="busy" @click="operation = undefined">{{ t('cancel') }}</button><button class="primary" type="submit" form="storage-cluster-operation" :disabled="busy || !canApply">{{ operationTitle }}</button></template>
  </UiDialog>
</template>

<style scoped>
.sc-layout { display: grid; grid-template-columns: minmax(160px, 210px) minmax(0, 1fr); gap: 20px; align-items: start; }
.sc-list { position: sticky; top: 93px; display: grid; gap: 8px; max-height: calc(100dvh - 110px); overflow-y: auto; padding: 4px; margin: -4px; }
.sc-list button { text-align: start; justify-content: start; gap: 12px; padding: 13px 15px; }
.sc-list button > svg { color: var(--muted); flex-shrink: 0; }
.sc-list button > span { display: grid; gap: 2px; min-width: 0; overflow-wrap: anywhere; }
.sc-list small, .sc-overview-meta, .sc-health-hint, .sc-address, .sc-member-details summary { color: var(--muted); }
.sc-list small { font-weight: normal; }
.sc-list .selected { background: var(--rose-soft); border-color: var(--rose-line); }
.sc-list .selected > svg { color: var(--rose); }
.sc-detail, .sc-discovery { min-width: 0; padding: clamp(16px, 2.2vw, 28px); }
.sc-detail { display: grid; gap: 24px; }
.sc-overview { display: grid; gap: 16px; }
.sc-heading, .sc-section-heading, .sc-actions, .sc-title, .sc-member-tools, .sc-overview-meta, .sc-discovered { display: flex; align-items: center; flex-wrap: wrap; gap: 10px; }
.sc-heading, .sc-section-heading, .sc-discovered { justify-content: space-between; }
.sc-title { min-width: 0; column-gap: 12px; }
.sc-heading h2, .sc-heading h3, .sc-section-heading h3, .sc-member h4, .sc-health-hint { margin: 0; overflow-wrap: anywhere; }
.sc-section-heading h3, .sc-heading h3, .sc-report, .sc-metrics dt, .sc-member-details summary span { display: flex; align-items: center; gap: 8px; }
.sc-section-heading h3 svg, .sc-heading h3 svg { color: var(--lilac); flex-shrink: 0; }
.sc-emblem, .sc-disk { display: grid; flex: none; place-items: center; border-radius: 12px; }
.sc-emblem { width: 40px; height: 40px; color: var(--rose); background: var(--rose-soft); }
.sc-actions { justify-content: flex-end; gap: 8px; }
.sc-actions > button, .sc-member-tools > button { max-width: 100%; }
.sc-health-hint { font-size: var(--small-size, 13px); }
.sc-metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 11rem), 1fr)); gap: 10px; margin: 0; }
.sc-metrics > div { min-width: 0; padding: 14px 16px; background: var(--surface-soft); border: 1px solid var(--line-soft); border-radius: 14px; }
.sc-metrics dt, .sc-facts dt { color: var(--muted); font-size: var(--small-size, 12px); }
.sc-metrics svg { flex: none; color: var(--lilac); }
.sc-metrics dd { margin: 6px 0 0; color: var(--ink); font-size: 1.45rem; font-weight: 700; line-height: 1.35; }
.sc-facts { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px 18px; margin: 0; }
.sc-facts dd { margin: 3px 0 0; overflow-wrap: anywhere; }
.sc-metrics dd, .sc-facts dd, .sc-overview-meta { font-variant-numeric: tabular-nums; }
.sc-overview-meta { gap: 6px 20px; }
.sc-member-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 27rem), 1fr)); gap: 14px; margin-top: 14px; }
.sc-member { display: flex; flex-direction: column; gap: 16px; min-width: 0; padding: 17px 18px 12px; border: 1px solid var(--line); border-radius: 16px; background: var(--surface); }
.sc-member-heading { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: start; gap: 10px; }
.sc-member-heading > div { min-width: 0; }
.sc-member-title { display: flex; align-items: center; flex-wrap: wrap; gap: 4px 8px; }
.sc-member h4 { font-size: 1.06rem; }
.sc-disk { width: 36px; height: 36px; color: var(--sky); background: var(--sky-soft); }
.sc-address { display: block; font-size: var(--small-size, 12px); margin: 1px 0 0; overflow-wrap: anywhere; }
.sc-member-facts { padding: 10px 12px; border-radius: 10px; background: var(--surface-soft); }
.sc-member-tools { margin-top: auto; gap: 8px; }
.sc-member-tools > button, .sc-member-tools :deep(.select-trigger) { box-sizing: border-box; min-height: 38px; padding: 7px 11px; font-size: var(--small-size, 12px); }
.sc-member-tools :deep(.sc-state) { width: auto; min-width: 0; gap: 10px; border-color: transparent; }
.sc-member-details { border-top: 1px solid var(--line-soft); padding-top: 10px; }
.sc-member-details summary { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 4px 12px; list-style: none; cursor: pointer; font-size: var(--small-size, 12px); }
.sc-member-details summary::-webkit-details-marker, .sc-points summary::-webkit-details-marker { display: none; }
.sc-member-details[open] summary svg, .sc-points[open] summary svg { transform: rotate(90deg); }
.sc-member-details small { font-size: var(--caption-size, 11px); }
.sc-identifiers { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 7px 14px; margin: 12px 0 0; font-size: var(--small-size, 12px); }
.sc-identifiers dt { color: var(--muted); }
.sc-identifiers dd { margin: 0; text-align: end; overflow-wrap: anywhere; }
.sc-members, .sc-backups { padding-top: 20px; border-top: 1px solid var(--line-soft); }
.sc-backups { display: grid; gap: 12px; }
.sc-backups h3, .sc-backups p, .sc-backup-group .sc-facts { margin: 0; }
.sc-backup-group { display: grid; gap: 8px; min-width: 0; overflow-wrap: anywhere; }
.sc-backup-group + .sc-backup-group { border-top: 1px solid var(--line-soft); padding-top: 12px; }
.sc-points { padding: 10px 14px; background: var(--surface-soft); border: 1px solid var(--line-soft); border-radius: 12px; }
.sc-points > summary { display: flex; align-items: center; gap: 8px; list-style: none; cursor: pointer; font-weight: var(--ui-weight, 700); }
.sc-points > :not(summary) { margin-top: 12px; }
.sc-empty { margin: 0; padding: 12px 0; color: var(--muted); }
.sc-points > .sc-empty { padding: 0; }
.sc-discovery { margin-top: 20px; }
.sc-discovery h2 { margin: 0; }
.sc-discovery h2 span { font-size: .85em; }
.sc-discovery > p { margin: 5px 0 0; }
.sc-discovered { margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--line-soft); }
.sc-discovered > div { min-width: 0; margin-inline-end: auto; overflow-wrap: anywhere; }
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
.sc-sources { display: grid; margin: 0; padding: 0; list-style: none; }
.sc-sources li { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 6px 14px; padding: 12px 0; }
.sc-sources li + li { border-top: 1px solid var(--line-soft); }
.sc-sources li:first-child { padding-top: 0; }
.sc-sources li:last-child { padding-bottom: 0; }
.sc-sources code { min-width: 0; overflow-wrap: anywhere; }
.sc-picker-field { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; justify-content: space-between; padding: 14px 16px; background: var(--surface-soft); border: 1px solid var(--line-soft); border-radius: 14px; }
.sc-picker-field p { margin: 4px 0 0; color: var(--muted); }
.sc-preview { margin: 0; padding: 10px 14px; border-radius: 10px; overflow-wrap: anywhere; }
@media (max-width: 980px) {
  .sc-layout { grid-template-columns: minmax(0, 1fr); gap: 14px; }
  .sc-list { position: static; display: flex; max-height: none; overflow-x: auto; }
  .sc-list button { flex: none; max-width: 100%; }
}
@media (max-width: 600px) {
  .sc-detail { gap: 18px; }
  .sc-overview-facts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .sc-member { padding: 14px 12px 10px; gap: 12px; }
  .sc-member-facts { column-gap: 8px; }
  .sc-heading, .sc-section-heading { gap: 12px; }
  .sc-actions { justify-content: flex-start; }
  .sc-discovered > div { flex-basis: 100%; }
  .sc-form-grid { grid-template-columns: minmax(0, 1fr); }
}
@media (max-width: 360px) {
  .sc-member-tools { display: grid; align-items: stretch; grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .sc-member-tools :deep(.sc-state) { grid-column: 1 / -1; justify-content: space-between; }
  .sc-member-tools > button { padding-inline: 6px; gap: 5px; }
}
</style>
