<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ArrowRight, Pause, Play, Plus, RefreshCw, X } from 'lucide-vue-next'
import { api, params, queries, write, uuid, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { report } from '../app/feedback'
import EmptyState from '../components/EmptyState.vue'
import ErrorNotice from '../components/ErrorNotice.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiListFooter from '../components/ui/UiListFooter.vue'
import UiSelect from '../components/ui/UiSelect.vue'

type Replication = components['schemas']['Replication']
type Options = components['schemas']['ReplicationOptions']
const endpoint = '/api/v1/replications'
let creationId = '', creationBody = ''
const cursor = ref(''), selected = ref(''), itemCursor = ref(''), creating = ref(false), busy = ref(false), error = ref<unknown>(), cancelling = ref(false)
const source = ref(''), target = ref(''), operation = ref('copy'), scope = ref('all'), prefix = ref(''), keys = ref(''), destination = ref(''), rename = ref(''), conflict = ref('report'), permissions = ref('inherit')
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/v1/buckets', { signal }) })
const jobs = useQuery({ queryKey: ['replications', cursor], queryFn: ({ signal }) => api<components['schemas']['ReplicationPage']>(endpoint + '?' + params({ after: cursor.value || undefined }), { signal }), refetchInterval: 5000 })
const job = useQuery({ queryKey: ['replication', selected], enabled: computed(() => !!selected.value), queryFn: ({ signal }) => api<Replication>(endpoint + '/' + selected.value, { signal }), refetchInterval: query => ['queued', 'running'].includes(query.state.data?.state || '') ? 2000 : false })
const items = useQuery({ queryKey: ['replication-items', selected, itemCursor], enabled: computed(() => !!selected.value), queryFn: ({ signal }) => api<components['schemas']['ReplicationItems']>(endpoint + '/' + selected.value + '/items?' + params({ after: itemCursor.value || undefined }), { signal }), refetchInterval: computed(() => ['queued', 'running'].includes(job.data.value?.state || '') ? 3000 : false) })
const details = computed({ get: () => !!selected.value, set: value => { if (!value) selected.value = '' } })
const sourceOptions = computed(() => (buckets.data.value || []).filter(bucket => bucket.status === 'active' && bucket.actions.includes('object.read') && (scope.value === 'objects' || bucket.actions.includes('bucket.list')) && (operation.value !== 'move' || bucket.actions.includes('object.delete'))).map(bucket => ({ value: bucket.id, label: bucket.name })))
const targetOptions = computed(() => (buckets.data.value || []).filter(bucket => bucket.status === 'active' && bucket.actions.includes('object.write') && (permissions.value === 'ignore' || bucket.actions.includes('object.acl')) && (scope.value !== 'all' || bucket.id !== source.value)).map(bucket => ({ value: bucket.id, label: bucket.name })))
const selections = computed(() => ['all', 'prefix', 'objects'].map(value => ({ value, label: t('replicationScope_' + value) })))
const permissionOptions = computed(() => ['inherit', 'ignore', 'public_read', 'private'].map(value => ({ value, label: t('replicationPermission_' + value) })))
const conflicts = computed(() => ['report', 'skip', 'overwrite'].map(value => ({ value, label: t('replicationConflict_' + value) })))
const operations = computed(() => ['copy', 'move'].map(value => ({ value, label: value === 'copy' ? t('copy') : t('replicationMove') })))
const paths = computed(() => keys.value.split('\n').filter(key => key.length > 0))
const preview = computed(() => scope.value === 'objects' ? paths.value.slice(0, 5).map(key => [key, paths.value.length === 1 && rename.value ? rename.value : destination.value + key]) : [[scope.value === 'prefix' ? prefix.value + '*' : '*', destination.value + '*']])
const active = computed(() => !!job.data.value && !['complete', 'cancelled'].includes(job.data.value.phase))
watch(selected, () => { itemCursor.value = ''; error.value = undefined })
watch(() => job.data.value?.state, state => { if (state && !['queued', 'running'].includes(state)) void queries.invalidateQueries({ queryKey: ['replication-items', selected.value] }) })
watch(sourceOptions, options => { if (!options.some(option => option.value === source.value)) source.value = options[0]?.value || '' })
watch(targetOptions, options => { if (!options.some(option => option.value === target.value)) target.value = options[0]?.value || '' })
function bucketName(id: string) { return buckets.data.value?.find(bucket => bucket.id === id)?.name || id }
function tone(value: Replication) { return value.phase === 'cancelled' ? 'gray' : ({ completed: 'mint', failed: 'rose', queued: 'cream', paused: 'cream', running: 'sky' } as Record<string, string>)[value.state] || 'gray' }
function status(value: Replication) { return value.phase === 'cancelled' ? t('replicationPhase_cancelled') : t(value.state) }
function reason(code: string) { const key = 'replicationError_' + code, message = t(key); return message === key ? code : message }
async function refresh() { await queries.invalidateQueries({ queryKey: ['replications'] }); await queries.invalidateQueries({ queryKey: ['replication'] }); await queries.invalidateQueries({ queryKey: ['replication-items'] }) }
async function create() {
  if (!source.value || !target.value || busy.value) return
  busy.value = true; error.value = undefined
  try {
    const request: Options = { source_bucket: source.value, target_bucket: target.value, operation: operation.value as Options['operation'],
      scope: scope.value === 'all' ? { selection: 'all' } : scope.value === 'prefix' ? { selection: 'prefix', prefix: prefix.value } : { selection: 'objects', keys: paths.value },
      target_prefix: scope.value === 'objects' && paths.value.length === 1 && rename.value ? '' : destination.value,
      target_key: scope.value === 'objects' && paths.value.length === 1 && rename.value ? rename.value : null,
      conflict: conflict.value as Options['conflict'], permissions: permissions.value as Options['permissions'] }
    const encoded = JSON.stringify(request)
    if (encoded !== creationBody) { creationId = uuid(); creationBody = encoded }
    const result = await api<components['schemas']['TaskStarted']>(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': creationId }, body: encoded })
    creationBody = ''; creationId = ''
    creating.value = false; cursor.value = ''; selected.value = result.task_id
    await refresh()
  } catch (value) { error.value = value } finally { busy.value = false }
}
async function command(action: components['schemas']['ReplicationCommand']) {
  if (!selected.value || busy.value) return
  busy.value = true
  try { await write(endpoint + '/' + selected.value + '/command', action); cancelling.value = false; await refresh() }
  catch (value) { report(value) } finally { busy.value = false }
}
</script>

<template>
  <section class="page">
    <header class="page-heading"><div><h1>{{ t('replications') }}</h1><p>{{ t('replicationsHint') }}</p></div><div class="actions"><button class="icon-button" :aria-label="t('refresh')" @click="refresh"><RefreshCw :size="17" /></button><button class="primary" :disabled="!sourceOptions.length" @click="creating = true; error = undefined"><Plus :size="16" />{{ t('replicationCreate') }}</button></div></header>
    <ErrorNotice :error="jobs.error.value || buckets.error.value" />
    <div v-if="jobs.data.value?.replications.length" class="surface table-scroll">
      <table><thead><tr><th>{{ t('replicationSource') }}</th><th>{{ t('replicationTarget') }}</th><th>{{ t('replicationStatus') }}</th><th>{{ t('replicationProgress') }}</th><th><span class="sr-only">{{ t('details') }}</span></th></tr></thead><tbody>
        <tr v-for="row in jobs.data.value.replications" :key="row.id"><td>{{ bucketName(row.options.source_bucket) }}</td><td>{{ bucketName(row.options.target_bucket) }}</td><td><span class="badge node-state" :data-tone="tone(row)">{{ status(row) }}</span></td><td>{{ row.progress.completed }} / {{ row.progress.discovered }}<small class="muted">{{ t('replicationPhase_' + row.phase) }}</small></td><td><button @click="selected = row.id">{{ t('details') }}<ArrowRight :size="15" /></button></td></tr>
      </tbody></table>
    </div>
    <div v-else-if="jobs.isPending.value" class="skeleton-row" role="status" :aria-label="t('loading')" />
    <EmptyState v-else-if="jobs.isSuccess.value" :title="t('replicationEmpty')" />
    <UiListFooter :first="!!cursor" :next="!!jobs.data.value?.next_after" @first="cursor = ''" @next="cursor = jobs.data.value?.next_after || ''" />
    <UiDialog v-model:open="creating" :title="t('replicationCreate')" :busy="busy" wide>
      <form id="replication-form" class="replication-form" @submit.prevent="create">
        <ErrorNotice :error="error" />
        <div class="form-grid"><label>{{ t('replicationSource') }}<UiSelect v-model="source" :options="sourceOptions" :label="t('replicationSource')" searchable /></label><label>{{ t('replicationTarget') }}<UiSelect v-model="target" :options="targetOptions" :label="t('replicationTarget')" searchable /></label></div>
        <div class="form-grid"><label>{{ t('replicationOperation') }}<UiSelect v-model="operation" :options="operations" :label="t('replicationOperation')" /></label><label>{{ t('replicationScope') }}<UiSelect v-model="scope" :options="selections" :label="t('replicationScope')" /></label></div>
        <p v-if="operation === 'move'" class="muted">{{ t('replicationMoveHint') }}</p>
        <label v-if="scope === 'prefix'">{{ t('replicationPrefix') }}<input v-model="prefix" autocomplete="off" /></label>
        <label v-if="scope === 'objects'">{{ t('replicationKeys') }}<textarea v-model="keys" rows="5" required spellcheck="false" /><small>{{ t('replicationKeysHint') }}</small></label>
        <label>{{ t('replicationDestination') }}<input v-model="destination" :disabled="scope === 'objects' && paths.length === 1 && !!rename" autocomplete="off" /></label>
        <label v-if="scope === 'objects' && paths.length === 1">{{ t('replicationRename') }}<input v-model="rename" autocomplete="off" /></label>
        <div class="form-grid"><label>{{ t('replicationConflict') }}<UiSelect v-model="conflict" :options="conflicts" :label="t('replicationConflict')" /></label><label>{{ t('replicationPermissions') }}<UiSelect v-model="permissions" :options="permissionOptions" :label="t('replicationPermissions')" /></label></div>
        <small v-if="permissions === 'ignore'">{{ t('replicationIgnoreHint') }}</small>
        <div class="mapping-preview"><strong>{{ t('replicationMapping') }}</strong><div v-for="[from, to] in preview" :key="from"><code>{{ from }}</code><ArrowRight :size="15" /><code>{{ to }}</code></div></div>
      </form>
      <template #footer><button :disabled="busy" @click="creating = false">{{ t('close') }}</button><button form="replication-form" type="submit" class="primary" :disabled="busy || !source || !target || scope === 'objects' && (!paths.length || paths.length > 1000)">{{ t('replicationStart') }}</button></template>
    </UiDialog>
    <UiDialog v-model:open="details" :title="t('replications')" wide>
      <ErrorNotice :error="job.error.value || items.error.value" />
      <template v-if="job.data.value">
        <div class="replication-summary"><strong>{{ bucketName(job.data.value.options.source_bucket) }} → {{ bucketName(job.data.value.options.target_bucket) }}</strong><span class="badge node-state" :data-tone="tone(job.data.value)">{{ status(job.data.value) }}</span></div>
        <p>{{ t('replicationPhase_' + job.data.value.phase) }} · {{ job.data.value.progress.completed }} / {{ job.data.value.progress.discovered }}</p>
        <p class="muted">{{ t('replicationCounts', { skipped: job.data.value.progress.skipped, conflicts: job.data.value.progress.conflicts, sources: job.data.value.progress.sources }) }}</p>
        <p v-if="job.data.value.error" role="alert">{{ reason(job.data.value.error) }}</p>
        <div class="table-scroll"><table><thead><tr><th>{{ t('replicationSource') }}</th><th>{{ t('replicationTarget') }}</th><th>{{ t('replicationStatus') }}</th></tr></thead><tbody><tr v-for="item in items.data.value?.items" :key="item.id"><td class="object-key">{{ item.source_key }}</td><td class="object-key">{{ item.target_key }}</td><td>{{ t('replicationItem_' + item.state) }}<small v-if="item.error">{{ reason(item.error) }}</small></td></tr></tbody></table></div>
        <UiListFooter :first="!!itemCursor" :next="!!items.data.value?.next_after" @first="itemCursor = ''" @next="itemCursor = items.data.value?.next_after || ''" />
      </template>
      <template #footer><button v-if="active" :disabled="busy" class="danger-button" @click="cancelling = true"><X :size="15" />{{ t('cancel') }}</button><button v-if="active && ['queued', 'running'].includes(job.data.value?.state || '')" :disabled="busy" @click="command('pause')"><Pause :size="15" />{{ t('pauseTask') }}</button><button v-if="active && ['paused', 'failed'].includes(job.data.value?.state || '')" :disabled="busy" @click="command('resume')"><Play :size="15" />{{ t('resumeTask') }}</button><button @click="details = false">{{ t('close') }}</button></template>
    </UiDialog>
    <UiDialog v-model:open="cancelling" :title="t('replicationCancel')" :description="t('replicationCancelHint')" :busy="busy"><template #footer><button :disabled="busy" @click="cancelling = false">{{ t('close') }}</button><button class="danger-button" :disabled="busy" @click="command('cancel')">{{ t('replicationCancel') }}</button></template></UiDialog>
  </section>
</template>

<style scoped>
.actions, .replication-summary { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
.replication-summary { justify-content: space-between; }
.replication-form { display: grid; gap: 18px; }
.replication-form label { display: grid; gap: 8px; }
.form-grid { display: grid; align-items: start; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
.page, .table-scroll { min-width: 0; max-width: 100%; }
.table-scroll { position: relative; }
.table-scroll table { min-width: 760px; }
th:first-child, td:first-child { min-width: 220px; }
.mapping-preview { display: grid; gap: 10px; }
.mapping-preview > div { display: grid; grid-template-columns: minmax(0, 1fr) 18px minmax(0, 1fr); gap: 10px; align-items: center; }
code, .object-key { overflow-wrap: anywhere; white-space: pre-wrap; }
td small { display: block; margin-top: 4px; }
@media (max-width: 600px) { .form-grid { grid-template-columns: 1fr; } }
</style>
