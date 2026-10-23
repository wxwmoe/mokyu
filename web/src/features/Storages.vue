<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import EmptyState from '../components/EmptyState.vue'
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { ArrowUpRight, ChevronRight, Cloud, Database, HardDrive, Pencil, Plus, RefreshCw, ShieldCheck } from 'lucide-vue-next'
import { ApiError, api, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { notify, report } from '../app/feedback'
import { secure } from '../app/security'
import { useDeployment } from '../app/cluster'
import UiSwitch from '../components/ui/UiSwitch.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import StoragePolicy from './StoragePolicy.vue'

type Storage = components['schemas']['StorageView']
type StorageProfile = components['schemas']['StorageProfile']
type ProfileKey = Exclude<keyof StorageProfile, 'storage_id' | 'revision'>
type ProfileField = { key: ProfileKey, valueType: 'text' | 'number' | 'boolean' }
type ManagedKey = components['schemas']['ManagedKeyView']
const route = useRoute(), router = useRouter()
const { status } = useDeployment()
const storages = useQuery({ queryKey: ['storages'], queryFn: ({ signal }) => api<Storage[]>('/api/v1/storages', { signal }) })
const keys = useQuery({ queryKey: ['managed-keys'], queryFn: ({ signal }) => api<ManagedKey[]>('/api/v1/keys', { signal }) })
const profileRevision = ref(1)
const single = computed(() => status.data.value?.deployment_mode === 'single')
const open = ref(false), editing = ref<Storage | null>(null), busy = ref(false), checking = ref('')
const profileDraft = ref<Record<ProfileKey, string | number | boolean | null> | null>(null)
const originalProfile = ref<typeof profileDraft.value>(null)
const form = ref<HTMLFormElement | null>(null)
const draft = reactive({ name: '', type: 's3', path: '/data/storage', endpoint: '', bucket: '', region: '', prefix: '', access_key: '', secret_key: '', block_key_id: '', enabled: true, initialize: true })
const durabilityOptions = computed(() => (draft.type === 's3' && profileDraft.value?.upload_cache ? ['durable', 'buffered', 'volatile'] : ['durable', 'buffered']).map(value => ({ value, label: t('storeDurability_' + value) })))
const types = computed(() => (single.value ? ['local', 's3'] : ['s3']).map(value => ({ value, label: t('storeType_' + value) })))
const keyOptions = computed(() => [{ value: '', label: t('storeNoEncryption') }, ...(keys.data.value || []).filter(row => row.purpose === 'block' && !row.revoked_at).map(row => ({ value: row.id, label: `${row.name} · ${row.algorithm}` }))])
const profileFields: ProfileField[] = [
  { key: 'connect_timeout', valueType: 'text' }, { key: 'request_timeout', valueType: 'text' }, { key: 'max_retries', valueType: 'number' }, { key: 'retry_timeout', valueType: 'text' }, { key: 'priority_aging', valueType: 'text' },
  { key: 'min_storage_duration', valueType: 'text' }, { key: 'unreferenced_grace', valueType: 'text' }, { key: 'deleted_chunk_retention', valueType: 'text' }, { key: 'upload_retention', valueType: 'text' }, { key: 'multipart_idle_timeout', valueType: 'text' }, { key: 'min_free_space', valueType: 'text' },
  { key: 'cache_enabled', valueType: 'boolean' }, { key: 'cache_soft_limit', valueType: 'text' }, { key: 'cache_entries', valueType: 'number' }, { key: 'upload_cache', valueType: 'boolean' }, { key: 'upload_cache_size', valueType: 'text' }, { key: 'upload_cache_overflow', valueType: 'text' },
  { key: 'compression_enabled', valueType: 'boolean' }, { key: 'compression_strategy', valueType: 'text' }, { key: 'compression_level', valueType: 'number' }, { key: 'compression_sample_level', valueType: 'number' }, { key: 'compression_min_savings_percent', valueType: 'number' }, { key: 'compression_min_savings_bytes', valueType: 'number' },
  { key: 'pack_enabled', valueType: 'boolean' }, { key: 'pack_max_size', valueType: 'text' }, { key: 'pack_upload_flush_timeout', valueType: 'text' }, { key: 'pack_interval', valueType: 'text' }, { key: 'pack_reuse_interval', valueType: 'text' }, { key: 'pack_reclaim_interval', valueType: 'text' }, { key: 'pack_repack_interval', valueType: 'text' }, { key: 'pack_repack_cooldown', valueType: 'text' }, { key: 'pack_reclaim_min_savings_bytes', valueType: 'text' },
  { key: 'pack_range_enabled', valueType: 'boolean' }, { key: 'pack_range_interval', valueType: 'text' }, { key: 'pack_range_window', valueType: 'text' }, { key: 'pack_range_min_downloads', valueType: 'number' }, { key: 'pack_range_min_savings_percent', valueType: 'number' }, { key: 'pack_range_min_savings_bytes', valueType: 'text' }, { key: 'pack_range_repack_after', valueType: 'text' }, { key: 'pack_range_repack_window', valueType: 'text' }, { key: 'pack_range_repack_retry_interval', valueType: 'text' },
  { key: 'pack_concurrency', valueType: 'number' }, { key: 'integrity_concurrency', valueType: 'number' }, { key: 'integrity_rps', valueType: 'number' }, { key: 'integrity_bandwidth', valueType: 'text' }, { key: 'integrity_batch_timeout', valueType: 'text' },
  { key: 'read_concurrency', valueType: 'number' }, { key: 'upload_concurrency', valueType: 'number' }, { key: 'control_concurrency', valueType: 'number' }, { key: 'pack_byte_budget', valueType: 'text' }, { key: 'integrity_byte_budget', valueType: 'text' }, { key: 'maintenance_concurrency', valueType: 'number' }, { key: 'maintenance_byte_budget', valueType: 'text' }, { key: 'migration_concurrency', valueType: 'number' }, { key: 'migration_byte_budget', valueType: 'text' },
]
const ioFields: ProfileField[] = [
  { key: 'public_cache_validation', valueType: 'text' }, { key: 'cors_cache_enabled', valueType: 'boolean' }, { key: 'public_cache_refresh_interval', valueType: 'text' }, { key: 'public_cache_max_age', valueType: 'text' },
  { key: 'read_source_validation', valueType: 'text' }, { key: 'source_validation_max_age', valueType: 'text' }, { key: 'read_integrity', valueType: 'text' },
  { key: 'file_delivery', valueType: 'text' }, { key: 'file_delivery_prefix', valueType: 'text' },
  { key: 'write_processing', valueType: 'text' }, { key: 'write_durability', valueType: 'text' }, { key: 'upload_cache', valueType: 'boolean' }, { key: 'upload_cache_overflow', valueType: 'text' },
  { key: 'write_flush_interval', valueType: 'text' }, { key: 'write_backlog_max_age', valueType: 'text' }, { key: 'background_scheduling', valueType: 'text' },
]
const ioKeys = new Set(ioFields.map(field => field.key))
const profileGroups = computed(() => [
  { title: 'storeGroupIo', fields: ioFields.filter(field => (field.key !== 'write_durability' || single.value) && (!field.key.startsWith('file_delivery') || draft.type === 'local') && (field.key !== 'file_delivery_prefix' || profileDraft.value?.file_delivery !== 'application')) },
  { title: 'storeGroupLifecycle', fields: profileFields.slice(0, 11) },
  { title: 'storeGroupCache', fields: profileFields.slice(11, 23).filter(field => !ioKeys.has(field.key)) },
  { title: 'storeGroupPacking', fields: profileFields.slice(23, 41) },
  { title: 'storeGroupWorker', fields: profileFields.slice(41) },
])
const profileGroupHints: Record<string, string> = {
  storeGroupIo: 'storeGroupIoHint', storeGroupLifecycle: 'storeGroupLifecycleHint', storeGroupCache: 'storeGroupCacheHint',
  storeGroupPacking: 'storeGroupPackingHint', storeGroupWorker: 'storeGroupWorkerHint',
}
function changedFields(fields: ProfileField[]) {
  return fields.filter(field => (field.key !== 'min_free_space' || draft.type === 'local')
    && profileDraft.value?.[field.key] !== originalProfile.value?.[field.key])
}
function revealInvalid(event: Event) {
  const group = event.target instanceof HTMLElement ? event.target.closest('details') : null
  if (group) group.open = true
}
async function revealChangedFields() {
  await nextTick()
  let first: HTMLElement | null = null
  for (const group of profileGroups.value) {
    for (const field of changedFields(group.fields)) {
      const element = form.value?.querySelector<HTMLElement>(`[data-profile-field="${field.key}"]`)
      const details = element?.closest('details')
      if (details) details.open = true
      first ||= element?.querySelector<HTMLElement>('input:not(:disabled), button:not(:disabled)') || null
    }
  }
  first?.focus()
  first?.scrollIntoView({ block: 'nearest' })
}
const profileLabel = (key: string) => t('profile_' + key)
const hasBlockKeys = computed(() => (keys.data.value || []).some(row => row.purpose === 'block' && !row.revoked_at))

function profileDefaults(type: string): Record<ProfileKey, string | number | boolean | null> {
  const s3 = type !== 'local'
  return {
    cdc_enabled: true, deduplication_enabled: false, write_processing: 'inline', write_durability: status.data.value?.deployment_mode === 'single' ? 'durable' : null,
    public_cache_validation: 'strict', cors_cache_enabled: false, public_cache_refresh_interval: '1s', public_cache_max_age: '5s', read_source_validation: 'strict', source_validation_max_age: '5s', read_integrity: 'inline', file_delivery: 'application', file_delivery_prefix: null,
    write_flush_interval: '0s', write_backlog_max_age: '300s', background_scheduling: 'balanced',
    connect_timeout: '5s', request_timeout: '30s', max_retries: 3, retry_timeout: '120s', priority_aging: '30s',
    min_storage_duration: '0s', unreferenced_grace: '48h', deleted_chunk_retention: '7d', upload_retention: '24h', multipart_idle_timeout: '24h', min_free_space: s3 ? null : '1GiB', cache_enabled: s3,
    cache_soft_limit: null, cache_entries: null, upload_cache: s3, upload_cache_size: null, upload_cache_overflow: 'fallback', compression_enabled: s3,
    compression_strategy: 'always', compression_level: 6, compression_sample_level: 3, compression_min_savings_percent: 2,
    compression_min_savings_bytes: 256, pack_enabled: s3, pack_max_size: '32MiB', pack_upload_flush_timeout: '60s', pack_interval: '1h',
    pack_reuse_interval: '5m', pack_reclaim_interval: '30m', pack_repack_interval: '24h', pack_repack_cooldown: '1h',
    pack_reclaim_min_savings_bytes: '4MiB', pack_range_enabled: s3, pack_range_interval: '15m', pack_range_window: '24h',
    pack_range_min_downloads: 8, pack_range_min_savings_percent: 50, pack_range_min_savings_bytes: '64MiB', pack_range_repack_after: '30d',
    pack_range_repack_window: '7d', pack_range_repack_retry_interval: '7d', pack_concurrency: 1, integrity_concurrency: 1,
    integrity_rps: null, integrity_bandwidth: null, integrity_batch_timeout: '30s', read_concurrency: null, upload_concurrency: null,
    control_concurrency: null, pack_byte_budget: null, integrity_byte_budget: null, maintenance_concurrency: null,
    maintenance_byte_budget: null, migration_concurrency: null, migration_byte_budget: null,
  } as Record<ProfileKey, string | number | boolean | null>
}
function resetConnection() {
  if (draft.type === 'local') {
    Object.assign(draft, { path: '/data/storage', endpoint: '', bucket: '', region: '', prefix: '', access_key: '', secret_key: '' })
  } else {
    Object.assign(draft, { endpoint: 'https://s3.amazonaws.com', bucket: '', region: 'us-east-1', prefix: '', access_key: '', secret_key: '' })
  }
}
async function edit(row?: Storage) {
  editing.value = row || null
  profileRevision.value = row?.profile_revision || 1
  Object.assign(draft, { name: row?.name || '', type: row?.type || 's3', path: row?.path || '/data/storage', endpoint: row?.endpoint || '', bucket: row?.bucket || '', region: row?.region || '', prefix: row?.prefix || '', access_key: '', secret_key: '', block_key_id: row?.block_key_id || '', enabled: row?.enabled ?? true, initialize: true })
  profileDraft.value = profileDefaults(draft.type)
  originalProfile.value = { ...profileDraft.value }
  open.value = true
  if (row) {
    busy.value = true
    try {
      const profile = await api<StorageProfile>(`/api/v1/storages/${encodeURIComponent(row.name)}/profile`)
      const { storage_id: _storageId, revision, ...editable } = profile
      profileRevision.value = revision
      profileDraft.value = editable as Record<ProfileKey, string | number | boolean | null>
      originalProfile.value = { ...profileDraft.value }
    } catch (error) {
      report(error)
      open.value = false
    } finally { busy.value = false }
  }
}
watch(() => draft.type, value => {
  if (!profileDraft.value) return
  if (!editing.value) {
    profileDraft.value = profileDefaults(value)
    originalProfile.value = { ...profileDraft.value }
  }
  else {
    profileDraft.value.min_free_space = value === 'local' ? profileDraft.value.min_free_space || '1GiB' : null
    if (value !== 'local') Object.assign(profileDraft.value, { file_delivery: 'application', file_delivery_prefix: null })
    if (value === 'local') Object.assign(profileDraft.value, { write_durability: profileDraft.value.write_durability === 'volatile' ? 'durable' : profileDraft.value.write_durability, cache_enabled: false, upload_cache: false, pack_enabled: false, pack_range_enabled: false })
  }
})
watch([() => route.query.edit, () => storages.data.value], async ([name, rows]) => {
  const row = rows?.find(item => item.name === String(name || ''))
  if (row && !open.value) await edit(row)
}, { immediate: true })
watch(open, value => {
  if (!value && route.query.edit) void router.replace({ query: { ...route.query, edit: undefined } })
})
function input() {
  const limits = { block_key_id: draft.block_key_id || null, prefix: draft.prefix.trim() }
  if (draft.type === 'local') return { type: 'local', path: draft.path.trim() || '/data/storage', ...limits }
  return { type: 's3', endpoint: draft.endpoint.trim(), bucket: draft.bucket.trim(), region: draft.region.trim(), ...limits, ...(draft.access_key.trim() ? { access_key: draft.access_key.trim() } : {}), ...(draft.secret_key.trim() ? { secret_key: draft.secret_key.trim() } : {}) }
}
function profileInput(field: ProfileField, event: Event) {
  if (!profileDraft.value) return
  const target = event.target as HTMLInputElement
  profileDraft.value[field.key] = field.valueType === 'boolean' ? target.checked : field.valueType === 'number' ? (target.value ? Number(target.value) : null) : target.value
}
function setDelivery(value: string) {
  if (!profileDraft.value) return
  profileDraft.value.file_delivery = value
  profileDraft.value.file_delivery_prefix = value === 'application' ? null : ''
}
function setProfileBoolean(key: ProfileKey, value: boolean) {
  if (!profileDraft.value) return
  profileDraft.value[key] = value
  if (key === 'upload_cache' && !value && profileDraft.value.write_durability === 'volatile') profileDraft.value.write_durability = 'durable'
  if (key === 'cdc_enabled' && !value) {
    Object.assign(profileDraft.value, { compression_enabled: false, pack_enabled: false, pack_range_enabled: false })
    draft.block_key_id = ''
  }
}
function profileDisabled(key: string) {
  return (draft.type === 'local' && (key.startsWith('cache_') || key.startsWith('pack_') || key.startsWith('upload_cache')))
    || (key === 'upload_cache_overflow' && !profileDraft.value?.upload_cache)
    || (profileDraft.value?.cdc_enabled === false && (key.startsWith('compression_') || key.startsWith('pack_')))
}
async function save() {
  busy.value = true
  try {
    const profile = profileDraft.value
      ? {
          storage_id: editing.value?.id || '00000000-0000-0000-0000-000000000000',
          revision: profileRevision.value,
          ...profileDraft.value,
        }
      : undefined
    const body = editing.value
      ? { ...input(), enabled: draft.enabled, profile }
      : { name: draft.name.trim(), initialize: draft.initialize, profile, ...input() }
    await secure(() => write<Storage>('/api/v1/storages' + (editing.value ? '/' + encodeURIComponent(editing.value.name) : ''), body, editing.value ? 'PUT' : 'POST'))
    open.value = false
    await queries.invalidateQueries({ queryKey: ['storages'] })
    await queries.invalidateQueries({ queryKey: ['bucket-projects'] })
    notify(t(editing.value && editing.value.type !== draft.type ? 'storeRelocated' : editing.value ? 'storeUpdated' : 'storeCreated'))
  } catch (error) {
    busy.value = false
    // Validation replies have no field location; reveal changed groups for correction.
    if (error instanceof ApiError && [400, 422].includes(error.status)) await revealChangedFields()
    report(error)
  } finally { busy.value = false }
}
async function toggle(row: Storage) {
  checking.value = row.name
  try { const result = await secure(() => write<Storage>('/api/v1/storages/' + encodeURIComponent(row.name), { enabled: !row.enabled }, 'PUT')); await storages.refetch(); await queries.invalidateQueries({ queryKey: ['bucket-projects'] }); notify(t(result.enabled ? 'storeEnabledNotice' : 'storeDisabledNotice')) }
  catch (error) { report(error) } finally { checking.value = '' }
}
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('storesEyebrow') }}</p><h1>{{ t('storages') }}</h1><p>{{ t('storesHint') }}</p></div><div class="heading-actions"><button class="primary" @click="edit()"><Plus :size="17" />{{ t('createStore') }}</button><button class="icon-button" :aria-label="t('refresh')" @click="storages.refetch()"><RefreshCw :size="17" /></button></div></div>
  <ErrorNotice v-if="storages.error.value" :error="storages.error.value" />
  <div v-if="storages.isPending.value" class="skeleton-row" />
  <div v-else-if="storages.data.value?.length" class="store-grid">
    <article v-for="row in storages.data.value" :key="row.id" class="surface store-card">
      <div class="section-heading"><h2><HardDrive v-if="row.type === 'local'" :size="19" /><Cloud v-else :size="19" />{{ row.name }}</h2><span class="badge" :class="{ muted: !row.enabled }">{{ t(row.enabled ? 'storeEnabled' : 'storeDisabled') }}</span></div>
      <div class="store-type"><Database :size="16" />{{ t('storeType_' + row.type) }}</div>
      <dl class="facts"><template v-if="row.type === 'local'"><dt>{{ t('storePath') }}</dt><dd><code>{{ row.path }}</code></dd></template><template v-else><dt>{{ t('storeEndpoint') }}</dt><dd><code>{{ row.endpoint }}</code></dd><dt>{{ t('storeBucket') }}</dt><dd>{{ row.bucket }}</dd><dt>{{ t('storeRegion') }}</dt><dd>{{ row.region }}</dd><dt>{{ t('storePrefix') }}</dt><dd>{{ row.prefix || '—' }}</dd></template><dt>{{ t('storeCurrentKey') }}</dt><dd><code>{{ row.block_key_id || t('storeNoEncryption') }}</code></dd><dt>{{ t('updatedAt') }}</dt><dd>{{ date(row.updated_at) }}</dd></dl>
      <p class="store-secret-note"><ShieldCheck :size="15" /><span>{{ t('storeSecretsHint') }}</span></p>
      <div class="actions"><RouterLink class="button" :to="'/storages/' + encodeURIComponent(row.name)"><ArrowUpRight :size="15" />{{ t('storeDetails') }}</RouterLink><button @click="edit(row)"><Pencil :size="15" />{{ t('editStore') }}</button><button :disabled="checking === row.name" @click="toggle(row)">{{ t(row.enabled ? 'disableStore' : 'enableStore') }}</button></div>
    </article>
  </div>
  <EmptyState v-else :title="t('noStores')" :description="t('noStoresHint')" />
  <UiDialog v-model:open="open" :title="t(editing ? 'editStore' : 'createStore')" :description="t('storeEditorHint')" :busy="busy" wide>
    <form ref="form" @submit.prevent="save" @invalid.capture="revealInvalid">
      <p class="store-draft-note"><Pencil :size="15" />{{ t('storeDraftHint') }}</p>
      <div class="store-form-grid">
        <div class="store-form-wide section-heading"><div><h3>{{ t('storeConnection') }}</h3><p class="field-help">{{ t('storeConnectionHint') }}</p></div></div>
        <label v-if="!editing">{{ t('storeName') }}<input v-model="draft.name" required maxlength="128" autocomplete="off" :disabled="busy"></label>
        <label class="store-type-field">{{ t('storeType') }}<UiSelect v-model="draft.type" :options="types" :label="t('storeType')" :disabled="busy" /></label>
        <p v-if="editing && editing.type !== draft.type" class="store-form-wide field-help">{{ t('storeRelocationHint') }}</p>
        <div class="store-form-wide preset-actions"><span class="field-help">{{ t('storeConnectionDefaultsHint') }}</span><button type="button" :disabled="busy" @click="resetConnection">{{ t('storeConnectionDefaults') }}</button></div>
        <label v-if="draft.type === 'local'" class="store-form-wide">{{ t('storePath') }}<input v-model="draft.path" required maxlength="4096" autocomplete="off" :disabled="busy"></label>
        <template v-else>
          <label>{{ t('storeEndpoint') }}<input v-model="draft.endpoint" required type="url" placeholder="https://s3.example.com" autocomplete="url" :disabled="busy"></label>
          <label>{{ t('storeBucket') }}<input v-model="draft.bucket" required autocomplete="off" :disabled="busy"></label>
          <label>{{ t('storeRegion') }}<input v-model="draft.region" required autocomplete="off" :disabled="busy"></label>
          <label>{{ t('storeAccessKey') }}<input v-model="draft.access_key" autocomplete="off" :placeholder="editing ? t('keepCurrentSecret') : ''" :disabled="busy"></label>
          <label>{{ t('storeSecretKey') }}<input v-model="draft.secret_key" type="password" autocomplete="new-password" :placeholder="editing ? t('keepCurrentSecret') : ''" :disabled="busy"></label>
        </template>
        <label class="store-form-wide">{{ t('storePrefix') }}<input v-model="draft.prefix" autocomplete="off" :disabled="busy"></label>
        <div class="store-form-wide section-heading"><div><h3>{{ t('storeChunkPolicy') }}</h3><p class="field-help">{{ t('storeChunkPolicyHint') }}</p></div></div>
        <div v-if="profileDraft" class="store-form-wide"><UiSwitch :model-value="Boolean(profileDraft.cdc_enabled)" :label="t('profile_cdc_enabled')" :disabled="busy" @update:model-value="setProfileBoolean('cdc_enabled', $event)" /><p class="field-help">{{ t('storeCdcHint') }}</p></div>
        <div v-if="profileDraft" class="store-form-wide"><UiSwitch :model-value="Boolean(profileDraft.cdc_enabled || profileDraft.deduplication_enabled)" :label="t('profile_deduplication_enabled')" :disabled="busy || Boolean(profileDraft.cdc_enabled)" @update:model-value="setProfileBoolean('deduplication_enabled', $event)" /><p class="field-help">{{ t(profileDraft.cdc_enabled ? 'storeChunkDedupHint' : 'storeRawDedupHint') }}</p></div>
        <label class="store-form-wide">{{ t('storeCurrentKey') }}<UiSelect v-model="draft.block_key_id" :options="keyOptions" :label="t('storeCurrentKey')" :disabled="busy || profileDraft?.cdc_enabled === false" /></label>
      <p class="store-form-wide field-help">{{ t(hasBlockKeys ? 'storeKeyHint' : 'storeNoKeyHint') }} <a href="/credentials?tab=managed&create=block" target="_blank" rel="noreferrer">{{ t('manageKeys') }}</a></p>
        <template v-if="profileDraft">
          <StoragePolicy v-model="profileDraft" :storage-type="draft.type" :existing-type="editing?.type" :name="editing?.name" :storage-id="editing?.id || '00000000-0000-0000-0000-000000000000'" :revision="profileRevision" :disabled="busy" />
          <details v-for="group in profileGroups" :key="group.title" class="store-form-wide profile-group" :data-profile-group="group.title">
            <summary class="profile-group-title"><strong>{{ t(single && group.title === 'storeGroupWorker' ? 'storeGroupWorkerSingle' : group.title) }}</strong><span v-if="changedFields(group.fields).length" class="profile-changes">{{ t('storeChangedFields', { count: changedFields(group.fields).length }) }}</span><ChevronRight :size="16" aria-hidden="true" /></summary>
            <p class="field-help">{{ t(single && group.title === 'storeGroupWorker' ? 'storeGroupWorkerHintSingle' : profileGroupHints[group.title]!) }}</p>
            <div class="profile-fields">
              <div v-for="field in group.fields.filter(field => field.key !== 'min_free_space' || draft.type === 'local')" :key="field.key" :data-profile-field="field.key" :class="field.valueType === 'boolean' ? 'profile-field profile-toggle' : 'profile-field'">
                <label v-if="field.key === 'cors_cache_enabled'"><span>{{ profileLabel(field.key) }}</span><UiSwitch :model-value="profileDraft.public_cache_validation === 'background' || Boolean(profileDraft.cors_cache_enabled)" :label="t(profileDraft.public_cache_validation === 'background' || profileDraft.cors_cache_enabled ? 'enabled' : 'disabled')" :disabled="busy || profileDraft.public_cache_validation === 'background'" @update:model-value="setProfileBoolean(field.key, $event)" /><small class="field-help">{{ t('profileHelp_cors_cache_enabled') }}</small></label>
                <template v-else-if="field.valueType === 'boolean'"><span>{{ profileLabel(field.key) }}</span><UiSwitch :model-value="Boolean(profileDraft[field.key])" :aria-label="profileLabel(field.key)" :label="profileDisabled(field.key) ? t('storeOptionUnused') : t(Boolean(profileDraft[field.key]) ? 'enabled' : 'disabled')" :disabled="busy || profileDisabled(field.key)" @update:model-value="setProfileBoolean(field.key, $event)" /></template>
                <label v-else-if="field.key === 'file_delivery'"><span>{{ profileLabel(field.key) }}</span><UiSelect :model-value="String(profileDraft.file_delivery)" :options="['application', 'x_accel_redirect', 'x_sendfile'].map(value => ({ value, label: t('storeDelivery_' + value) }))" :label="profileLabel(field.key)" :disabled="busy" @update:model-value="setDelivery" /><small class="field-help">{{ t('storeDeliveryHint') }}</small></label>
                <label v-else-if="field.key === 'file_delivery_prefix'"><span>{{ profileLabel(field.key) }}</span><input :aria-label="profileLabel(field.key)" :value="profileDraft.file_delivery_prefix ?? ''" :placeholder="profileDraft.file_delivery === 'x_accel_redirect' ? '/_mokyu_raw/' : '/srv/mokyu/raw/'" :disabled="busy" @input="profileInput(field, $event)"><small class="field-help">{{ t('storeDeliveryPrefixHint') }}</small></label>
                <label v-else-if="field.key === 'write_processing'"><span>{{ profileLabel(field.key) }}</span><UiSelect :model-value="String(profileDraft.write_processing)" :options="['inline', 'background'].map(value => ({ value, label: t('storeProcessing_' + value) }))" :label="profileLabel(field.key)" :disabled="busy" @update:model-value="profileDraft.write_processing = $event" /><small class="field-help">{{ t('storeProcessingHint') }}</small></label>
                <label v-else-if="field.key === 'write_durability'"><span>{{ profileLabel(field.key) }}</span><UiSelect :model-value="String(profileDraft.write_durability ?? 'durable')" :options="durabilityOptions" :label="profileLabel(field.key)" :disabled="busy" aria-describedby="storage-durability-hint" @update:model-value="profileDraft.write_durability = $event" /><small id="storage-durability-hint" class="field-help">{{ t(profileDraft.write_durability === 'volatile' ? 'storeMemoryHint' : 'storeDurabilityHint') }}</small></label>
                <label v-else-if="field.key === 'upload_cache_overflow'"><span>{{ profileLabel(field.key) }}</span><UiSelect :model-value="String(profileDraft.upload_cache_overflow)" :options="['fallback', 'reject'].map(value => ({ value, label: t('storeOverflow_' + value) }))" :label="profileLabel(field.key)" :disabled="busy || profileDisabled(field.key)" aria-describedby="storage-upload-overflow-hint" @update:model-value="profileDraft.upload_cache_overflow = $event" /><small id="storage-upload-overflow-hint" class="field-help">{{ t('storeOverflowHint') }}</small></label>
                <label v-else-if="field.key === 'read_integrity'"><span>{{ profileLabel(field.key) }}</span><UiSelect :model-value="String(profileDraft[field.key])" :options="['inline', 'background', 'off'].map(value => ({ value, label: t('storeReadIntegrity_' + value) }))" :label="profileLabel(field.key)" :disabled="busy" @update:model-value="profileDraft[field.key] = $event" /><small class="field-help">{{ t('profileHelp_read_integrity') }}</small></label>
                <label v-else-if="['public_cache_validation', 'read_source_validation'].includes(field.key)"><span>{{ profileLabel(field.key) }}</span><UiSelect :model-value="String(profileDraft[field.key])" :options="['strict', 'background'].map(value => ({ value, label: t('storeReadValidation_' + value) }))" :label="profileLabel(field.key)" :disabled="busy" @update:model-value="profileDraft[field.key] = $event" /><small v-if="field.key === 'read_source_validation'" class="field-help">{{ t(single ? 'profileHelp_read_source_validationSingle' : 'profileHelp_read_source_validation') }}</small></label>
                <label v-else-if="field.key === 'background_scheduling'"><span>{{ profileLabel(field.key) }}</span><UiSelect :model-value="String(profileDraft.background_scheduling)" :options="['balanced', 'foreground'].map(value => ({ value, label: t('storeScheduling_' + value) }))" :label="profileLabel(field.key)" :disabled="busy" aria-describedby="storage-scheduling-hint" @update:model-value="profileDraft.background_scheduling = $event" /><small id="storage-scheduling-hint" class="field-help">{{ t('storeSchedulingHint') }}</small></label>
                <label v-else><span class="profile-field-label">{{ profileLabel(field.key) }}<small v-if="profileDisabled(field.key)" class="profile-option-note">{{ t('storeOptionUnused') }}</small></span><input :aria-label="profileLabel(field.key)" :aria-describedby="['multipart_idle_timeout', 'min_free_space', 'write_flush_interval', 'write_backlog_max_age'].includes(field.key) ? 'storage-profile-' + field.key + '-hint' : undefined" :type="field.valueType === 'number' ? 'number' : 'text'" :min="field.key.endsWith('_concurrency') ? 1 : undefined" :value="profileDraft[field.key] ?? ''" :disabled="busy || profileDisabled(field.key)" @input="profileInput(field, $event)"><small v-if="['multipart_idle_timeout', 'min_free_space', 'write_flush_interval', 'write_backlog_max_age'].includes(field.key)" :id="'storage-profile-' + field.key + '-hint'" class="field-help">{{ t('profileHelp_' + field.key) }}</small></label>
              </div>
            </div>
          </details>
        </template>
      </div>
      <p class="store-secret-note"><ShieldCheck :size="15" /><span>{{ t('managedKeysHint') }} <a href="/credentials?tab=managed" target="_blank" rel="noreferrer">{{ t('manageKeys') }}</a></span></p>
      <UiSwitch v-if="!editing" v-model="draft.initialize" class="store-form-switch" :label="t('initializeStore')" :disabled="busy" />
      <UiSwitch v-if="editing" v-model="draft.enabled" class="store-form-switch" :label="t('storeEnabled')" :disabled="busy" />
      <p class="field-help">{{ t(draft.type === 'local' ? 'localStoreHint' : 's3StoreHint') }}</p>
      <div class="form-footer"><button type="button" :disabled="busy" @click="open = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy">{{ t('save') }}</button></div>
    </form>
  </UiDialog>
</template>
