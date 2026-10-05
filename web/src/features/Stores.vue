<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import EmptyState from '../components/EmptyState.vue'
import { computed, reactive, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Cloud, Database, HardDrive, Pencil, Plus, RefreshCw, ShieldCheck } from 'lucide-vue-next'
import { api, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { notify, report } from '../app/feedback'
import { secure } from '../app/security'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'

type Store = components['schemas']['StoreView']
type ManagedKey = components['schemas']['ManagedKeyView']
const stores = useQuery({ queryKey: ['stores'], queryFn: ({ signal }) => api<Store[]>('/api/stores', { signal }) })
const keys = useQuery({ queryKey: ['managed-keys'], queryFn: ({ signal }) => api<ManagedKey[]>('/api/keys', { signal }) })
const open = ref(false), editing = ref<Store | null>(null), busy = ref(false), checking = ref('')
const draft = reactive({ name: '', type: 'local', path: '/data/storage', endpoint: '', bucket: '', region: '', prefix: '', access_key: '', secret_key: '', current_key_id: '', read_concurrency: null as number | null, upload_concurrency: null as number | null, control_concurrency: null as number | null, enabled: true })
const types = computed(() => ['local', 's3'].map(value => ({ value, label: t('storeType_' + value) })))
const keyOptions = computed(() => [{ value: '', label: t('storeNoEncryption') }, ...(keys.data.value || []).filter(row => row.purpose === 'block' && !row.revoked_at).map(row => ({ value: row.id, label: `${row.name} · ${row.algorithm}` }))])

function edit(row?: Store) {
  editing.value = row || null
  Object.assign(draft, { name: row?.name || '', type: row?.type || 'local', path: row?.path || '/data/storage', endpoint: row?.endpoint || '', bucket: row?.bucket || '', region: row?.region || '', prefix: row?.prefix || '', access_key: '', secret_key: '', current_key_id: row?.current_key_id || '', read_concurrency: row?.read_concurrency ?? null, upload_concurrency: row?.upload_concurrency ?? null, control_concurrency: row?.control_concurrency ?? null, enabled: row?.enabled ?? true })
  open.value = true
}
function input() {
  const limits = { current_key_id: draft.current_key_id || null, read_concurrency: draft.read_concurrency, upload_concurrency: draft.upload_concurrency, control_concurrency: draft.control_concurrency }
  if (draft.type === 'local') return { type: 'local', path: draft.path.trim() || '/data/storage', ...limits }
  return { type: 's3', endpoint: draft.endpoint.trim(), bucket: draft.bucket.trim(), region: draft.region.trim(), prefix: draft.prefix.trim(), ...limits, ...(draft.access_key.trim() ? { access_key: draft.access_key.trim() } : {}), ...(draft.secret_key.trim() ? { secret_key: draft.secret_key.trim() } : {}) }
}
async function save() {
  busy.value = true
  try {
    const body = editing.value ? input() : { name: draft.name.trim(), ...input() }
    await secure(() => write<Store>('/api/stores' + (editing.value ? '/' + encodeURIComponent(editing.value.name) : ''), body, editing.value ? 'PUT' : 'POST'))
    open.value = false
    await queries.invalidateQueries({ queryKey: ['stores'] })
    await queries.invalidateQueries({ queryKey: ['bucket-projects'] })
    notify(t(editing.value ? 'storeSaved' : 'storeCreated'))
  } catch (error) { report(error) } finally { busy.value = false }
}
async function check(row: Store) {
  checking.value = row.name
  try { await secure(() => api<Store>('/api/stores/' + encodeURIComponent(row.name) + '/check', { method: 'POST' })); notify(t('storeChecked')) }
  catch (error) { report(error) } finally { checking.value = '' }
}
async function toggle(row: Store) {
  checking.value = row.name
  try { await secure(() => write<Store>('/api/stores/' + encodeURIComponent(row.name), { enabled: !row.enabled }, 'PUT')); await stores.refetch(); await queries.invalidateQueries({ queryKey: ['bucket-projects'] }); notify(t('storeSaved')) }
  catch (error) { report(error) } finally { checking.value = '' }
}
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('storesEyebrow') }}</p><h1>{{ t('stores') }}</h1><p>{{ t('storesHint') }}</p></div><div class="heading-actions"><button class="primary" @click="edit()"><Plus :size="17" />{{ t('createStore') }}</button><button class="icon-button" :aria-label="t('refresh')" @click="stores.refetch()"><RefreshCw :size="17" /></button></div></div>
  <ErrorNotice v-if="stores.error.value" :error="stores.error.value" />
  <div v-if="stores.isPending.value" class="skeleton-row" />
  <div v-else-if="stores.data.value?.length" class="store-grid">
    <article v-for="row in stores.data.value" :key="row.id" class="surface store-card">
      <div class="section-heading"><h2><HardDrive v-if="row.type === 'local'" :size="19" /><Cloud v-else :size="19" />{{ row.name }}</h2><span class="badge" :class="{ muted: !row.enabled }">{{ t(row.enabled ? 'storeEnabled' : 'storeDisabled') }}</span></div>
      <div class="store-type"><Database :size="16" />{{ t('storeType_' + row.type) }}</div>
      <dl class="facts"><template v-if="row.type === 'local'"><dt>{{ t('storePath') }}</dt><dd><code>{{ row.path }}</code></dd></template><template v-else><dt>{{ t('storeEndpoint') }}</dt><dd><code>{{ row.endpoint }}</code></dd><dt>{{ t('storeBucket') }}</dt><dd>{{ row.bucket }}</dd><dt>{{ t('storeRegion') }}</dt><dd>{{ row.region }}</dd><dt>{{ t('storePrefix') }}</dt><dd>{{ row.prefix || '—' }}</dd></template><dt>{{ t('storeCurrentKey') }}</dt><dd><code>{{ row.current_key_id || t('storeNoEncryption') }}</code></dd><dt>{{ t('storeReadConcurrency') }}</dt><dd>{{ row.read_concurrency || '—' }}</dd><dt>{{ t('storeUploadConcurrency') }}</dt><dd>{{ row.upload_concurrency || '—' }}</dd><dt>{{ t('storeControlConcurrency') }}</dt><dd>{{ row.control_concurrency || '—' }}</dd><dt>{{ t('updatedAt') }}</dt><dd>{{ date(row.updated_at) }}</dd></dl>
      <p class="field-help"><ShieldCheck :size="14" />{{ t('storeSecretsHint') }}</p>
      <div class="actions"><button @click="edit(row)"><Pencil :size="15" />{{ t('editStore') }}</button><button :disabled="checking === row.name" @click="check(row)"><ShieldCheck :size="15" />{{ t('checkStore') }}</button><button :disabled="checking === row.name" @click="toggle(row)">{{ t(row.enabled ? 'disableStore' : 'enableStore') }}</button></div>
    </article>
  </div>
  <EmptyState v-else :title="t('noStores')" :description="t('noStoresHint')"><button class="primary" @click="edit()"><Plus :size="17" />{{ t('createStore') }}</button></EmptyState>
  <UiDialog v-model:open="open" :title="t(editing ? 'editStore' : 'createStore')" :description="t('storeEditorHint')" :busy="busy" wide>
    <form @submit.prevent="save"><div class="store-form-grid"><label v-if="!editing">{{ t('storeName') }}<input v-model="draft.name" required maxlength="128" autocomplete="off" :disabled="busy"></label><label>{{ t('storeType') }}<UiSelect v-model="draft.type" :options="types" :label="t('storeType')" :disabled="busy" /></label><label class="store-form-wide">{{ t('storeCurrentKey') }}<UiSelect v-model="draft.current_key_id" :options="keyOptions" :label="t('storeCurrentKey')" :disabled="busy" /></label><label>{{ t('storeReadConcurrency') }}<input v-model.number="draft.read_concurrency" type="number" min="1" autocomplete="off" :placeholder="t('storeLimitHint')" :disabled="busy"></label><label>{{ t('storeUploadConcurrency') }}<input v-model.number="draft.upload_concurrency" type="number" min="1" autocomplete="off" :placeholder="t('storeLimitHint')" :disabled="busy"></label><label>{{ t('storeControlConcurrency') }}<input v-model.number="draft.control_concurrency" type="number" min="1" autocomplete="off" :placeholder="t('storeLimitHint')" :disabled="busy"></label><label v-if="draft.type === 'local'" class="store-form-wide">{{ t('storePath') }}<input v-model="draft.path" required maxlength="4096" autocomplete="off" :disabled="busy"></label><template v-else><label class="store-form-wide">{{ t('storeEndpoint') }}<input v-model="draft.endpoint" required type="url" placeholder="https://s3.example.com" autocomplete="url" :disabled="busy"></label><label>{{ t('storeBucket') }}<input v-model="draft.bucket" required autocomplete="off" :disabled="busy"></label><label>{{ t('storeRegion') }}<input v-model="draft.region" required autocomplete="off" :disabled="busy"></label><label class="store-form-wide">{{ t('storePrefix') }}<input v-model="draft.prefix" autocomplete="off" :disabled="busy"></label><label>{{ t('storeAccessKey') }}<input v-model="draft.access_key" autocomplete="off" :placeholder="editing ? t('keepCurrentSecret') : ''" :disabled="busy"></label><label>{{ t('storeSecretKey') }}<input v-model="draft.secret_key" type="password" autocomplete="new-password" :placeholder="editing ? t('keepCurrentSecret') : ''" :disabled="busy"></label></template></div><UiCheckbox v-if="editing" v-model="draft.enabled" :label="t('storeEnabled')" :disabled="busy" /><p class="field-help">{{ t(draft.type === 'local' ? 'localStoreHint' : 's3StoreHint') }}</p><div class="form-footer"><button type="button" :disabled="busy" @click="open = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy">{{ t('save') }}</button></div></form>
  </UiDialog>
</template>
