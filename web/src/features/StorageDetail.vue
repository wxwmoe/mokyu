<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Cloud, Database, HardDrive, Pencil, RefreshCw, ShieldCheck, WandSparkles } from 'lucide-vue-next'
import ErrorNotice from '../components/ErrorNotice.vue'
import EmptyState from '../components/EmptyState.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import { api, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { date, t } from '../app/i18n'
import { errorText, notify, report } from '../app/feedback'
import { secure } from '../app/security'

type Storage = components['schemas']['StorageView']
type StorageProfile = components['schemas']['StorageProfile']
const route = useRoute(), router = useRouter()
const name = computed(() => String(route.params.name || ''))
const storages = useQuery({ queryKey: ['storages'], queryFn: ({ signal }) => api<Storage[]>('/api/v1/storages', { signal }) })
const storage = computed(() => storages.data.value?.find(row => row.name === name.value))
const profile = useQuery({
  queryKey: computed(() => ['storage-profile', name.value]),
  enabled: computed(() => !!storage.value),
  queryFn: ({ signal }) => api<StorageProfile>(`/api/v1/storages/${encodeURIComponent(name.value)}/profile`, { signal }),
})
type RewritePreview = {
  preview?: {
    target_algorithm?: string | null
    compression_strategy?: string | null
    loose_chunks?: number
    packed_chunks?: number
    effect?: string | null
  }
  confirmation?: string
}
const checking = ref(false), checkOpen = ref(false), checkPhrase = ref('')
const rewriteOpen = ref(false), rewritePreview = ref<RewritePreview | null>(null)
const rewritePhrase = ref(''), rewriteBusy = ref(false), rewriteError = ref('')
const checkError = ref('')
const profileSummary = computed(() => {
  const value = profile.data.value
  if (!value) return []
  return [
    [t('storeCurrentKey'), storage.value?.block_key_id || t('storeNoEncryption')],
    [t('storeReadConcurrency'), value.read_concurrency ?? t('storeLimitHint')],
    [t('storeUploadConcurrency'), value.upload_concurrency ?? t('storeLimitHint')],
    [t('storeControlConcurrency'), value.control_concurrency ?? t('storeLimitHint')],
    [t('storeCompression'), value.compression_enabled ? `${value.compression_strategy} · ${value.compression_level}` : t('disabled')],
    [t('storePacking'), value.pack_enabled ? t('enabled') : t('disabled')],
  ]
})
async function refresh() {
  await Promise.all([storages.refetch(), profile.refetch()])
}
function edit() {
  if (storage.value) void router.push({ path: '/storages', query: { edit: storage.value.name } })
}
async function toggle() {
  if (!storage.value) return
  checking.value = true
  try {
    await secure(() => write<Storage>(`/api/v1/storages/${encodeURIComponent(storage.value!.name)}`, { enabled: !storage.value!.enabled }, 'PUT'))
    await queries.invalidateQueries({ queryKey: ['storages'] })
    notify(t('storeSaved'))
  } catch (error) { report(error) } finally { checking.value = false }
}
async function check() {
  if (!storage.value) return
  checkPhrase.value = ''
  checkError.value = ''
  checkOpen.value = true
}
async function confirmCheck() {
  if (!storage.value || checkPhrase.value !== storage.value.name) return
  checking.value = true
  try {
    await secure(() => api<Storage>(`/api/v1/storages/${encodeURIComponent(storage.value!.name)}/check`, { method: 'POST' }))
    checkOpen.value = false
    notify(t('storeChecked'))
  } catch (error) { checkError.value = errorText(error) } finally { checking.value = false }
}
async function previewRewrite() {
  if (!storage.value) return
  rewriteOpen.value = true; rewritePreview.value = null; rewritePhrase.value = ''; rewriteError.value = ''; rewriteBusy.value = true
  try {
    rewritePreview.value = await secure(() => write<RewritePreview>(`/api/v1/storages/${encodeURIComponent(storage.value!.name)}/rewrite`, { action: 'preview', target_key: storage.value!.block_key_id }))
  } catch (error) { rewriteError.value = errorText(error) } finally { rewriteBusy.value = false }
}
async function startRewrite() {
  if (!storage.value || !rewritePreview.value) return
  rewriteBusy.value = true; rewriteError.value = ''
  try {
    await secure(() => write(`/api/v1/storages/${encodeURIComponent(storage.value!.name)}/rewrite`, { action: 'start', target_key: storage.value!.block_key_id, confirmation: rewritePhrase.value }))
    rewriteOpen.value = false
    notify(t('storeRewriteStarted'))
  } catch (error) { rewriteError.value = errorText(error) } finally { rewriteBusy.value = false }
}
</script>

<template>
  <div v-if="storage" class="page-heading storage-detail-heading">
    <div><RouterLink class="back-link" to="/storages"><ArrowLeft :size="15" />{{ t('backToStorages') }}</RouterLink><p class="eyebrow">{{ t('storeDetailsEyebrow') }}</p><h1>{{ storage.name }}</h1><p>{{ t('storeDetailsHint') }}</p></div>
    <div class="heading-actions"><button class="icon-button" :aria-label="t('refresh')" @click="refresh"><RefreshCw :size="17" /></button><button @click="edit"><Pencil :size="16" />{{ t('editStore') }}</button><button :disabled="checking" @click="toggle">{{ t(storage.enabled ? 'disableStore' : 'enableStore') }}</button></div>
  </div>
  <ErrorNotice v-if="storages.error.value || profile.error.value" :error="storages.error.value || profile.error.value" />
  <EmptyState v-if="!storages.isPending.value && !storage" :title="t('storeNotFound')" :description="t('storeNotFoundHint')"><RouterLink class="button primary" to="/storages"><ArrowLeft :size="16" />{{ t('backToStorages') }}</RouterLink></EmptyState>
  <template v-else-if="storage">
    <div class="storage-detail-grid">
      <section class="surface storage-detail-card"><div class="section-heading"><h2><Cloud v-if="storage.type === 's3'" :size="19" /><HardDrive v-else :size="19" />{{ t('storeConnection') }}</h2><span class="badge" :class="{ muted: !storage.enabled }">{{ t(storage.enabled ? 'storeEnabled' : 'storeDisabled') }}</span></div><div class="store-type"><Database :size="16" />{{ t('storeType_' + storage.type) }}</div><dl class="facts"><template v-if="storage.type === 'local'"><dt>{{ t('storePath') }}</dt><dd><code>{{ storage.path }}</code></dd></template><template v-else><dt>{{ t('storeEndpoint') }}</dt><dd><code>{{ storage.endpoint }}</code></dd><dt>{{ t('storeBucket') }}</dt><dd>{{ storage.bucket }}</dd><dt>{{ t('storeRegion') }}</dt><dd>{{ storage.region }}</dd><dt>{{ t('storePrefix') }}</dt><dd>{{ storage.prefix || '—' }}</dd></template><dt>{{ t('updatedAt') }}</dt><dd>{{ date(storage.updated_at) }}</dd></dl><p class="store-secret-note"><ShieldCheck :size="15" /><span>{{ t('storeSecretsHint') }}</span></p></section>
      <section class="surface storage-detail-card"><div class="section-heading"><h2><ShieldCheck :size="19" />{{ t('storeProfile') }}</h2><span class="badge">v{{ profile.data.value?.revision || storage.profile_revision }}</span></div><p class="field-help">{{ t('storeProfileHint') }}</p><div v-if="profile.isPending.value" class="skeleton-row" /><dl v-else class="facts"><template v-for="item in profileSummary" :key="String(item[0])"><dt>{{ item[0] }}</dt><dd><code v-if="typeof item[1] === 'string' && String(item[1]).includes('-')">{{ item[1] }}</code><span v-else>{{ item[1] }}</span></dd></template></dl><button class="button" @click="edit"><Pencil :size="15" />{{ t('editStore') }}</button></section>
    </div>
    <section class="surface storage-maintenance"><div class="section-heading"><div><h2><WandSparkles :size="19" />{{ t('storeMaintenance') }}</h2><p>{{ t('storeMaintenanceHint') }}</p></div><RouterLink class="button" :to="{ path: '/tasks', query: { storage: storage.id } }">{{ t('viewTasks') }}</RouterLink></div><div class="maintenance-notice"><ShieldCheck :size="18" /><p>{{ t('storeMaintenanceWarning') }}</p></div><div class="actions"><button :disabled="checking" @click="check"><ShieldCheck :size="15" />{{ t('checkStore') }}</button><button :disabled="storage.type === 'local' || checking" @click="previewRewrite"><WandSparkles :size="15" />{{ t('storeRewrite') }}</button></div><p v-if="storage.type === 'local'" class="field-help">{{ t('storeRewriteLocalHint') }}</p></section>
  </template>
  <UiDialog v-model:open="checkOpen" :title="t('checkStore')" :description="t('storeCheckConfirmHint')" :busy="checking"><p v-if="checkError" class="error" role="alert">{{ checkError }}</p><form @submit.prevent="confirmCheck"><label>{{ t('typeConfirmation', { value: storage?.name || '' }) }}<input v-model="checkPhrase" autocomplete="off" spellcheck="false" :disabled="checking"></label><div class="form-footer"><button type="button" :disabled="checking" @click="checkOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="checking || checkPhrase !== storage?.name">{{ t('checkStore') }}</button></div></form></UiDialog>
  <UiDialog v-model:open="rewriteOpen" :title="t('storeRewrite')" :description="t('storeRewriteHint')" :busy="rewriteBusy"><p v-if="rewriteError" class="error" role="alert">{{ rewriteError }}</p><dl v-if="rewritePreview?.preview" class="facts rewrite-preview"><dt>{{ t('storeTargetAlgorithm') }}</dt><dd>{{ rewritePreview.preview.target_algorithm || t('storeNoEncryption') }}</dd><dt>{{ t('storeCompression') }}</dt><dd>{{ rewritePreview.preview.compression_strategy || t('disabled') }}</dd><dt>{{ t('storeLooseChunks') }}</dt><dd>{{ rewritePreview.preview.loose_chunks ?? '—' }}</dd><dt>{{ t('storePackedChunks') }}</dt><dd>{{ rewritePreview.preview.packed_chunks ?? '—' }}</dd></dl><p v-if="rewritePreview?.preview?.effect" class="field-help">{{ t('storeRewriteEffect') }}</p><form v-if="rewritePreview?.confirmation" @submit.prevent="startRewrite"><label>{{ t('typeConfirmation', { value: rewritePreview.confirmation }) }}<input v-model="rewritePhrase" autocomplete="off" spellcheck="false" :disabled="rewriteBusy"></label><div class="form-footer"><button type="button" :disabled="rewriteBusy" @click="rewriteOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="rewriteBusy || rewritePhrase !== rewritePreview.confirmation">{{ t('startTask') }}</button></div></form></UiDialog>
</template>
