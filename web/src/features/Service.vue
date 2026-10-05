<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { TabsRoot, TabsList, TabsTrigger, TabsContent } from 'reka-ui'
import { HeartPulse, Database, Cpu, Cloud, Search, KeyRound, Copy, RefreshCw, Plus, ShieldCheck, Trash2 } from 'lucide-vue-next'
import { api, write, bytes } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { secure } from '../app/security'
import { copy, notify, report } from '../app/feedback'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'

type ManagedKey = components['schemas']['ManagedKeyView']
type RuntimeSettings = components['schemas']['RuntimeSettingsView']

const tab = ref('runtime'), search = ref(''), section = ref('')
const status = useQuery({ queryKey: ['service-status'], queryFn: ({ signal }) => api<components['schemas']['ServiceStatus']>('/api/service/status', { signal }), refetchInterval: 5000 })
const config = useQuery({ queryKey: ['service-config'], enabled: computed(() => tab.value === 'config'), queryFn: ({ signal }) => api<components['schemas']['ConfigField'][]>('/api/service/config', { signal }) })
const controlSettings = useQuery({ queryKey: ['web-settings'], enabled: computed(() => tab.value === 'config'), queryFn: ({ signal }) => api<RuntimeSettings>('/api/settings/web', { signal }) })
const runtimeSettings = useQuery({ queryKey: ['runtime-settings'], enabled: computed(() => tab.value === 'config'), queryFn: ({ signal }) => api<RuntimeSettings[]>('/api/settings', { signal }) })
const keys = useQuery({ queryKey: ['managed-keys'], enabled: computed(() => tab.value === 'tools'), queryFn: ({ signal }) => api<ManagedKey[]>('/api/keys', { signal }) })
const counter = computed(() => Object.fromEntries((status.data.value?.counters || []).map(row => [row.key, row.value])))
const queues = computed(() => Object.entries((status.data.value?.backend_queues || {}) as Record<string, { limit: number; running: number; queued: Record<string, number>; oldest_wait_seconds: number }>))
const catalog = computed(() => status.data.value?.catalog as { phase?: string; scanned?: string; last_error?: string } | undefined)
const sections = computed(() => [{ value: '', label: t('allConfigSections') }, ...[...new Set((config.data.value || []).map(row => row.key.split('.')[0]!))].map(value => ({ value, label: value }))])
const fields = computed(() => (config.data.value || []).filter(row => (!section.value || row.key.startsWith(section.value + '.')) && `${row.key} ${row.value}`.toLowerCase().includes(search.value.toLowerCase())))
const keyPurposes = computed(() => ['block', 'access', 'signing'].map(value => ({ value, label: t('keyPurpose_' + value) })))
const keyAlgorithms = computed(() => [{ value: 'aes-256-gcm', label: 'AES-256-GCM' }, { value: 'chacha20-poly1305', label: 'ChaCha20-Poly1305' }])
const keyOpen = ref(false), keyName = ref(''), keyPurpose = ref('block'), keyAlgorithm = ref('aes-256-gcm'), keyBusy = ref(false)
const actionOpen = ref(false), selectedKey = ref<ManagedKey | null>(null)
const settingsBusy = ref(false)
const runtimeOpen = ref(false), runtimeSection = ref(''), runtimeRevision = ref(0), runtimeDraft = ref('{}'), runtimeError = ref('')
const controlDraft = ref({ origin: '', secure_cookie: true, session_lifetime: '', gravatar_base_url: '' })
watch(controlSettings.data, value => { const settings = value?.value as Record<string, unknown> | undefined; if (settings && !settingsBusy.value) controlDraft.value = { origin: String(settings.origin || ''), secure_cookie: settings.secure_cookie !== false, session_lifetime: String(settings.session_lifetime || ''), gravatar_base_url: String(settings.avatar_url || '') } }, { immediate: true })

function openKeyEditor() { keyName.value = ''; keyPurpose.value = 'block'; keyAlgorithm.value = 'aes-256-gcm'; keyOpen.value = true }
async function createKey() {
  keyBusy.value = true
  try {
    await secure(() => write<ManagedKey>('/api/keys', { name: keyName.value.trim(), purpose: keyPurpose.value, ...(keyPurpose.value === 'block' ? { algorithm: keyAlgorithm.value } : {}) }))
    keyOpen.value = false
    await keys.refetch()
    notify(t('keyCreated'))
  } catch (error) { report(error) } finally { keyBusy.value = false }
}
function requestAction(row: ManagedKey) { selectedKey.value = row; actionOpen.value = true }
async function executeAction() {
  if (!selectedKey.value) return
  keyBusy.value = true
  try {
    await secure(() => api(`/api/keys/${selectedKey.value!.id}`, { method: 'DELETE' }))
    actionOpen.value = false
    await keys.refetch()
    notify(t('keyRevoked'))
  } catch (error) { report(error) } finally { keyBusy.value = false }
}
async function saveControlSettings() {
  settingsBusy.value = true
  try { await secure(() => write<RuntimeSettings>('/api/settings/web', { value: { origin: controlDraft.value.origin, secure_cookie: controlDraft.value.secure_cookie, session_lifetime: controlDraft.value.session_lifetime, avatar_url: controlDraft.value.gravatar_base_url }, revision: controlSettings.data.value?.revision }, 'PUT')); await controlSettings.refetch(); await config.refetch(); notify(t('controlSettingsSaved')) }
  catch (error) { report(error) } finally { settingsBusy.value = false }
}
function editRuntime(row: RuntimeSettings) {
  runtimeSection.value = row.section; runtimeRevision.value = row.revision; runtimeDraft.value = JSON.stringify(row.value, null, 2); runtimeError.value = ''; runtimeOpen.value = true
}
async function saveRuntime() {
  runtimeError.value = ''
  let value: unknown
  try { value = JSON.parse(runtimeDraft.value) } catch { runtimeError.value = 'Invalid JSON'; return }
  settingsBusy.value = true
  try { await secure(() => write<RuntimeSettings>(`/api/settings/${encodeURIComponent(runtimeSection.value)}`, { value, revision: runtimeRevision.value }, 'PUT')); runtimeOpen.value = false; await runtimeSettings.refetch(); await config.refetch(); notify(t('runtimeSettingsSaved')) }
  catch (error) { runtimeError.value = String(error) } finally { settingsBusy.value = false }
}
function diagnostic() { copy(JSON.stringify({ service: status.data.value, configuration: config.data.value, web_settings: controlSettings.data.value }, null, 2), t('diagnosticCopied')) }
</script>
<template><div class="page-heading"><div><p class="eyebrow">{{ t('serviceEyebrow') }}</p><h1>{{ t('service') }}</h1><p>{{ t('serviceHint') }}</p></div><div class="heading-actions"><button @click="diagnostic"><Copy :size="16" />{{ t('copyDiagnostics') }}</button><button class="icon-button" :aria-label="t('refresh')" @click="status.refetch(); config.refetch(); controlSettings.refetch(); keys.refetch()"><RefreshCw :size="17" /></button></div></div><ErrorNotice v-if="status.error.value" :error="status.error.value" />
  <TabsRoot v-model="tab"><TabsList class="tabs-list" :aria-label="t('service')"><TabsTrigger value="runtime" class="tab-trigger">{{ t('liveRuntime') }}</TabsTrigger><TabsTrigger value="config" class="tab-trigger">{{ t('runtimeConfig') }}</TabsTrigger><TabsTrigger value="tools" class="tab-trigger">{{ t('administratorTools') }}</TabsTrigger></TabsList>
    <TabsContent value="runtime"><div class="service-metrics"><section class="surface service-metric rose"><HeartPulse :size="25" /><span>{{ t('serviceMode') }}</span><strong>{{ status.data.value ? t(status.data.value.maintenance ? 'recoveryMode' : 'serviceOnline') : '—' }}</strong><small v-if="status.data.value">Mokyu {{ status.data.value.version }}</small></section><section class="surface service-metric lilac"><Cpu :size="25" /><span>{{ t('processMemory') }}</span><strong>{{ counter.rss_bytes && counter.rss_bytes !== 'null' ? bytes(Number(counter.rss_bytes)) : '—' }}</strong><small>{{ t('activeStreams', { count: counter.active_streams || '0' }) }}</small></section><section class="surface service-metric mint"><Database :size="25" /><span>{{ t('databasePool') }}</span><strong>{{ counter.db_pool_idle || '—' }} / {{ counter.db_pool_size || '—' }}</strong><small>{{ t('idleConnections') }}</small></section></div><div class="service-queues"><section v-for="[kind, queue] in queues" :key="kind" class="surface queue-card"><div class="section-heading"><h2><Cloud :size="19" />{{ t('queue_' + kind) }}</h2><span class="badge">{{ queue.running }} / {{ queue.limit }}</span></div><p class="field-help">{{ t('queueHint') }}</p><dl class="facts"><dt>{{ t('foregroundQueue') }}</dt><dd>{{ queue.queued.foreground }}</dd><dt>{{ t('uploadQueue') }}</dt><dd>{{ queue.queued.upload }}</dd><dt>{{ t('maintenanceQueue') }}</dt><dd>{{ queue.queued.maintenance }}</dd><dt>{{ t('oldestWait') }}</dt><dd>{{ t('secondsShort', { value: queue.oldest_wait_seconds }) }}</dd></dl></section></div><section class="surface recovery-card"><div class="section-heading"><h2>{{ t('indexReadiness') }}</h2></div><dl class="facts"><dt>{{ t('catalogState') }}</dt><dd>{{ t('catalog_' + (catalog?.phase || 'unknown')) }}</dd><dt>{{ t('catalogScanned') }}</dt><dd>{{ catalog?.scanned || '—' }}</dd><dt>{{ t('inventoryAsOf') }}</dt><dd>{{ status.data.value?.inventory_as_of ? date(status.data.value.inventory_as_of) : t('collectingInsights') }}</dd><dt>{{ t('startedAt') }}</dt><dd>{{ status.data.value?.started_at ? date(status.data.value.started_at) : '—' }}</dd></dl><p v-if="catalog?.last_error || status.data.value?.inventory_error" class="error">{{ catalog?.last_error || status.data.value?.inventory_error }}</p><p v-if="status.data.value?.inventory_stale" class="field-help">{{ t('snapshotStale') }}</p></section><section class="surface recovery-card"><details><summary>{{ t('runtimeCounters') }}</summary><dl class="runtime-counters"><template v-for="row in status.data.value?.counters" :key="row.key"><dt>{{ row.key }}</dt><dd>{{ row.value }}</dd></template></dl></details></section></TabsContent>
    <TabsContent value="config"><div class="info-callout"><p>{{ t('runtimeConfigHint') }}</p></div><ErrorNotice v-if="config.error.value || controlSettings.error.value || runtimeSettings.error.value" :error="config.error.value || controlSettings.error.value || runtimeSettings.error.value" /><section class="surface config-board"><div class="section-heading"><div><h2>{{ t('controlSettings') }}</h2><p class="field-help">{{ t('controlSettingsHint') }}</p></div><span v-if="controlSettings.data.value?.restart_required" class="badge">{{ t('restartRequired') }}</span></div><form v-if="controlSettings.data.value" class="control-settings-form" @submit.prevent="saveControlSettings"><div class="form-pair"><label class="field"><span>{{ t('controlOrigin') }}</span><input v-model="controlDraft.origin" required autocomplete="off"></label><label class="field"><span>{{ t('sessionLifetime') }}</span><input v-model="controlDraft.session_lifetime" required autocomplete="off"></label></div><label class="field"><span>{{ t('gravatarBaseUrl') }}</span><input v-model="controlDraft.gravatar_base_url" required autocomplete="off"></label><UiCheckbox v-model="controlDraft.secure_cookie" :label="t('secureCookie')" :disabled="settingsBusy" /><p class="field-help">{{ t('restartControlHint') }}</p><button class="primary" :disabled="settingsBusy || !controlDraft.origin || !controlDraft.session_lifetime || !controlDraft.gravatar_base_url">{{ t('saveControlSettings') }}</button></form></section><section class="surface config-board"><div class="section-heading"><div><h2>{{ t('runtimeSections') }}</h2><p class="field-help">{{ t('runtimeSectionsHint') }}</p></div></div><div class="config-fields"><article v-for="row in runtimeSettings.data.value" :key="row.section"><div><code>{{ row.section }}</code><span class="badge">{{ row.restart_required ? t('restartRequired') : t('liveSetting') }}</span></div><strong>{{ t('revisionLabel', { value: row.revision }) }}</strong><button @click="editRuntime(row)">{{ t('editRuntimeSettings') }}</button></article></div></section><section class="surface config-board"><div class="task-filters"><UiSelect v-model="section" :options="sections" :label="t('configSection')" /><label class="inline-search"><Search :size="17" /><input v-model="search" :placeholder="t('searchConfiguration')" :aria-label="t('searchConfiguration')"></label></div><div class="config-fields"><article v-for="row in fields" :key="row.key"><div><code>{{ row.key }}</code><span class="badge">{{ t('configSource_' + row.source) }}</span></div><strong>{{ row.value === '[redacted]' ? t('redactedValue') : row.value === 'null' ? t('notConfigured') : row.value }}</strong><small>{{ t(row.restart ? 'restartRequired' : 'liveSetting') }}</small></article></div><p v-if="config.data.value && !fields.length" class="field-help">{{ t('noConfigMatches') }}</p></section></TabsContent>
    <TabsContent value="tools"><section class="surface managed-keys-card"><div class="section-heading"><div><h2><KeyRound :size="19" />{{ t('managedKeys') }}</h2><p class="field-help">{{ t('managedKeysHint') }}</p></div><button class="primary" @click="openKeyEditor"><Plus :size="16" />{{ t('createKey') }}</button></div><ErrorNotice v-if="keys.error.value" :error="keys.error.value" /><div v-if="keys.isPending.value" class="skeleton-row" /><div class="managed-key-list"><article v-for="row in keys.data.value" :key="row.id" class="managed-key-row"><span class="key-symbol"><ShieldCheck :size="20" /></span><div class="managed-key-main"><strong>{{ row.name }}</strong><small>{{ t('keyPurpose_' + row.purpose) }} · {{ row.algorithm }} · {{ date(row.created_at) }}</small><code>{{ row.fingerprint }}</code></div><span class="badge" :class="{ muted: !!row.revoked_at || !row.in_use }">{{ row.revoked_at ? t('keyRevoked') : row.in_use ? t('keyInUse') : t('keyAvailable') }}</span><div class="actions"><button v-if="!row.revoked_at" class="danger-button" @click="requestAction(row)"><Trash2 :size="15" />{{ t('revokeKey') }}</button></div></article></div><p v-if="keys.data.value && !keys.data.value.length" class="field-help">{{ t('noManagedKeys') }}</p></section></TabsContent>
  </TabsRoot><UiDialog v-model:open="runtimeOpen" :title="runtimeSection" :description="t('runtimeEditorHint')" :busy="settingsBusy"><p v-if="runtimeError" class="error">{{ runtimeError }}</p><textarea v-model="runtimeDraft" rows="18" spellcheck="false" :disabled="settingsBusy"></textarea><template #footer><button :disabled="settingsBusy" @click="runtimeOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="settingsBusy" @click="saveRuntime">{{ t('save') }}</button></template></UiDialog><UiDialog v-model:open="keyOpen" :title="t('createKey')" :description="t('createKeyHint')" :busy="keyBusy"><form @submit.prevent="createKey"><label>{{ t('keyName') }}<input v-model="keyName" required maxlength="128" autocomplete="off" :disabled="keyBusy"></label><label>{{ t('keyPurpose') }}<UiSelect v-model="keyPurpose" :options="keyPurposes" :label="t('keyPurpose')" :disabled="keyBusy" /></label><label v-if="keyPurpose === 'block'">{{ t('keyAlgorithm') }}<UiSelect v-model="keyAlgorithm" :options="keyAlgorithms" :label="t('keyAlgorithm')" :disabled="keyBusy" /></label><p class="field-help">{{ t('keyMaterialPrivate') }}</p><div class="form-footer"><button type="button" :disabled="keyBusy" @click="keyOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="keyBusy || !keyName.trim()"><KeyRound :size="16" />{{ t('createKey') }}</button></div></form></UiDialog><UiDialog v-model:open="actionOpen" :title="t('revokeKey')" :description="t('revokeManagedKeyHint')" :busy="keyBusy"><p class="object-path">{{ selectedKey?.name }}</p><template #footer><button :disabled="keyBusy" @click="actionOpen = false">{{ t('cancel') }}</button><button class="danger-button" :disabled="keyBusy" @click="executeAction">{{ t('revokeKey') }}</button></template></UiDialog>
</template>
