<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Settings2, Save, Search } from 'lucide-vue-next'
import { api, write } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { notify, errorText } from '../app/feedback'
import { secure } from '../app/security'
import ErrorNotice from '../components/ErrorNotice.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import UiSwitch from '../components/ui/UiSwitch.vue'
import UiDialog from '../components/ui/UiDialog.vue'

type RuntimeSettings = components['schemas']['RuntimeSettingsView']
type ConfigField = components['schemas']['ConfigField']
type RuntimeField = { key: string; kind: 'text' | 'number' | 'storage'; example?: string; optional?: boolean }

const runtimeSettings = useQuery({ queryKey: ['runtime-settings'], queryFn: ({ signal }) => api<RuntimeSettings[]>('/api/v1/runtime-settings', { signal }) })
const controlSettings = computed(() => runtimeSettings.data.value?.find(row => row.section === 'web'))
const storages = useQuery({ queryKey: ['storages'], queryFn: ({ signal }) => api<components['schemas']['StorageView'][]>('/api/v1/storages', { signal }) })
const stagingOptions = computed(() => [{ value: '', label: t('notConfigured') }, ...(storages.data.value || []).filter(row => row.type === 's3' && row.enabled).map(row => ({ value: row.id, label: row.name }))])
const config = useQuery({ queryKey: ['service-config'], queryFn: ({ signal }) => api<ConfigField[]>('/api/v1/system/config', { signal }) })
const controlDraft = ref({ origin: '', secure_cookie: true, session_lifetime: '', gravatar_base_url: '' })
const settingsBusy = ref(false), section = ref(''), search = ref(''), settingsError = ref('')
const runtimeOpen = ref(false), runtimeSection = ref(''), runtimeRevision = ref(0), runtimeDraft = ref<Record<string, unknown>>({}), runtimeError = ref('')
const runtimeDefinitions: Record<string, { title: string; fields: RuntimeField[] }> = {
  requests: { title: 'runtimeGroupRequests', fields: [{ key: 's3_domain', kind: 'text', optional: true }, { key: 's3_region', kind: 'text' }, { key: 's3_chunk_limit', kind: 'text', example: '8MiB' }, { key: 'read_idle_timeout', kind: 'text', example: '120s' }, { key: 'upload_idle_timeout', kind: 'text', example: '120s' }] },
  uploads: { title: 'runtimeGroupUploads', fields: [{ key: 'idle_timeout', kind: 'text', example: '24h' }, { key: 'sweep_interval', kind: 'text', example: '5m' }, { key: 'client_idle_timeout', kind: 'text', example: '120s' }, { key: 'max_active_uploads', kind: 'number', optional: true }, { key: 'staging_storage_id', kind: 'storage', optional: true }, { key: 'staging_bytes_limit', kind: 'text', example: '10GiB', optional: true }] },
  metrics: { title: 'runtimeGroupMetrics', fields: [{ key: 'scope_limit', kind: 'number' }, { key: 'storage_interval', kind: 'text' }, { key: 'storage_retention', kind: 'text' }, { key: 'runtime_interval', kind: 'text' }, { key: 'runtime_retention', kind: 'text' }, { key: 'refresh_interval', kind: 'text' }, { key: 'query_timeout', kind: 'text' }, { key: 'access_flush_interval', kind: 'text' }, { key: 'access_retention', kind: 'text' }] },
  maintenance: { title: 'runtimeGroupMaintenance', fields: [{ key: 'interval', kind: 'text' }, { key: 'batch_size', kind: 'number' }, { key: 'max_duration', kind: 'text' }, { key: 'task_retention', kind: 'text' }, { key: 'audit_retention', kind: 'text' }] },
}
const runtimeFieldGroups = computed(() => { const definition = runtimeDefinitions[runtimeSection.value]; return definition ? [{ title: definition.title, fields: definition.fields.filter(field => field.key in runtimeDraft.value) }] : [] })
const editableSections = computed(() => (runtimeSettings.data.value || []).filter(row => row.section in runtimeDefinitions))
const sections = computed(() => [{ value: '', label: t('allConfigSections') }, ...[...new Set((config.data.value || []).map(row => row.key.split('.')[0]!))].map(value => ({ value, label: t('configSection_' + value) }))])
const fields = computed(() => (config.data.value || []).filter(row => (!section.value || row.key.startsWith(section.value + '.')) && `${row.key} ${fieldLabel(row.key)} ${row.value}`.toLowerCase().includes(search.value.toLowerCase())))
const visibleFields = computed(() => fields.value.filter(row => !row.key.startsWith('effective.')))
const advancedFields = computed(() => fields.value.filter(row => row.key.startsWith('effective.')))
function human(key: string) { return key.split('.').map(part => part.replaceAll('_', ' ').replace(/\b\w/g, char => char.toUpperCase())).join(' · ') }
function configKey(prefix: string, key: string) { return prefix + key.replace('.', '_') }
function fieldLabel(key: string) { const [group, name] = key.split('.'); const value = group && (group in runtimeDefinitions || group === 'web') ? t('runtimeField_' + name) : t(configKey('configField_', key)); return value.startsWith('runtimeField_') || value.startsWith('configField_') ? human(key) : value }
function fieldHelp(key: string) { const [group, name] = key.split('.'); const value = group && (group in runtimeDefinitions || group === 'web') ? t('runtimeHelp_' + name) : t(configKey('configHelp_', key)); return value.startsWith('runtimeHelp_') || value.startsWith('configHelp_') ? t('runtimeFieldHint') : value }
watch(controlSettings, row => { if (row && !settingsBusy.value && row.value && typeof row.value === 'object') controlDraft.value = { ...controlDraft.value, ...(row.value as Partial<typeof controlDraft.value>) } }, { immediate: true })

async function saveControlSettings() {
  settingsBusy.value = true; settingsError.value = ''
  try { await secure(() => write<RuntimeSettings>('/api/v1/runtime-settings/web', { value: controlDraft.value, revision: controlSettings.value?.revision }, 'PUT')); await runtimeSettings.refetch(); await config.refetch(); notify(t('controlSettingsSaved')) }
  catch (error) { settingsError.value = errorText(error) } finally { settingsBusy.value = false }
}
function editRuntime(row: RuntimeSettings) { runtimeSection.value = row.section; runtimeRevision.value = row.revision; runtimeDraft.value = row.value && typeof row.value === 'object' ? { ...(row.value as Record<string, unknown>) } : {}; runtimeError.value = ''; runtimeOpen.value = true }
function updateRuntimeField(field: RuntimeField, value: string | number | null) { runtimeDraft.value[field.key] = field.optional && value === '' ? null : value }
function inputRuntimeField(field: RuntimeField, event: Event) { const value = (event.target as HTMLInputElement).value; updateRuntimeField(field, field.kind === 'number' ? (value === '' ? null : Number(value)) : value) }
async function saveRuntime() {
  runtimeError.value = ''; settingsBusy.value = true
  try { await secure(() => write<RuntimeSettings>(`/api/v1/runtime-settings/${encodeURIComponent(runtimeSection.value)}`, { value: runtimeDraft.value, revision: runtimeRevision.value }, 'PUT')); runtimeOpen.value = false; await runtimeSettings.refetch(); await config.refetch(); notify(t('runtimeSettingsSaved')) }
  catch (error) { runtimeError.value = errorText(error) } finally { settingsBusy.value = false }
}
</script>

<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('runtimeConfigEyebrow') }}</p><h1>{{ t('runtimeConfig') }}</h1><p>{{ t('runtimeConfigHint') }}</p></div></div>
  <ErrorNotice v-if="config.error.value || runtimeSettings.error.value || storages.error.value" :error="config.error.value || runtimeSettings.error.value || storages.error.value" />
  <section class="surface config-board runtime-config-card">
    <div class="section-heading"><div><h2><Settings2 :size="19" />{{ t('controlSettings') }}</h2><p class="field-help">{{ t('controlSettingsHint') }}</p></div><span v-if="controlSettings?.restart_required" class="badge">{{ t('restartRequired') }}</span></div>
    <form v-if="controlSettings" class="runtime-form" @submit.prevent="saveControlSettings">
      <label class="field"><span>{{ t('controlOrigin') }}</span><input v-model="controlDraft.origin" required type="url" placeholder="https://mokyu.example.com" autocomplete="off" :disabled="settingsBusy"><small class="field-help">{{ t('runtimeHelp_origin') }}</small></label>
      <label class="field"><span>{{ t('sessionLifetime') }}</span><input v-model="controlDraft.session_lifetime" required placeholder="12h" autocomplete="off" :disabled="settingsBusy"><small class="field-help">{{ t('runtimeHelp_session_lifetime') }}</small></label>
      <label class="field runtime-form-wide"><span>{{ t('gravatarBaseUrl') }}</span><input v-model="controlDraft.gravatar_base_url" required type="url" autocomplete="off" :disabled="settingsBusy"><small class="field-help">{{ t('runtimeHelp_gravatar_base_url') }}</small></label>
      <UiSwitch v-model="controlDraft.secure_cookie" :label="t('secureCookie')" :disabled="settingsBusy" />
      <p class="field-help runtime-form-wide">{{ t('restartControlHint') }}</p>
      <p v-if="settingsError" class="error runtime-form-wide" role="alert">{{ settingsError }}</p>
      <div class="form-footer runtime-form-wide"><span class="field-help">{{ t('runtimeSaveHint') }}</span><button class="primary" :disabled="settingsBusy || !controlDraft.origin || !controlDraft.session_lifetime || !controlDraft.gravatar_base_url"><Save :size="16" />{{ t('saveControlSettings') }}</button></div>
    </form>
  </section>
  <section class="surface config-board runtime-config-card"><div class="section-heading"><div><h2>{{ t('runtimeSections') }}</h2><p class="field-help">{{ t('runtimeSectionsHint') }}</p></div></div><div class="runtime-section-list"><article v-for="row in editableSections" :key="row.section"><div><strong>{{ t(runtimeDefinitions[row.section]!.title) }}</strong><span class="badge">{{ row.restart_required ? t('restartRequired') : t('liveSetting') }}</span></div><span class="field-help">{{ t('revisionLabel', { value: row.revision }) }}</span><button @click="editRuntime(row)">{{ t('editRuntimeSettings') }}</button></article></div></section>
  <section class="surface config-board runtime-config-card"><div class="task-filters config-filterbar"><UiSelect v-model="section" :options="sections" :label="t('configSection')" /><label class="inline-search"><Search :size="17" /><input v-model="search" :placeholder="t('searchConfiguration')" :aria-label="t('searchConfiguration')"></label></div><div class="config-fields"><article v-for="row in visibleFields" :key="row.key"><div><strong>{{ fieldLabel(row.key) }}</strong><code class="technical-key">{{ row.key }}</code><span class="badge config-source-badge">{{ t('configSource_' + row.source) }}</span><small class="field-help">{{ fieldHelp(row.key) }}</small></div><strong>{{ row.value === '[redacted]' ? t('redactedValue') : row.value === 'null' ? t('notConfigured') : row.value }}</strong><small>{{ t(row.restart ? 'restartRequired' : 'liveSetting') }}</small></article></div><p v-if="config.data.value && !fields.length" class="field-help">{{ t('noConfigMatches') }}</p><details v-if="advancedFields.length" class="advanced-config"><summary>{{ t('advancedConfig') }} <span class="badge">{{ advancedFields.length }}</span></summary><p class="field-help">{{ t('advancedConfigHint') }}</p><div class="config-fields"><article v-for="row in advancedFields" :key="row.key"><div><strong>{{ fieldLabel(row.key) }}</strong><code class="technical-key">{{ row.key }}</code><span class="badge config-source-badge">{{ t('configSource_' + row.source) }}</span><small class="field-help">{{ fieldHelp(row.key) }}</small></div><strong>{{ row.value === '[redacted]' ? t('redactedValue') : row.value === 'null' ? t('notConfigured') : row.value }}</strong><small>{{ t(row.restart ? 'restartRequired' : 'liveSetting') }}</small></article></div></details></section>
  <UiDialog v-model:open="runtimeOpen" :title="t(runtimeDefinitions[runtimeSection]?.title || runtimeSection)" :description="t('runtimeEditorHint')" :busy="settingsBusy"><p v-if="runtimeError" class="error">{{ runtimeError }}</p><div class="runtime-editor-fields"><template v-for="group in runtimeFieldGroups" :key="group.title"><h3 class="runtime-group-title">{{ t(group.title) }}</h3><label v-for="field in group.fields" :key="field.key" class="field"><span>{{ fieldLabel(runtimeSection + '.' + field.key) }}</span><UiSelect v-if="field.kind === 'storage'" :model-value="String(runtimeDraft[field.key] || '')" :options="stagingOptions" :label="fieldLabel(runtimeSection + '.' + field.key)" :disabled="settingsBusy" @update:model-value="updateRuntimeField(field, $event)" /><input v-else :type="field.kind === 'number' ? 'number' : 'text'" :placeholder="field.example" :value="runtimeDraft[field.key] == null ? '' : String(runtimeDraft[field.key])" :disabled="settingsBusy" @input="inputRuntimeField(field, $event)"><small class="field-help">{{ fieldHelp(runtimeSection + '.' + field.key) }}</small></label></template><p v-if="!runtimeFieldGroups.length" class="field-help">{{ t('notConfigured') }}</p></div><template #footer><button :disabled="settingsBusy" @click="runtimeOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="settingsBusy" @click="saveRuntime">{{ t('save') }}</button></template></UiDialog>
</template>
