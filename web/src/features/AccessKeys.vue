<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { onBeforeRouteLeave, useRoute } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import { KeyRound, Plus, Copy, ShieldCheck, RefreshCw, Trash2, ChevronRight } from 'lucide-vue-next'
import { api, write, params, session, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { secure } from '../app/security'
import { report, notify, errorText } from '../app/feedback'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import BucketPermissions from '../components/BucketPermissions.vue'
import EmptyState from '../components/EmptyState.vue'
type Credential = components['schemas']['Credential']
type Token = components['schemas']['Token']
type Row = Credential | Token
type Grant = components['schemas']['BucketGrant']
type Action = components['schemas']['Action']
const route = useRoute(), service = computed(() => route.name === 'credentials'), after = ref(''), project = ref('')
const owner = computed(() => !service.value && session.value?.role === 'admin' ? String(route.query.user || '') : '')
const endpoint = computed(() => service.value ? '/api/credentials' : '/api/tokens')
const rows = useQuery({ queryKey: ['access-keys', service, project, after, owner], queryFn: async ({ signal }) => {
  const q = params({ after: after.value || undefined, project: service.value ? project.value || undefined : undefined, user: owner.value || undefined })
  if (service.value) { const page = await api<components['schemas']['CredentialPage']>(endpoint.value + '?' + q, { signal }); return { rows: page.credentials as Row[], next: page.next } }
  const page = await api<components['schemas']['TokenPage']>(endpoint.value + '?' + q, { signal }); return { rows: page.tokens as Row[], next: page.next }
} })
const projects = useQuery({ queryKey: ['projects'], enabled: service, queryFn: ({ signal }) => api<components['schemas']['Project'][]>('/api/projects', { signal }) })
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const title = computed(() => t(service.value ? 'credentials' : 'tokens'))
const options = computed(() => projects.data.value?.map(item => ({ value: item.id, label: item.name })) || [])
const editOpen = ref(false), selected = ref<Row | null>(null), busy = ref(false), detail = ref<Row | null>(null)
const draft = reactive({ label: '', project: '', system: false, enabled: true, expiry: '90d', grants: [] as Grant[] })
const expiryOptions = computed(() => ['keep', '7d', '30d', '90d', '365d', 'never'].filter(value => selected.value || value !== 'keep').map(value => ({ value, label: t('expiry_' + value) })))
const available = computed(() => buckets.data.value?.filter(bucket => !service.value || bucket.project_id === draft.project) || [])
const actions: Action[] = ['bucket.list', 'object.read', 'object.write', 'object.delete', 'object.acl', 'bucket.settings', 'storage.inspect']
const secret = ref(''), access = ref(''), acknowledged = ref(false), secretInput = ref<HTMLInputElement | null>(null)
const secretOpen = computed({ get: () => !!secret.value, set: value => { if (!value && acknowledged.value) { secret.value = ''; access.value = '' } } })
const detailOpen = computed({ get: () => !!detail.value, set: value => { if (!value) detail.value = null } })
const operation = ref<'revoke' | 'rotate' | ''>(''), confirm = ref(''), overlap = ref('24h')
const operationOpen = computed({ get: () => !!operation.value, set: value => { if (!value) operation.value = '' } })
const overlapOptions = computed(() => ['0s', '1h', '24h', '7d'].map(value => ({ value, label: t('overlap_' + value) })))
function id(row: Row | null) { return row ? 'access_key' in row ? row.access_key : row.id : '' }
function isActive(row: Row) { return 'active' in row ? row.active : row.enabled && (!row.expires_at || new Date(row.expires_at).getTime() > Date.now()) }
function canEdit(row: Row) { return !('user_id' in row) || (row.user_id === session.value?.id && !row.revoked_at) }
watch([service, project, owner], () => { after.value = ''; detail.value = null; editOpen.value = false })
watch(() => draft.project, () => { draft.grants = draft.grants.filter(grant => available.value.some(bucket => bucket.id === grant.bucket_id)) })
watch(operation, () => { confirm.value = ''; overlap.value = '24h' })
function edit(row?: Row) {
  selected.value = row || null
  Object.assign(draft, { label: row?.label || '', project: row && 'project_id' in row ? row.project_id : project.value || options.value[0]?.value || '', system: !!(row && 'system' in row && row.system), enabled: row && 'enabled' in row ? row.enabled : true, expiry: row ? 'keep' : service.value ? 'never' : '90d', grants: (row?.grants || []).map(grant => ({ ...grant, actions: [...grant.actions] })) })
  editOpen.value = true
}
function expires() { return ['never', 'keep'].includes(draft.expiry) ? null : draft.expiry }
function reveal(value: string, key = '') { acknowledged.value = false; access.value = key; secret.value = value }
async function saved() { await rows.refetch(); notify(t('saved')) }
async function save() {
  busy.value = true
  try {
    const grants = draft.system && !service.value ? [] : draft.grants.filter(grant => grant.actions.length)
    if (service.value) {
      if (selected.value) {
        const key = id(selected.value)
        detail.value = await secure(() => write<Credential>(endpoint.value + '/' + key, { label: draft.label, enabled: draft.enabled, expires_in: expires(), keep_expiry: draft.expiry === 'keep', grants }, 'PUT'))
      } else {
        const row = await secure(() => write<components['schemas']['CredentialSecret']>(endpoint.value, { project_id: draft.project, label: draft.label, expires_in: expires(), grants }))
        reveal(row.secret_key, row.access_key)
      }
    } else {
      const input = { label: draft.label, system: draft.system, expires_in: expires(), keep_expiry: draft.expiry === 'keep', grants }
      if (selected.value) detail.value = await secure(() => write<Token>(endpoint.value + '/' + id(selected.value!), input, 'PUT'))
      else { const row = await secure(() => write<components['schemas']['TokenSecret']>(endpoint.value, input)); reveal(row.secret) }
    }
    editOpen.value = false; await saved()
  } catch (error) { report(error) } finally { busy.value = false }
}
async function execute() {
  if (!detail.value) return
  busy.value = true
  try {
    if (operation.value === 'rotate') {
      const row = await secure(() => write<components['schemas']['CredentialRotation']>(endpoint.value + '/' + id(detail.value!) + '/rotate', { overlap: overlap.value, expires_in: null }))
      reveal(row.secret_key, row.access_key)
    } else await secure(() => api(endpoint.value + '/' + id(detail.value!), { method: 'DELETE' }))
    detail.value = null; operation.value = ''; await saved()
  } catch (error) { report(error) } finally { busy.value = false }
}
async function copySecret() {
  try { await navigator.clipboard.writeText(access.value ? `AWS_ACCESS_KEY_ID=${access.value}\nAWS_SECRET_ACCESS_KEY=${secret.value}` : secret.value); notify(t('copied')) }
  catch { secretInput.value?.focus(); secretInput.value?.select(); notify(t('secretSelect')) }
}
function unloading(event: BeforeUnloadEvent) { if (secret.value && !acknowledged.value) { event.preventDefault(); event.returnValue = '' } }
window.addEventListener('beforeunload', unloading)
onBeforeUnmount(() => { secret.value = ''; window.removeEventListener('beforeunload', unloading) })
onBeforeRouteLeave(() => { if (secret.value && !acknowledged.value) { notify(t('saveSecretHint')); return false } })
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('accessEyebrow') }}</p><h1>{{ title }}</h1><p>{{ t(service ? 'credentialsHint' : 'tokensHint') }}</p></div><button v-if="!owner" class="primary" @click="edit()"><Plus :size="17" />{{ t(service ? 'createCredential' : 'createToken') }}</button></div>
  <div class="info-callout"><ShieldCheck :size="22" /><p>{{ t(service ? 'serviceOwnership' : 'tokenIntersection') }}</p></div>
  <div v-if="service" class="key-filter"><UiSelect v-model="project" :options="[{ value: '', label: t('allProjects') }, ...options]" :label="t('projects')" /></div>
  <ErrorNotice v-if="rows.error.value || buckets.error.value" :error="rows.error.value || buckets.error.value" />
  <section class="surface key-list"><button v-for="row in rows.data.value?.rows" :key="id(row)" class="key-row" @click="detail = row"><span class="key-symbol"><KeyRound :size="22" /></span><span class="key-name"><strong>{{ row.label || t('unnamedKey') }}</strong><code>{{ 'access_key' in row ? row.access_key : row.prefix + '…' }}</code></span><span class="key-dates"><small>{{ t('expires') }}</small>{{ row.expires_at ? date(row.expires_at) : t('expiry_never') }}</span><span class="badge" :class="{ muted: !isActive(row) }">{{ t(isActive(row) ? 'keyActive' : 'keyInactive') }}</span><ChevronRight :size="17" /></button><EmptyState v-if="rows.data.value && !rows.data.value.rows.length" :title="t('noKeys')" :description="t('noKeysHint')" /><div class="list-footer"><button v-if="after" @click="after = ''">{{ t('firstPage') }}</button><button v-if="rows.data.value?.next" @click="after = rows.data.value.next">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div></section>
  <UiDialog v-model:open="detailOpen" :title="detail?.label || title" drawer><template v-if="detail"><div class="key-hero"><span class="key-symbol"><KeyRound :size="32" /></span><div><p class="eyebrow">{{ t('system' in detail && detail.system ? 'fullManagement' : 'scopedKey') }}</p><code>{{ id(detail) }}</code></div></div><dl class="key-facts"><dt>{{ t('created') }}</dt><dd>{{ date(detail.created_at) }}</dd><dt>{{ t('expires') }}</dt><dd>{{ detail.expires_at ? date(detail.expires_at) : t('expiry_never') }}</dd><dt>{{ t('lastSeen') }}</dt><dd>{{ detail.last_used_at ? date(detail.last_used_at) : t('neverUsed') }}</dd></dl><div class="detail-section"><h3>{{ t('permissions') }}</h3><p v-if="'system' in detail && detail.system" class="error">{{ t('systemTokenHint') }}</p><div v-for="grant in detail.grants" :key="grant.bucket_id" class="grant-summary"><strong>{{ buckets.data.value?.find(bucket => bucket.id === grant.bucket_id)?.name || grant.bucket_id }}</strong><div class="permission-pills"><span v-for="action in grant.actions" :key="action" class="badge">{{ t('action_' + action) }}</span></div></div><p v-if="!detail.grants.length && !('system' in detail && detail.system)" class="field-help">{{ t('noScope') }}</p></div><div class="actions"><button v-if="canEdit(detail)" @click="edit(detail)">{{ t('editKey') }}</button><button v-if="service && isActive(detail)" @click="operation = 'rotate'"><RefreshCw :size="16" />{{ t('rotateKey') }}</button><button v-if="!('revoked_at' in detail) || !detail.revoked_at" class="danger-button" @click="operation = 'revoke'"><Trash2 :size="16" />{{ t('revokeKey') }}</button></div></template></UiDialog>
  <UiDialog v-model:open="editOpen" :title="t(selected ? 'editKey' : service ? 'createCredential' : 'createToken')" :busy="busy" wide><form @submit.prevent="save"><div class="key-editor"><section><label>{{ t('keyLabel') }}<input v-model="draft.label" required maxlength="128" :disabled="busy" autocomplete="off"></label><label v-if="service">{{ t('projects') }}<UiSelect v-model="draft.project" :options="options" :label="t('projects')" :disabled="busy || !!selected" /></label><label>{{ t('expires') }}<UiSelect v-model="draft.expiry" :options="expiryOptions" :label="t('expires')" :disabled="busy" /></label><UiCheckbox v-if="service && selected" v-model="draft.enabled" :label="t('keyEnabled')" :disabled="busy" /><UiCheckbox v-if="!service && session?.role === 'admin'" v-model="draft.system" :label="t('fullManagement')" :disabled="busy" /><p v-if="draft.system && !service" class="error">{{ t('systemTokenHint') }}</p><p class="field-help">{{ t('noScope') }}</p></section><section class="permission-section"><h3><ShieldCheck :size="18" />{{ t('permissions') }}</h3><BucketPermissions v-if="service || !draft.system" v-model="draft.grants" :buckets="available" :actions="actions" :disabled="busy" /><div v-else class="scope-summary"><ShieldCheck :size="40" /><p>{{ t('systemTokenHint') }}</p></div></section></div><div class="form-footer"><button type="button" :disabled="busy" @click="editOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy || (service && !draft.project)">{{ t(selected ? 'save' : service ? 'createCredential' : 'createToken') }}</button></div></form></UiDialog>
  <UiDialog v-model:open="operationOpen" :title="t(operation === 'rotate' ? 'rotateKey' : 'revokeKey')" :description="t(operation === 'rotate' ? 'rotationHint' : 'revokeKeyHint')" :busy="busy"><form @submit.prevent="execute"><label v-if="operation === 'rotate'">{{ t('overlap') }}<UiSelect v-model="overlap" :options="overlapOptions" :label="t('overlap')" :disabled="busy" /></label><label v-else>{{ t('confirmIdentity', { name: detail?.label || id(detail!) }) }}<input v-model="confirm" :disabled="busy" autocomplete="off"></label><div class="form-footer"><button type="button" :disabled="busy" @click="operation = ''">{{ t('cancel') }}</button><button :class="operation === 'rotate' ? 'primary' : 'danger-button'" :disabled="busy || (operation === 'revoke' && confirm !== (detail?.label || (detail && id(detail))))">{{ t(operation === 'rotate' ? 'rotateKey' : 'revokeKey') }}</button></div></form></UiDialog>
  <UiDialog v-model:open="secretOpen" :title="t('saveSecret')" :description="t('saveSecretHint')" :busy="!acknowledged"><div class="secret-emblem"><KeyRound :size="28" /></div><label v-if="access">{{ t('accessKeyId') }}<input :value="access" readonly autocomplete="off"></label><label>{{ t('secretValue') }}<input ref="secretInput" :value="secret" readonly autocomplete="off" spellcheck="false"></label><button @click="copySecret"><Copy :size="16" />{{ t('copySecret') }}</button><div class="detail-section"><UiCheckbox v-model="acknowledged" :label="t('secretSaved')" /></div><template #footer><button class="primary" :disabled="!acknowledged" @click="secretOpen = false">{{ t('done') }}</button></template></UiDialog>
</template>
