<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useInfiniteQuery, useQuery } from '@tanstack/vue-query'
import { ChevronRight, Cloud, Pencil, Plus, RefreshCw, Trash2 } from 'lucide-vue-next'
import { api, params, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { notify, report } from '../app/feedback'
import { secure } from '../app/security'
import EmptyState from '../components/EmptyState.vue'
import ErrorNotice from '../components/ErrorNotice.vue'
import ExternalTest from './ExternalTest.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'

type Bucket = components['schemas']['ExternalBucket']
type Connection = components['schemas']['ExternalConnection']
const route = useRoute(), router = useRouter(), project = ref('')
const endpoint = '/api/v1/external-buckets'
const projects = useQuery({ queryKey: ['projects'], queryFn: ({ signal }) => api<components['schemas']['Project'][]>('/api/v1/projects', { signal }) })
const options = computed(() => projects.data.value?.map(item => ({ value: item.id, label: item.name })) || [])
const buckets = useInfiniteQuery({
  queryKey: computed(() => ['external-buckets', 'list', project.value]),
  initialPageParam: '',
  queryFn: ({ signal, pageParam }) => api<components['schemas']['ExternalBucketPage']>(endpoint + '?' + params({ project: project.value || undefined, after: pageParam || undefined, limit: '100' }), { signal }),
  getNextPageParam: page => page.next || undefined,
})
const rows = computed(() => buckets.data.value?.pages.flatMap(page => page.items) || [])
const selectedId = computed(() => String(route.params.external || ''))
const selected = useQuery({ queryKey: computed(() => ['external-bucket', selectedId.value]), enabled: computed(() => !!selectedId.value), queryFn: ({ signal }) => api<Bucket>(endpoint + '/' + encodeURIComponent(selectedId.value), { signal }) })
const detailOpen = computed({ get: () => !!selectedId.value, set: value => { if (!value) void router.replace({ name: 'external-buckets', query: route.query }) } })
const editorOpen = ref(false), editing = ref<Bucket | null>(null), deleting = ref(false), confirm = ref(''), busy = ref(false), error = ref<unknown>(null)
const draft = reactive({ name: '', project: '', endpoint: '', region: 'us-east-1', bucket: '', prefix: '', addressing: 'path', allowHttp: false, enabled: true, access: '', secret: '' })
const addressing = computed(() => [{ value: 'path', label: t('externalPathAddressing') }, { value: 'virtual', label: t('externalHostAddressing') }])
function clearSecrets() { draft.access = ''; draft.secret = '' }
watch(editorOpen, open => { if (!open) clearSecrets() })
watch(deleting, () => { confirm.value = ''; error.value = null })
onBeforeUnmount(clearSecrets)
function edit(row?: Bucket) {
  editing.value = row || null
  const value = row?.connection
  Object.assign(draft, { name: row?.name || '', project: row?.project_id || project.value || options.value[0]?.value || '', endpoint: value?.endpoint_url || '', region: value?.region || 'us-east-1', bucket: value?.bucket || '', prefix: value?.prefix || '', addressing: value?.virtual_hosted ? 'virtual' : 'path', allowHttp: value?.allow_http || false, enabled: row?.enabled ?? true, access: '', secret: '' })
  error.value = null; editorOpen.value = true
}
async function save() {
  busy.value = true; error.value = null
  const connection: Connection = { endpoint_url: draft.endpoint.trim(), region: draft.region.trim(), bucket: draft.bucket.trim(), prefix: draft.prefix, virtual_hosted: draft.addressing === 'virtual', allow_http: draft.allowHttp }
  try {
    const row = editing.value
      ? await secure(() => write<Bucket>(endpoint + '/' + editing.value!.id, { revision: editing.value!.revision, name: draft.name.trim(), connection, enabled: draft.enabled, ...(draft.access ? { access_key: draft.access } : {}), ...(draft.secret ? { secret_key: draft.secret } : {}) }, 'PATCH'))
      : await secure(() => write<Bucket>(endpoint, { project_id: draft.project, name: draft.name.trim(), connection, access_key: draft.access, secret_key: draft.secret }))
    queries.setQueryData(['external-bucket', row.id], row)
    if (!editing.value) await router.replace({ name: 'external-buckets', params: { external: row.id }, query: route.query })
    editorOpen.value = false
    await queries.invalidateQueries({ queryKey: ['external-buckets'] })
    notify(t('externalSaved'))
  } catch (value) { error.value = value; report(value) } finally { busy.value = false }
}
async function remove() {
  const row = selected.data.value
  if (!row) return
  busy.value = true; error.value = null
  try {
    await secure(() => write(endpoint + '/' + row.id, { revision: row.revision }, 'DELETE'))
    deleting.value = false; detailOpen.value = false
    queries.removeQueries({ queryKey: ['external-bucket', row.id], exact: true })
    await queries.invalidateQueries({ queryKey: ['external-buckets'] })
    notify(t('externalRemoved'))
  } catch (value) { error.value = value; report(value) } finally { busy.value = false }
}
</script>

<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('administration') }}</p><h1>{{ t('externalBuckets') }}</h1><p>{{ t('externalBucketsHint') }}</p></div><button class="primary" @click="edit()"><Plus :size="17" />{{ t('externalAdd') }}</button></div>
  <div class="key-filter"><UiSelect v-model="project" :options="[{ value: '', label: t('allProjects') }, ...options]" :label="t('projects')" /></div>
  <ErrorNotice :error="buckets.error.value || projects.error.value" />
  <button v-if="buckets.isError.value || projects.isError.value" :disabled="buckets.isFetching.value || projects.isFetching.value" @click="buckets.refetch(); projects.refetch()"><RefreshCw :size="15" />{{ t('externalRetry') }}</button>
  <section class="surface key-list">
    <div v-if="buckets.isPending.value" class="skeleton-row" role="status" :aria-label="t('loading')" />
    <button v-for="row in rows" :key="row.id" class="key-row" @click="router.push({ name: 'external-buckets', params: { external: row.id }, query: route.query })"><span class="key-symbol"><Cloud :size="22" /></span><span class="key-name"><strong>{{ row.name }}</strong><code>{{ row.connection.bucket }}<template v-if="row.connection.prefix"> / {{ row.connection.prefix }}</template></code></span><span class="key-dates"><small>{{ t('projects') }}</small>{{ projects.data.value?.find(item => item.id === row.project_id)?.name || row.project_id }}</span><span class="badge node-state" :data-tone="row.enabled ? 'mint' : 'gray'">{{ t(row.enabled ? 'externalEnabled' : 'disabled') }}</span><ChevronRight :size="17" /></button>
    <EmptyState v-if="buckets.isSuccess.value && !rows.length" :title="t('externalEmpty')" :description="t('externalEmptyHint')" />
    <div v-if="buckets.hasNextPage.value" class="form-footer"><button :disabled="buckets.isFetchingNextPage.value" @click="buckets.fetchNextPage()">{{ t('externalLoadMore') }}</button></div>
  </section>
  <UiDialog v-model:open="detailOpen" :title="selected.data.value?.name || t('externalBuckets')" drawer>
    <ErrorNotice :error="selected.error.value" />
    <p v-if="selected.isPending.value" role="status">{{ t('loading') }}</p>
    <button v-if="selected.isError.value" :disabled="selected.isFetching.value" @click="selected.refetch()">{{ t('externalRetry') }}</button>
    <template v-if="selected.data.value">
      <dl class="key-facts"><dt>{{ t('externalEndpoint') }}</dt><dd class="object-path">{{ selected.data.value.connection.endpoint_url }}</dd><dt>{{ t('bucket') }}</dt><dd>{{ selected.data.value.connection.bucket }}</dd><dt>{{ t('externalPrefix') }}</dt><dd>{{ selected.data.value.connection.prefix || '—' }}</dd><dt>{{ t('externalRegion') }}</dt><dd>{{ selected.data.value.connection.region }}</dd><dt>{{ t('externalAddressing') }}</dt><dd>{{ t(selected.data.value.connection.virtual_hosted ? 'externalHostAddressing' : 'externalPathAddressing') }}</dd><dt>{{ t('created') }}</dt><dd>{{ date(selected.data.value.created_at) }}</dd></dl>
      <div class="actions"><button @click="edit(selected.data.value)"><Pencil :size="15" />{{ t('externalEdit') }}</button><button class="danger-button" @click="deleting = true"><Trash2 :size="15" />{{ t('externalRemove') }}</button></div>
      <ExternalTest :key="selected.data.value.id" :bucket="selected.data.value" />
    </template>
  </UiDialog>
  <UiDialog v-model:open="editorOpen" :title="t(editing ? 'externalEdit' : 'externalAdd')" :busy="busy" wide>
    <form @submit.prevent="save"><ErrorNotice :error="error" />
      <div class="key-editor"><section><label>{{ t('name') }}<input v-model="draft.name" required maxlength="128" :disabled="busy"></label><label>{{ t('projects') }}<UiSelect v-model="draft.project" :options="options" :label="t('projects')" :disabled="busy || !!editing" /></label><label>{{ t('externalEndpoint') }}<input v-model="draft.endpoint" type="url" required maxlength="2048" :disabled="busy" spellcheck="false"></label><label>{{ t('externalRegion') }}<input v-model="draft.region" required maxlength="128" :disabled="busy"></label><label>{{ t('bucket') }}<input v-model="draft.bucket" required maxlength="255" :disabled="busy" spellcheck="false"></label><label>{{ t('externalPrefix') }}<input v-model="draft.prefix" maxlength="512" :disabled="busy" spellcheck="false"></label><p class="field-help">{{ t('externalPrefixHint') }}</p></section>
      <section><label>{{ t('externalAddressing') }}<UiSelect v-model="draft.addressing" :options="addressing" :label="t('externalAddressing')" :disabled="busy" /></label><UiCheckbox v-model="draft.allowHttp" :label="t('externalAllowHttp')" :disabled="busy" /><label>{{ t('accessKeyId') }}<input v-model="draft.access" :required="!editing" maxlength="256" :disabled="busy" autocomplete="off" spellcheck="false"></label><label>{{ t('secretValue') }}<input v-model="draft.secret" type="password" :required="!editing" maxlength="4096" :disabled="busy" autocomplete="new-password"></label><p class="field-help">{{ t(editing ? 'externalKeepCredentials' : 'externalCredentialsHint') }}</p><UiCheckbox v-if="editing" v-model="draft.enabled" :label="t('externalEnabled')" :disabled="busy" /></section></div>
      <div class="form-footer"><button type="button" :disabled="busy" @click="editorOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy || !draft.project">{{ t('save') }}</button></div>
    </form>
  </UiDialog>
  <UiDialog v-model:open="deleting" :title="t('externalRemove')" :description="t('externalRemoveHint')" :busy="busy"><form @submit.prevent="remove"><ErrorNotice :error="error" /><label>{{ t('confirmIdentity', { name: selected.data.value?.name || '' }) }}<input v-model="confirm" autocomplete="off" :disabled="busy"></label><div class="form-footer"><button type="button" :disabled="busy" @click="deleting = false">{{ t('cancel') }}</button><button class="danger-button" :disabled="busy || !selected.data.value || confirm !== selected.data.value.name">{{ t('externalRemove') }}</button></div></form></UiDialog>
</template>
