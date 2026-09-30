<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter, onBeforeRouteLeave, onBeforeRouteUpdate } from 'vue-router'
import { TabsRoot, TabsList, TabsTrigger, TabsContent } from 'reka-ui'
import { Settings2, Globe, ShieldCheck, Flower2, Plus, Trash2, Save, RefreshCw, ArrowUpRight } from 'lucide-vue-next'
import { api, write, queries, session, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { notify, report, errorText, copy } from '../app/feedback'
import CreateBucket from '../components/CreateBucket.vue'
import BucketLifecycle from '../components/BucketLifecycle.vue'
import QuotaPanel from '../components/QuotaPanel.vue'
import EmptyState from '../components/EmptyState.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import UiDialog from '../components/ui/UiDialog.vue'

type Settings = components['schemas']['SettingsInput']
type Rule = components['schemas']['CorsRule']
const route = useRoute(), router = useRouter(), id = computed(() => String(route.params.bucket || ''))
const admin = computed(() => session.value?.role === 'admin')
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const manageable = computed(() => buckets.data.value?.filter(b => b.actions.includes('bucket.settings')) || [])
const filter = ref(''), filtered = computed(() => manageable.value.filter(b => b.name.toLowerCase().includes(filter.value.toLowerCase())))
const settings = useQuery({ queryKey: ['bucket-settings', id], enabled: computed(() => !!id.value), queryFn: ({ signal }) => api<components['schemas']['BucketSettings']>(`/api/buckets/${id.value}/settings`, { signal }) })
const draft = ref<Settings>(), baseline = ref(''), saving = ref(false)
function reset() {
  const value = settings.data.value
  if (!value) { draft.value = undefined; baseline.value = ''; return }
  const b = value.bucket
  draft.value = JSON.parse(JSON.stringify({ revision: b.revision, cors: b.cors, website_enabled: b.website_enabled, index_document: b.index_document, error_document: b.error_document, public_base_url: b.public_base_url, uploads_paused: b.uploads_paused, ...(admin.value ? { domains: value.domains } : {}) }))
  baseline.value = JSON.stringify(draft.value)
}
const dirty = computed(() => !!draft.value && JSON.stringify(draft.value) !== baseline.value)
watch(id, () => { draft.value = undefined; baseline.value = '' })
watch(settings.data, () => { if (!dirty.value) reset() }, { immediate: true })
const locked = computed(() => saving.value || settings.data.value?.bucket.state !== 'active')
const domains = computed({ get: () => (draft.value?.domains || []).join('\n'), set: value => { if (draft.value) draft.value.domains = lines(value) } })
const methods = ['GET', 'HEAD', 'PUT', 'POST', 'DELETE', 'OPTIONS']
function lines(value: string) { return value.split('\n') }
function clean(values: string[]) { return values.map(v => v.trim()).filter(Boolean) }
function method(rule: Rule, value: string, checked: boolean) { rule.methods = checked ? [...rule.methods, value] : rule.methods.filter(m => m !== value) }
function addRule(preset = false) { draft.value?.cors.push({ origins: preset ? ['*'] : [], methods: preset ? [...methods] : ['GET', 'HEAD'], headers: preset ? ['*'] : [], expose: preset ? ['*'] : [], max_age: preset ? 86400 : 300 }) }
async function refresh() { if (!await canLeave()) return; baseline.value = JSON.stringify(draft.value); const value = await settings.refetch(); if (value.error) report(value.error); else reset() }
async function save() {
  if (!draft.value) return
  saving.value = true
  try { const value = await write<components['schemas']['BucketSettings']>(`/api/buckets/${id.value}/settings`, { ...draft.value, domains: draft.value.domains ? clean(draft.value.domains) : undefined, cors: draft.value.cors.map(r => ({ ...r, origins: clean(r.origins), headers: clean(r.headers || []), expose: clean(r.expose || []) })) }, 'PUT'); baseline.value = JSON.stringify(draft.value); queries.setQueryData(['bucket-settings', id.value], value); reset(); await queries.invalidateQueries({ queryKey: ['buckets'] }); notify(t('bucketSettingsSaved')) }
  catch (error) { report(error) } finally { saving.value = false }
}
const discardOpen = ref(false)
let decide: ((value: boolean) => void) | undefined
function discard(value: boolean) { decide?.(value); decide = undefined; discardOpen.value = false }
watch(discardOpen, value => { if (!value) discard(false) })
function canLeave() { return !dirty.value || new Promise<boolean>(resolve => { decide?.(false); decide = resolve; discardOpen.value = true }) }
onBeforeRouteLeave(canLeave); onBeforeRouteUpdate(to => to.params.bucket === route.params.bucket || canLeave())
function beforeUnload(event: BeforeUnloadEvent) { if (dirty.value) { event.preventDefault(); event.returnValue = '' } }
window.addEventListener('beforeunload', beforeUnload)
onBeforeUnmount(() => { window.removeEventListener('beforeunload', beforeUnload); decide?.(false) })
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('littleHomes') }}</p><h1>{{ settings.data.value?.bucket.name || t('buckets') }}</h1><p>{{ t('bucketSettingsHint') }}</p></div><div class="heading-actions"><template v-if="id"><RouterLink class="button" :to="`/media/${id}`"><ArrowUpRight :size="16" />{{ t('browseFiles') }}</RouterLink><button :disabled="saving" @click="refresh"><RefreshCw :size="16" />{{ t('refresh') }}</button></template><CreateBucket v-else @created="router.push('/buckets/' + $event.id)" /></div></div>
  <ErrorNotice v-if="settings.error.value || buckets.error.value" :error="settings.error.value || buckets.error.value" />
  <template v-if="!id"><label class="search-field"><span class="sr-only">{{ t('findBucket') }}</span><input v-model="filter" :placeholder="t('findBucket')" type="search"></label><div class="bucket-grid"><RouterLink v-for="bucket in filtered" :key="bucket.id" class="bucket-card" :to="'/buckets/' + bucket.id"><img src="/assets/mokyu-pack.svg" alt="" width="64" height="64"><h2>{{ bucket.name }}</h2><span class="badge">{{ t(bucket.uploads_paused ? 'uploadsPaused' : bucket.state) }}</span><Settings2 class="bucket-arrow" :size="19" /></RouterLink></div><EmptyState v-if="!buckets.isPending.value && !filtered.length" :title="t('noBuckets')" :description="t('bucketSettingsEmpty')" /></template>
  <template v-else-if="draft && settings.data.value"><nav class="breadcrumbs bucket-back"><RouterLink to="/buckets">{{ t('buckets') }}</RouterLink><span>/</span><span>{{ settings.data.value.bucket.name }}</span><span v-if="settings.data.value.bucket.state !== 'active'" class="badge">{{ t(settings.data.value.bucket.state) }}</span></nav>
    <QuotaPanel v-if="settings.data.value.bucket.actions.includes('storage.inspect')" kind="bucket" :id="id" />
    <TabsRoot default-value="connection" class="surface bucket-settings"><TabsList class="tabs-list" :aria-label="t('buckets')"><TabsTrigger class="tab-trigger" value="connection"><Globe :size="16" />{{ t('bucketConnection') }}</TabsTrigger><TabsTrigger class="tab-trigger" value="cors"><ShieldCheck :size="16" />CORS</TabsTrigger><TabsTrigger class="tab-trigger" value="website"><Flower2 :size="16" />{{ t('bucketWebsite') }}</TabsTrigger></TabsList>
      <fieldset class="bucket-fields" :disabled="locked">
        <TabsContent value="connection"><h2>{{ t('bucketConnection') }}</h2><p class="field-help">{{ t('bucketConnectionHint') }}</p><label class="field"><span>{{ t('bucketName') }}</span><div class="input-action"><input :value="settings.data.value.bucket.name" readonly><button type="button" @click="copy(settings.data.value.bucket.name)">{{ t('copy') }}</button></div></label><label class="field"><span>{{ t('publicBaseUrl') }}</span><input v-model="draft.public_base_url" type="url" placeholder="https://media.example.com/" :readonly="!admin" maxlength="2048"></label><p class="field-help">{{ t('publicBaseUrlHint') }}</p><label class="field"><span>{{ t('bucketDomains') }}</span><textarea v-if="admin" v-model="domains" rows="4" placeholder="media.example.com" /><textarea v-else :value="settings.data.value.domains.join('\n')" rows="3" readonly /></label><p class="field-help">{{ t('bucketDomainsHint') }}</p></TabsContent>
        <TabsContent value="cors"><div class="section-heading"><h2>{{ t('corsTitle') }}</h2><button type="button" :disabled="draft.cors.length >= 100" @click="addRule(true)">{{ t('corsPreset') }}</button></div><p class="field-help">{{ t('corsHint') }}</p><p v-if="!draft.cors.length" class="info-callout">{{ t('corsOff') }}</p><section v-for="(rule, index) in draft.cors" :key="index" class="cors-rule"><div class="section-heading"><h3>{{ t('corsRule', { count: index + 1 }) }}</h3><button type="button" class="icon-button" :aria-label="t('removeCorsRule', { count: index + 1 })" @click="draft.cors.splice(index, 1)"><Trash2 :size="16" /></button></div><div class="form-pair"><label class="field"><span>{{ t('corsOrigins') }}</span><textarea :value="rule.origins.join('\n')" rows="3" placeholder="https://example.com" @input="rule.origins = lines(($event.target as HTMLTextAreaElement).value)" /></label><label class="field"><span>{{ t('corsHeaders') }}</span><textarea :value="(rule.headers || []).join('\n')" rows="3" placeholder="*" @input="rule.headers = lines(($event.target as HTMLTextAreaElement).value)" /></label></div><p class="field-help">{{ t('onePerLine') }}</p><div class="cors-methods"><UiCheckbox v-for="verb in methods" :key="verb" :label="verb" :model-value="rule.methods.includes(verb)" @update:model-value="method(rule, verb, $event)" /></div><div class="form-pair"><label class="field"><span>{{ t('corsExpose') }}</span><textarea :value="(rule.expose || []).join('\n')" rows="2" placeholder="ETag" @input="rule.expose = lines(($event.target as HTMLTextAreaElement).value)" /></label><label class="field"><span>{{ t('corsMaxAge') }}</span><input v-model.number="rule.max_age" type="number" min="0" max="86400" step="1"></label></div></section><button type="button" :disabled="draft.cors.length >= 100" @click="addRule()"><Plus :size="16" />{{ t('addCorsRule') }}</button></TabsContent>
        <TabsContent value="website"><h2>{{ t('bucketWebsite') }}</h2><p class="field-help">{{ t('websiteHint') }}</p><UiCheckbox v-model="draft.website_enabled" :label="t('enableWebsite')" /><label class="field"><span>{{ t('indexDocument') }}</span><input v-model="draft.index_document" maxlength="255" placeholder="index.html"></label><label class="field"><span>{{ t('errorDocument') }}</span><input v-model="draft.error_document" maxlength="1024" placeholder="404.html"></label><p class="info-callout">{{ t('websitePublicHint') }}</p></TabsContent>
      </fieldset><div class="form-footer bucket-save"><span class="field-help">{{ t(dirty ? 'unsavedChanges' : 'bucketSavedHint') }}</span><button :disabled="locked || !dirty" @click="reset">{{ t('cancel') }}</button><button class="primary" :disabled="locked || !dirty" @click="save"><Save :size="16" />{{ t(saving ? 'saving' : 'save') }}</button></div>
    </TabsRoot>
    <BucketLifecycle v-if="admin" :bucket="settings.data.value.bucket" :dirty="dirty" @changed="settings.refetch()" @removed="router.push('/buckets')" />
  </template>
  <div v-else-if="settings.isPending.value" class="skeleton-row" role="status"><span class="sr-only">{{ t('opening') }}</span></div>
  <UiDialog v-model:open="discardOpen" :title="t('unsavedChanges')" :description="t('bucketDiscardHint')"><template #footer><button @click="discard(false)">{{ t('keepEditing') }}</button><button class="danger-button" @click="discard(true)">{{ t('discard') }}</button></template></UiDialog>
</template>
