<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { BookHeart, Download, Filter, KeyRound, Users, FolderHeart, Settings2, LogIn, ChevronRight, Copy } from 'lucide-vue-next'
import { api, params, session, ApiError } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { copy, errorText, report } from '../app/feedback'
import Avatar from '../components/Avatar.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import EmptyState from '../components/EmptyState.vue'
type Event = components['schemas']['Event']
const actor = ref(''), search = ref(''), action = ref(''), source = ref(''), outcome = ref(''), days = ref('7'), after = ref(''), selected = ref<Event | null>(null)
const since = ref(new Date(Date.now() - 7 * 86400000).toISOString())
watch(days, value => { since.value = value ? new Date(Date.now() - Number(value) * 86400000).toISOString() : '' })
watch([actor, action, source, outcome, since], () => { after.value = '' })
const query = computed(() => params({ actor: actor.value || undefined, action: action.value || undefined, source: source.value || undefined, outcome: outcome.value || undefined, since: since.value || undefined, after: after.value || undefined, limit: '50' }))
const page = useQuery({ queryKey: ['audit', query], queryFn: ({ signal }) => api<components['schemas']['Page']>('/api/audit?' + query.value, { signal }) })
const selectedId = computed(() => selected.value?.id || '')
const detail = useQuery({ queryKey: ['audit-detail', selectedId], enabled: computed(() => !!selectedId.value), queryFn: ({ signal }) => api<Event>('/api/audit/' + selectedId.value, { signal }) })
const current = computed(() => detail.data.value || selected.value)
const open = computed({ get: () => !!selected.value, set: value => { if (!value) selected.value = null } })
const groups = computed(() => ['', 'user.', 'project.', 'credential.', 'token.', 'session.', 'account.', 'bucket.', 'object.'].map(value => ({ value, label: t(value ? 'auditGroup_' + value.slice(0, -1) : 'auditAllActions') })))
const sources = computed(() => ['', 'web', 'token', 'cli'].map(value => ({ value, label: t(value ? 'auditSource_' + value : 'auditAllSources') })))
const outcomes = computed(() => ['', 'succeeded', 'failed', 'partial', 'unknown'].map(value => ({ value, label: t(value ? 'auditOutcome_' + value : 'auditAllOutcomes') })))
const periods = computed(() => ['1', '7', '30', ''].map(value => ({ value, label: value ? t('auditDays', { count: value }) : t('auditAllTime') })))
function title(event: Event) { const key = 'auditAction_' + event.action; return t(key) === key ? event.action : t(key) }
function group(event: Event) { return event.action.split('.')[0] || 'account' }
function icon(event: Event) { const kind = group(event); return ['credential', 'token'].includes(kind) ? KeyRound : kind === 'user' ? Users : ['project', 'bucket', 'object'].includes(kind) ? FolderHeart : kind === 'session' ? LogIn : Settings2 }
function summary(event: Event) { const detail = event.detail as Record<string, unknown>; return String(detail.label || detail.name || detail.username || event.target || t('auditInstance')) }
function attempted(event: Event) { return (event.detail as Record<string, unknown>).identity_verified === false }
function reset() { actor.value = ''; search.value = ''; action.value = ''; source.value = ''; outcome.value = ''; days.value = '7'; after.value = '' }
const exporting = ref(false), controller = new AbortController()
onBeforeUnmount(() => controller.abort())
async function download() {
  exporting.value = true
  try {
    const response = await fetch('/api/audit/export?' + query.value, { credentials: 'same-origin', signal: controller.signal })
    if (!response.ok) { const error = await response.json(); throw new ApiError(response.status, error.code, error.request_id) }
    const url = URL.createObjectURL(await response.blob()), link = document.createElement('a')
    link.href = url; link.download = 'mokyu-audit.jsonl'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000)
  } catch (error) { if (!controller.signal.aborted) report(error) } finally { exporting.value = false }
}
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('auditEyebrow') }}</p><h1>{{ t('audit') }}</h1><p>{{ t(session?.role === 'admin' ? 'auditHint' : 'auditMemberHint') }}</p></div><button :disabled="exporting || !page.data.value?.events.length" @click="download"><Download :size="17" />{{ t('auditExport') }}</button></div>
  <section class="surface audit-filters"><div class="section-heading"><h2><Filter :size="17" />{{ t('auditFilter') }}</h2><button class="text-button" @click="reset">{{ t('auditReset') }}</button></div><div class="audit-filter-grid"><label>{{ t('auditPeriod') }}<UiSelect v-model="days" :options="periods" :label="t('auditPeriod')" /></label><label>{{ t('auditAction') }}<UiSelect v-model="action" :options="groups" :label="t('auditAction')" /></label><label>{{ t('auditSource') }}<UiSelect v-model="source" :options="sources" :label="t('auditSource')" /></label><label>{{ t('auditOutcome') }}<UiSelect v-model="outcome" :options="outcomes" :label="t('auditOutcome')" /></label></div><form v-if="session?.role === 'admin'" class="audit-search" @submit.prevent="actor = search"><input v-model="search" type="search" :placeholder="t('auditSearchActor')" :aria-label="t('auditSearchActor')" maxlength="128"><button>{{ t('search') }}</button></form></section>
  <p v-if="page.error.value" class="error" role="alert">{{ errorText(page.error.value) }}</p>
  <section class="surface audit-timeline"><div class="section-heading"><h2><BookHeart :size="19" />{{ t('auditRecent') }}</h2><button :disabled="page.isFetching.value" @click="page.refetch()">{{ t('refresh') }}</button></div><button v-for="event in page.data.value?.events" :key="event.id" class="audit-row" @click="selected = event"><span class="audit-icon" :data-group="group(event)"><component :is="icon(event)" :size="20" /></span><span class="audit-description"><strong>{{ title(event) }}</strong><span>{{ summary(event) }}</span><small><span v-if="attempted(event)">{{ t('auditAttempt') }} </span>{{ event.actor_label }} · {{ t('auditSource_' + event.source) }}</small></span><span class="audit-when"><span class="badge" :class="{ 'audit-problem': ['failed', 'partial'].includes(event.outcome) }">{{ t('auditOutcome_' + event.outcome) }}</span><time :datetime="event.created_at">{{ date(event.created_at) }}</time></span><ChevronRight :size="16" /></button><EmptyState v-if="page.data.value && !page.data.value.events.length" :title="t('auditEmpty')" :description="t('auditEmptyHint')" /><div class="list-footer"><button v-if="after" @click="after = ''">{{ t('firstPage') }}</button><button v-if="page.data.value?.next" @click="after = page.data.value.next">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div></section>
  <UiDialog v-model:open="open" :title="current ? title(current) : t('audit')" drawer><p v-if="detail.error.value" class="error" role="alert">{{ errorText(detail.error.value) }}</p><template v-if="current"><div class="person-hero"><Avatar :name="current.actor_label" large /><div><p class="eyebrow">{{ t(attempted(current) ? 'auditAttempt' : 'auditActor') }}</p><h2>{{ current.actor_label }}</h2><span class="badge">{{ t('auditOutcome_' + current.outcome) }}</span></div></div><dl class="key-facts"><dt>{{ t('auditWhen') }}</dt><dd>{{ date(current.created_at) }}</dd><dt>{{ t('auditSource') }}</dt><dd>{{ t('auditSource_' + current.source) }}</dd><dt>{{ t('auditTarget') }}</dt><dd>{{ summary(current) }}</dd><template v-if="current.status"><dt>HTTP</dt><dd>{{ current.status }}</dd></template></dl><p v-if="current.outcome === 'unknown'" class="info-callout">{{ t('auditUnknownHint') }}</p><div v-if="current.request_id" class="detail-section"><h3>Request ID</h3><button class="text-button request-id" @click="copy(current.request_id)">{{ current.request_id }}<Copy :size="14" /></button></div><details class="detail-section"><summary>{{ t('auditTechnical') }}</summary><pre class="audit-json">{{ JSON.stringify(current.detail, null, 2) }}</pre><p class="field-help">{{ t('auditRecordId') }}: {{ current.id }}</p></details></template></UiDialog>
</template>
