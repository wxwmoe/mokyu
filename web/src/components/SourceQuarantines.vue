<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ShieldAlert, RefreshCw } from 'lucide-vue-next'
import { api, params } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'
import UiSelect from './ui/UiSelect.vue'
import UiListFooter from './ui/UiListFooter.vue'
import MaintenanceConfirm from './MaintenanceConfirm.vue'

const storage = ref(''), source = ref('chunk'), after = ref(''), id = ref(''), confirm = ref(false)
const operation = ref<'quarantine' | 'recheck'>('quarantine'), input = ref<Record<string, unknown>>({})
const storages = useQuery({ queryKey: ['storages'], queryFn: ({ signal }) => api<components['schemas']['StorageView'][]>('/api/v1/storages', { signal }) })
const options = computed(() => (storages.data.value || []).map(item => ({ value: item.id, label: item.name })))
watch(options, items => { if (!storage.value && items.length === 1) storage.value = items[0]!.value }, { immediate: true })
watch([storage, source], () => { after.value = '' })
const sources = computed(() => ['chunk', 'pack', 'raw'].map(value => ({ value, label: t('physical_' + value) })))
const idPattern = computed(() => source.value === 'raw' ? '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' : '[1-9][0-9]*')
const validId = computed(() => new RegExp('^' + idPattern.value + '$').test(id.value))
const page = useQuery({ queryKey: ['quarantines', storage, source, after], enabled: computed(() => !!storage.value), queryFn: ({ signal }) => api<components['schemas']['SourcePage']>('/api/v1/maintenance/quarantines?' + params({ storage_id: storage.value, source: source.value, after: after.value || undefined }), { signal }), refetchInterval: 10_000 })
function preview(action: 'quarantine' | 'recheck', value: string) { input.value = { storage_id: storage.value, source: source.value, id: value }; operation.value = action; confirm.value = true }
</script>

<template>
  <section class="surface recovery-card source-quarantines">
    <div class="section-heading"><h2><ShieldAlert :size="21" />{{ t('quarantinedSources') }}</h2><button class="icon-button" :disabled="!storage" :aria-label="t('refresh')" @click="page.refetch()"><RefreshCw :size="17" /></button></div>
    <p class="field-help">{{ t('quarantinedSourcesHint') }}</p>
    <div class="source-filters"><label>{{ t('taskStorage') }}<UiSelect v-model="storage" :options="options" :label="t('taskStorage')" /></label><label>{{ t('physicalSource') }}<UiSelect v-model="source" :options="sources" :label="t('physicalSource')" /></label></div>
    <ErrorNotice v-if="storages.error.value || page.error.value" :error="storages.error.value || page.error.value" />
    <div v-if="storage && page.isPending.value" class="skeleton-row" />
    <p v-if="page.data.value && !page.data.value.items.length" class="field-help">{{ t('noQuarantinedSources') }}</p>
    <div class="issue-list"><article v-for="item in page.data.value?.items" :key="item.id"><div class="section-heading"><strong>{{ t('physical_' + item.source) }} #{{ item.id }}</strong><span class="badge error-badge">{{ t('sourceQuarantined') }}</span></div><p class="field-help">{{ item.code }} · {{ date(item.quarantined_at) }}</p><details><summary>{{ t('technicalDetails') }}</summary><dl class="facts"><dt>{{ t('sourceRevision') }}</dt><dd>{{ item.revision }}</dd><dt>{{ t('sourceLocation') }}</dt><dd><code>{{ item.location_id }}</code></dd><dt>{{ t('sourceState') }}</dt><dd>{{ item.state }}</dd></dl></details><button :disabled="item.state !== 'ready'" @click="preview('recheck', item.id)">{{ t('recheckSource') }}</button></article></div>
    <UiListFooter :first="!!after" :next="!!page.data.value?.next" @first="after = ''" @next="after = page.data.value!.next || ''" />
    <details class="source-manual"><summary>{{ t('quarantineExactSource') }}</summary><p class="field-help">{{ t('quarantineExactHint') }}</p><form @submit.prevent="preview('quarantine', id)"><label>{{ t('physicalSourceId') }}<input v-model="id" :inputmode="source === 'raw' ? 'text' : 'numeric'" :pattern="idPattern" required></label><button class="danger" :disabled="!storage || !validId">{{ t('quarantineSource') }}</button></form></details>
  </section>
  <MaintenanceConfirm v-model:open="confirm" :operation="operation" :input="input" />
</template>

<style scoped>
.source-filters, .source-manual form { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px; align-items: end; margin: 18px 0; }
.source-filters label, .source-manual label { display: grid; gap: 6px; min-width: 0; }
.source-quarantines strong, .source-quarantines code { overflow-wrap: anywhere; }
.source-quarantines .issue-list { margin: 16px 0; }
.source-manual { padding-top: 18px; border-top: 1px solid var(--line-soft); }
@media (max-width: 600px) { .source-filters, .source-manual form { grid-template-columns: minmax(0, 1fr); } }
</style>
