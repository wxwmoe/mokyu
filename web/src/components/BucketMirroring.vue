<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useInfiniteQuery, useQuery } from '@tanstack/vue-query'
import { ArrowDown, ArrowUp, ChevronDown, CloudDownload, FolderOpen, History, Pencil, Plus, RefreshCw, Route, ShieldCheck, Trash2 } from 'lucide-vue-next'
import { api, bytes, byteSize, params, parseByteSize, queries, session, uuid, write, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { notify, errorText } from '../app/feedback'
import ErrorNotice from './ErrorNotice.vue'
import UiCheckbox from './ui/UiCheckbox.vue'
import UiDialog from './ui/UiDialog.vue'
import UiListFooter from './ui/UiListFooter.vue'
import UiSwitch from './ui/UiSwitch.vue'
import MirroringSource from './MirroringSource.vue'
import MirroringProgress from './MirroringProgress.vue'
type View = components['schemas']['MirroringView']
type Rule = components['schemas']['MirroringRule']
type Source = components['schemas']['MirroringSource']
type Preview = components['schemas']['MirroringPreview']
type Probe = components['schemas']['MirroringProbe']
const props = defineProps<{ bucket: Bucket }>(), emit = defineEmits<{ dirty: [boolean] }>()
const base = computed(() => '/api/v1/buckets/' + props.bucket.id + '/mirroring')
const view = useQuery({ queryKey: ['mirroring', base], queryFn: ({ signal }) => api<View>(base.value, { signal }) })
const admin = computed(() => session.value?.role === 'admin')
const draft = ref<View>(), baseline = ref(''), size = ref(''), busy = ref(false), error = ref<unknown>()
const fingerprint = () => JSON.stringify([draft.value, size.value])
const dirty = computed(() => !!draft.value && fingerprint() !== baseline.value)
const locked = computed(() => busy.value || props.bucket.status !== 'active')
function reset(value = view.data.value) {
  if (!value) return
  draft.value = JSON.parse(JSON.stringify(value))
  size.value = value.config.max_object_size == null ? '' : byteSize(value.config.max_object_size)
  baseline.value = fingerprint(); error.value = undefined
}
watch(view.data, value => { if (!dirty.value) reset(value) }, { immediate: true })
const editing = ref<Rule>(), primary = ref<InstanceType<typeof MirroringSource>>(), backup = ref<InstanceType<typeof MirroringSource>>()
const editorOpen = computed({ get: () => !!editing.value, set: value => { if (!value) editing.value = undefined } })
const editorError = ref<unknown>(), deleting = ref<Rule>(), unblocking = ref<string>()
const ruleName = ref<HTMLInputElement>()
watch([dirty, editorOpen], () => emit('dirty', dirty.value || editorOpen.value))
onBeforeUnmount(() => emit('dirty', false))
const external = useInfiniteQuery({ queryKey: ['external-buckets', 'mirroring', props.bucket.project_id], initialPageParam: '',
  queryFn: ({ signal, pageParam }) => api<components['schemas']['ExternalBucketPage']>('/api/v1/external-buckets?' + params({ project: props.bucket.project_id, after: pageParam || undefined, limit: '100' }), { signal }),
  getNextPageParam: page => page.next || undefined })
const sources = computed(() => (external.data.value?.pages.flatMap(page => page.items) || []).filter(source => source.enabled && source.actions.includes('object.read')).map(source => ({ value: source.id, label: source.name, bucket: source.connection.bucket, prefix: source.connection.prefix || '' })))
const http = (): Source => ({ type: 'http', url: '', allow_http: false, allow_private: false, follow_redirects: false, headers: {}, header_names: [] })
function sourceLabel(source: Source) {
  if (source.type === 'http') return source.url
  const name = sources.value.find(item => item.value === source.external_id)?.label || t('externalBuckets')
  return source.prefix ? name + ' · ' + source.prefix : name
}
function sourceAddress(address: string) {
  const match = /^external:\/\/([^/]+)\/(.*)$/s.exec(address)
  const connection = match && sources.value.find(item => item.value === match[1])
  return connection ? `s3://${connection.bucket}/${connection.prefix ? connection.prefix + '/' : ''}${decodeURIComponent(match![2]!)}` : address
}
function edit(rule?: Rule) {
  editorError.value = undefined
  editing.value = rule ? JSON.parse(JSON.stringify(rule)) : { id: uuid(), name: '', prefix: '', strip_prefix: false, add_prefix: '', public_read: false, source: http(), backup: null }
}
function apply() {
  if (!editing.value || !draft.value) return
  try {
    primary.value?.flush(); backup.value?.flush()
    const item = editing.value, index = draft.value.config.rules.findIndex(rule => rule.id === item.id)
    if (index < 0) draft.value.config.rules.push(item)
    else draft.value.config.rules.splice(index, 1, item)
    editing.value = undefined
  } catch (e) { editorError.value = e }
}
function move(index: number, by: number) {
  const rules = draft.value!.config.rules, item = rules.splice(index, 1)[0]!
  rules.splice(index + by, 0, item)
}
function input() {
  const value = JSON.parse(JSON.stringify(draft.value!)) as View
  value.config.max_object_size = size.value.trim() ? parseByteSize(size.value) : null
  return value
}
async function save() {
  busy.value = true; error.value = undefined
  try { const value = await write<View>(base.value, input(), 'PUT'); reset(value); queries.setQueryData(['mirroring', base.value], value); notify(t('bucketSettingsSaved')) }
  catch (e) { error.value = e } finally { busy.value = false }
}
defineExpose({ refresh: async () => {
  const result = await view.refetch()
  if (result.error) error.value = result.error
  else reset(result.data)
} })
const key = ref(''), preview = ref<Preview>(), probe = ref<Probe>(), checking = ref(false), checkError = ref<unknown>()
watch([key, size, draft], () => { preview.value = undefined; probe.value = undefined; checkError.value = undefined }, { deep: true })
async function check(test = false, backup = false) {
  checking.value = true; checkError.value = undefined; probe.value = undefined
  const before = fingerprint(), objectKey = key.value
  try {
    const value = input()
    if (test) {
      const result = await write<Probe>(base.value + '/test', { ...value, key: objectKey, backup })
      if (before === fingerprint() && objectKey === key.value) probe.value = result
    } else {
      const result = await write<Preview>(base.value + '/preview', { config: value.config, key: objectKey })
      if (before === fingerprint() && objectKey === key.value) preview.value = result
    }
  } catch (e) { if (before === fingerprint() && objectKey === key.value) checkError.value = e } finally { checking.value = false }
}
const blockCursor = ref(''), taskCursor = ref(''), monitoring = ref(false)
const blocks = useQuery({ queryKey: ['mirroring-blocks', base, blockCursor], enabled: monitoring,
  queryFn: ({ signal }) => api<components['schemas']['BlockPage']>(base.value + '/blocks?' + params({ after: blockCursor.value || undefined, limit: '10' }), { signal }) })
const tasks = useQuery({ queryKey: ['mirroring-tasks', base, taskCursor], enabled: monitoring,
  queryFn: ({ signal }) => api<components['schemas']['JobPage']>(base.value + '/tasks?' + params({ token: taskCursor.value || undefined, limit: '8' }), { signal }), refetchInterval: 3000 })
async function unblock() {
  if (unblocking.value == null) return
  busy.value = true
  try { await write(base.value + '/unblock', { key: unblocking.value }); unblocking.value = undefined; await blocks.refetch() }
  catch (e) { error.value = e } finally { busy.value = false }
}
async function task(id: string, resume: boolean) {
  busy.value = true; error.value = undefined
  try { await write(base.value + '/tasks', { task: id, resume }); await tasks.refetch() }
  catch (e) { error.value = e } finally { busy.value = false }
}
</script>
<template>
  <section class="bucket-panel form-stack mirror-panel" :aria-busy="view.isPending.value">
    <header class="bucket-section-head">
      <div class="bucket-heading"><span class="policy-icon sky"><CloudDownload :size="22" aria-hidden="true" /></span><div><h2>{{ t('mirrorTitle') }}</h2><p class="field-help">{{ t('mirrorHint') }}</p></div></div>
      <span v-if="draft" class="badge node-state" :data-tone="dirty ? 'cream' : view.data.value?.config.enabled ? 'mint' : 'gray'">{{ t(dirty ? 'unsavedChanges' : view.data.value?.config.enabled ? 'mirrorEnabled' : 'mirrorDisabled') }}</span>
    </header>
    <ErrorNotice :error="error || view.error.value" />
    <div v-if="view.isPending.value" class="skeleton-row" />
    <form v-if="draft" class="bucket-subpanel form-stack mirror-settings" @submit.prevent="save">
      <div class="mirror-toggle">
        <div><h3>{{ t('mirrorEnable') }}</h3><p class="field-help">{{ t(draft.config.enabled ? 'mirrorEnabledHint' : 'mirrorDisabledHint') }}</p></div>
        <UiSwitch v-model="draft.config.enabled" :aria-label="t('mirrorEnable')" :disabled="locked" />
      </div>
      <template v-if="draft.config.enabled">
        <section class="form-stack mirror-rule-section">
          <header class="bucket-section-head"><div><h3>{{ t('mirrorRules') }}</h3><p class="field-help">{{ t('mirrorRuleOrder') }}</p></div><button type="button" :disabled="locked || draft.config.rules.length >= 64" @click="edit()"><Plus :size="16" />{{ t('mirrorAddRule') }}</button></header>
          <div v-if="!draft.config.rules.length" class="mirror-empty"><Route :size="24" aria-hidden="true" /><p>{{ t('mirrorNoRules') }}</p></div>
          <ol v-else class="mirror-rules"><li v-for="(rule, index) in draft.config.rules" :key="rule.id">
            <span class="mirror-rule-order" aria-hidden="true">{{ index + 1 }}</span>
            <div class="mirror-rule-text">
              <strong>{{ rule.name }}</strong><span class="mirror-rule-prefix"><FolderOpen :size="14" aria-hidden="true" />{{ rule.prefix || t('mirrorAllKeys') }}</span>
              <div class="mirror-rule-source"><span>{{ t('mirrorPrimary') }}</span><span>{{ sourceLabel(rule.source) }}</span></div>
              <div v-if="rule.backup" class="mirror-rule-source"><span>{{ t('mirrorBackupOrigin') }}</span><span>{{ sourceLabel(rule.backup) }}</span></div>
            </div>
            <div class="actions mirror-rule-actions"><button type="button" class="icon-button" :aria-label="t('mirrorMoveUp')" :disabled="locked || index === 0" @click="move(index, -1)"><ArrowUp :size="16" /></button><button type="button" class="icon-button" :aria-label="t('mirrorMoveDown')" :disabled="locked || index === draft.config.rules.length - 1" @click="move(index, 1)"><ArrowDown :size="16" /></button><button type="button" class="icon-button" :aria-label="t('mirrorEdit')" :disabled="locked" @click="edit(rule)"><Pencil :size="16" /></button><button type="button" class="icon-button" :aria-label="t('mirrorRemove')" :disabled="locked" @click="deleting = rule"><Trash2 :size="16" /></button></div>
          </li></ol>
        </section>
        <details class="mirror-advanced"><summary>{{ t('mirrorLimits') }}</summary><div class="form-stack mirror-options"><div class="form-pair">
          <label v-for="field in (['connect_timeout_seconds','header_timeout_seconds','idle_timeout_seconds','negative_ttl_seconds'] as const)" :key="field" class="field"><span>{{ t('mirror_' + field) }}</span><input v-model.number="draft.config[field]" type="number" :min="field === 'negative_ttl_seconds' ? 0 : 1" :max="field === 'connect_timeout_seconds' ? 300 : 3600" step="1" required :disabled="locked"></label>
        </div><label class="field"><span>{{ t('mirrorMaxSize') }}</span><input v-model="size" placeholder="5GiB" :disabled="locked"><small class="field-help">{{ t('mirrorMaxSizeHint') }}</small></label></div></details>
      </template>
      <footer class="mirror-settings-footer"><p class="field-help" role="status">{{ t(dirty ? 'unsavedChanges' : 'mirrorSavedHint') }}</p><div class="actions"><button type="button" :disabled="locked || !dirty" @click="reset()">{{ t('cancel') }}</button><button class="primary" :disabled="locked || !dirty || (draft.config.enabled && !draft.config.rules.length)">{{ t(busy ? 'saving' : 'save') }}</button></div></footer>
    </form>
    <section v-if="draft?.config.enabled" class="bucket-subpanel form-stack">
      <header class="bucket-heading"><span class="chunk-bead sky"><Route :size="18" aria-hidden="true" /></span><div><h3>{{ t('mirrorPreview') }}</h3><p class="field-help">{{ t('mirrorPreviewHint') }}</p></div></header>
      <form class="form-stack" @submit.prevent="check()"><label class="field"><span>{{ t('mirrorObjectKey') }}</span><input v-model="key" required maxlength="1024" placeholder="photos/a.jpg"></label><div class="actions"><button :disabled="checking || !key">{{ t('mirrorPreview') }}</button><button type="button" :disabled="checking || !key" @click="check(true)">{{ t('mirrorTest') }}</button><button v-if="draft.config.rules.some(rule => rule.backup)" type="button" :disabled="checking || !key" @click="check(true, true)">{{ t('mirrorTestBackup') }}</button></div></form>
      <ErrorNotice :error="checkError" />
      <div v-if="preview" class="mirror-path" role="status"><span>{{ key }}</span><ArrowDown :size="16" aria-hidden="true" /><strong>{{ preview.source ? sourceAddress(preview.source) : t('mirrorNoMatch') }}</strong></div>
      <div v-if="probe" class="form-stack" role="status"><div class="bucket-section-head"><span class="badge node-state" data-tone="mint">{{ t('mirrorTestPassed') }}</span><span class="field-help">{{ t(probe.backup ? 'mirrorBackupOrigin' : 'mirrorPrimary') }}</span></div><code class="break-path">{{ sourceAddress(probe.source) }}</code><dl class="bucket-metrics"><div><dt>{{ t('mirrorObjectSize') }}</dt><dd>{{ probe.size == null ? t('mirrorUnknown') : bytes(probe.size) }}</dd></div><div><dt>{{ t('mirrorResponseTime') }}</dt><dd>{{ probe.elapsed_ms }} ms</dd></div></dl><p class="field-help">{{ t('mirrorTestHint') }}</p></div>
    </section>
    <details class="mirror-activity" @toggle="monitoring = ($event.target as HTMLDetailsElement).open">
      <summary><History :size="18" aria-hidden="true" /><span>{{ t('mirrorActivity') }}</span><ChevronDown class="mirror-activity-chevron" :size="16" aria-hidden="true" /></summary>
      <div class="mirror-activity-grid">
        <section class="form-stack mirror-activity-section" :aria-label="t('mirrorTasks')" :aria-busy="tasks.isFetching.value">
          <header class="bucket-section-head"><h3>{{ t('mirrorTasks') }}</h3><button type="button" class="icon-button" :aria-label="t('refresh')" :disabled="tasks.isFetching.value" @click="tasks.refetch()"><RefreshCw :size="16" /></button></header>
          <ErrorNotice :error="tasks.error.value" />
          <div v-if="tasks.isPending.value" class="skeleton-row" />
          <div v-else-if="!tasks.error.value && !tasks.data.value?.items.length" class="mirror-empty"><CloudDownload :size="24" aria-hidden="true" /><div><strong>{{ t('mirrorNoTasks') }}</strong><p>{{ t('mirrorTasksHint') }}</p></div></div>
          <article v-for="job in tasks.data.value?.items" :key="job.id" class="form-stack mirror-fill"><strong class="break-path">{{ (job.detail as any).key }}</strong><MirroringProgress :job="job" /><div v-if="['queued','running','paused','failed'].includes(job.status)" class="actions"><button v-if="['queued','running'].includes(job.status)" :disabled="busy" @click="task(job.id, false)">{{ t('pauseTask') }}</button><button v-else :disabled="busy" @click="task(job.id, true)">{{ t('resumeTask') }}</button></div></article>
          <UiListFooter :first="!!taskCursor" :next="!!tasks.data.value?.next_cursor" @first="taskCursor = ''" @next="taskCursor = tasks.data.value!.next_cursor || ''" />
        </section>
        <section class="form-stack mirror-activity-section" :aria-label="t('mirrorBlocks')" :aria-busy="blocks.isFetching.value">
          <header class="bucket-section-head"><h3>{{ t('mirrorBlocks') }}</h3><button type="button" class="icon-button" :aria-label="t('refresh')" :disabled="blocks.isFetching.value" @click="blocks.refetch()"><RefreshCw :size="16" /></button></header>
          <p class="field-help">{{ t('mirrorBlocksHint') }}</p>
          <ErrorNotice :error="blocks.error.value" />
          <div v-if="blocks.isPending.value" class="skeleton-row" />
          <div v-else-if="!blocks.error.value && !blocks.data.value?.keys.length" class="mirror-empty"><ShieldCheck :size="24" aria-hidden="true" /><p>{{ t('mirrorNoBlocks') }}</p></div>
          <div v-for="value in blocks.data.value?.keys" :key="value" class="mirror-block"><span class="break-path">{{ value }}</span><button :disabled="locked" @click="unblocking = value">{{ t('mirrorUnblock') }}</button></div>
          <UiListFooter :first="!!blockCursor" :next="!!blocks.data.value?.next" @first="blockCursor = ''" @next="blockCursor = blocks.data.value!.next || ''" />
        </section>
      </div>
    </details>
  </section>
  <UiDialog v-model:open="editorOpen" :title="t('mirrorEditRule')" medium @open-auto-focus.prevent="ruleName?.focus()">
    <form v-if="editing" id="mirroring-rule" class="form-stack" @submit.prevent="apply">
      <ErrorNotice :error="editorError || external.error.value" />
      <label class="field"><span>{{ t('mirrorRuleName') }}</span><input ref="ruleName" v-model="editing.name" required maxlength="128"></label>
      <section class="bucket-subpanel form-stack"><h3>{{ t('mirrorMapping') }}</h3><div class="form-pair"><label class="field"><span>{{ t('mirrorPrefix') }}</span><input v-model="editing.prefix" maxlength="1024" placeholder="media/" spellcheck="false"></label><label class="field"><span>{{ t('mirrorAddPrefix') }}</span><input v-model="editing.add_prefix" maxlength="1024" placeholder="archive/" spellcheck="false"></label></div><p class="field-help">{{ t('mirrorMappingHint') }}</p><UiCheckbox :model-value="!!editing.strip_prefix" @update:model-value="editing.strip_prefix = $event" :label="t('mirrorStripPrefix')" /></section>
      <UiCheckbox :model-value="!!editing.public_read" @update:model-value="editing.public_read = $event" :label="t('mirrorPublic')" :disabled="!props.bucket.actions.includes('object.acl')" />
      <section class="bucket-subpanel form-stack mirror-editor-source"><h3>{{ t('mirrorPrimary') }}</h3><MirroringSource ref="primary" v-model="editing.source" :options="sources" :admin="admin" /></section>
      <UiCheckbox :model-value="!!editing.backup" @update:model-value="editing.backup = $event ? http() : null" :label="t('mirrorBackup')" />
      <section v-if="editing.backup" class="bucket-subpanel form-stack mirror-editor-source"><h3>{{ t('mirrorBackupOrigin') }}</h3><MirroringSource ref="backup" v-model="editing.backup" :options="sources" :admin="admin" /></section>
      <button v-if="external.hasNextPage.value" type="button" :disabled="external.isFetchingNextPage.value" @click="external.fetchNextPage()">{{ t('mirrorMoreSources') }}</button>
    </form>
    <template #footer><button @click="editorOpen = false">{{ t('cancel') }}</button><button type="submit" form="mirroring-rule" class="primary">{{ t('mirrorApplyRule') }}</button></template>
  </UiDialog>
  <UiDialog :open="!!deleting" @update:open="!$event && (deleting = undefined)" :title="t('mirrorRemoveRule')" :description="t('mirrorRemoveRuleHint')"><template #footer><button @click="deleting = undefined">{{ t('cancel') }}</button><button class="danger" @click="draft!.config.rules = draft!.config.rules.filter(rule => rule.id !== deleting!.id); deleting = undefined">{{ t('mirrorRemove') }}</button></template></UiDialog>
  <UiDialog :open="unblocking != null" @update:open="!$event && (unblocking = undefined)" :busy="busy" :title="t('mirrorUnblock')" :description="t('mirrorUnblockHint')"><p class="break-path">{{ unblocking }}</p><p v-if="error" class="error">{{ errorText(error) }}</p><template #footer><button :disabled="busy" @click="unblocking = undefined">{{ t('cancel') }}</button><button class="primary" :disabled="busy" @click="unblock">{{ t('mirrorUnblock') }}</button></template></UiDialog>
</template>
<style scoped>
.mirror-panel :is(h3, p) { margin: 0; }
.mirror-panel h3 { font-size: 15px; }
.mirror-settings { gap: 20px; }
.mirror-toggle { display: flex; align-items: center; justify-content: space-between; gap: 16px; }
.mirror-toggle > div { display: grid; gap: 6px; min-width: 0; }
.mirror-toggle > .ui-switch { flex: none; }
.mirror-rule-section, .mirror-advanced { border-top: 1px solid var(--line-soft); padding-top: 20px; }
.mirror-rule-section header > div { display: grid; min-width: 0; gap: 6px; flex: 1 1 240px; }
.mirror-settings-footer { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px 20px; padding-top: 16px; border-top: 1px solid var(--line-soft); }
.mirror-settings-footer > p { flex: 1 1 180px; }
.mirror-settings-footer > .actions { margin-left: auto; }
.mirror-options { margin-top: 16px; }
.mirror-rules { list-style: none; padding: 0; margin: 0; border: 1px solid var(--line-soft); border-radius: 12px; background: var(--surface); }
.mirror-rules li { display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 12px; align-items: start; padding: 16px; }
.mirror-rules li + li { border-top: 1px solid var(--line-soft); }
.mirror-rule-order { display: grid; place-items: center; width: 28px; height: 28px; border-radius: 9px; color: var(--sky); background: var(--sky-soft); font-variant-numeric: tabular-nums; }
.mirror-rule-text { display: grid; gap: 8px; min-width: 0; overflow-wrap: anywhere; }
.mirror-rule-prefix { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: var(--small-size, 12px); white-space: pre-wrap; }
.mirror-rule-prefix > svg { flex: none; }
.mirror-rule-source { display: flex; flex-wrap: wrap; gap: 4px 12px; font-size: var(--small-size, 12px); white-space: pre-wrap; }
.mirror-rule-source > span:first-child { color: var(--muted); flex: none; }
.mirror-rule-source > span:last-child { min-width: 0; }
.mirror-rule-actions { flex-wrap: nowrap; }
.mirror-path { display: grid; gap: 10px; padding: 16px; border-radius: 12px; background: var(--sky-soft); overflow-wrap: anywhere; white-space: pre-wrap; }
.mirror-path > :is(span, svg) { color: var(--muted); }
.mirror-activity { min-width: 0; border: 1px solid var(--line-soft); border-radius: 16px; }
.mirror-activity > summary { display: flex; align-items: center; gap: 10px; padding: 18px 20px; color: var(--ink); font-weight: var(--ui-weight, 700); list-style: none; }
.mirror-activity > summary::-webkit-details-marker { display: none; }
.mirror-activity > summary > svg { flex: none; color: var(--muted); }
.mirror-activity-chevron { margin-left: auto; transform: rotate(-90deg); }
.mirror-activity[open] .mirror-activity-chevron { transform: none; }
.mirror-activity-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); border-top: 1px solid var(--line-soft); }
.mirror-activity-section { padding: 20px; }
.mirror-activity-section + .mirror-activity-section { border-left: 1px solid var(--line-soft); }
.mirror-empty { display: flex; align-items: center; gap: 12px; min-width: 0; padding: 20px 16px; border-radius: 12px; background: var(--surface-soft); color: var(--muted); }
.mirror-empty > svg { flex: none; color: var(--sky); }
.mirror-empty > div { display: grid; gap: 6px; }
.mirror-empty strong { color: var(--ink); font-weight: var(--ui-weight, 700); }
.mirror-empty p { font-size: var(--small-size, 12px); line-height: 1.65; }
.mirror-fill { gap: 12px; padding: 16px 0; border-bottom: 1px solid var(--line-soft); }
.mirror-fill:last-of-type { border-bottom: 0; padding-bottom: 0; }
.mirror-block { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.mirror-block > button { flex: none; }
.mirror-editor-source > h3 { font-size: 15px; }
@media (max-width: 900px) { .mirror-activity-grid { grid-template-columns: minmax(0, 1fr); } .mirror-activity-section + .mirror-activity-section { border-left: 0; border-top: 1px solid var(--line-soft); } }
@media (max-width: 640px) { .mirror-rules li { grid-template-columns: auto minmax(0, 1fr); } .mirror-rule-actions { grid-column: 2; justify-content: flex-end; } .mirror-activity-section { padding: 16px; } }
@media (max-width: 480px) { .mirror-block { flex-wrap: wrap; } .mirror-settings-footer > p { flex-basis: 100%; } .mirror-empty { align-items: flex-start; padding: 16px 12px; } }
</style>
