<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ArchiveRestore, Clock3, Eye, Trash2 } from 'lucide-vue-next'
import { api, bytes, params, write, type Bucket } from '../api/client'
import { t, date } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'
import UiDialog from './ui/UiDialog.vue'
import UiListFooter from './ui/UiListFooter.vue'
import type { components } from '../api/schema'
const props = defineProps<{ bucket: Bucket; objectKey?: string }>()
const emit = defineEmits<{ changed: [] }>()
type Page = components['schemas']['VersionPage']
type Row = Page['rows'][number]
const prefix = ref(''), cursor = ref<{ key?: string; version?: string }>({}), busy = ref(false), error = ref<unknown>()
const action = ref<{ row: Row; restore: boolean }>()
const confirming = computed({ get: () => !!action.value, set: v => { if (!v) action.value = undefined } })
const base = computed(() => '/api/v1/buckets/' + props.bucket.id + '/versions')
const filter = computed(() => props.objectKey ?? prefix.value)
const query = useQuery({ queryKey: ['versions', base, filter, cursor], queryFn: ({ signal }) => api<Page>(base.value + '?' + params({prefix: filter.value, max_keys: '25', key_marker: cursor.value.key, version_marker: cursor.value.version}), { signal }) })
const rows = computed(() => query.data.value?.rows.filter(r => props.objectKey === undefined || r.key === props.objectKey) || [])
watch([filter, base], () => { cursor.value = {} })
function next() { const p = query.data.value; if (p?.next_key) cursor.value = { key: p.next_key, version: p.next_version || undefined } }
function args(row: Row) { return { key: row.key, version_id: row.version_id } }
async function apply() {
  if (!action.value) return
  busy.value = true; error.value = undefined
  try { const {row, restore} = action.value
    if (restore) await write(base.value + '/restore', args(row)); else await write(base.value + '?' + params(args(row)), undefined, 'DELETE')
    action.value = undefined; await query.refetch(); emit('changed')
  } catch (e) { error.value = e } finally { busy.value = false }
}
</script>
<template>
  <div class="version-history">
    <label v-if="objectKey === undefined" class="field"><span>{{ t('versionPrefix') }}</span><input v-model="prefix" :placeholder="t('versionAllKeys')" /></label>
    <ErrorNotice v-if="query.error.value" :error="query.error.value" :title="t(query.data.value ? 'versionRefreshFailed' : 'versionLoadFailed')" :retry="() => query.refetch()" :retrying="query.isFetching.value" />
    <ErrorNotice v-if="!action && error" :error="error" />
    <p v-if="query.isPending.value">{{ t('loading') }}</p><p v-else-if="query.isSuccess.value && !rows.length" class="field-help">{{ t('versionEmpty') }}</p>
    <article v-for="row in rows" :key="row.key + ':' + row.version_id" class="version-row">
      <div class="version-title"><Clock3 :size="16"/><strong>{{ objectKey ? date(new Date(row.modified_us / 1000).toISOString()) : row.key }}</strong><span v-if="row.latest" class="badge">{{ t('versionCurrent') }}</span></div>
      <small>{{ row.size == null ? t('versionMarker') : bytes(row.size) }} · <code>{{ row.version_id }}</code></small>
      <div class="version-actions">
        <a v-if="row.size != null && bucket.actions.includes('object.versions.read')" class="button" :href="base + '/content?' + params(args(row))" target="_blank" rel="noopener"><Eye :size="14"/>{{ t('preview') }}</a>
        <button v-if="row.size != null && !row.latest && bucket.actions.includes('object.versions.restore') && bucket.actions.includes('object.versions.read') && bucket.actions.includes('object.write')" @click="action = {row, restore: true}"><ArchiveRestore :size="14"/>{{ t('versionRestore') }}</button>
        <button v-if="bucket.actions.includes('object.versions.delete')" @click="action = {row, restore: false}"><Trash2 :size="14"/>{{ t('versionRemove') }}</button>
      </div>
    </article>
    <UiListFooter v-if="query.data.value" :first="!!cursor.key" :next="!!query.data.value.next_key && (!objectKey || query.data.value.next_key === objectKey)" :summary="t('pageItems', {count: rows.length})" @first="cursor = {}" @next="next"/>
    <UiDialog v-model:open="confirming" :busy="busy" :title="t(action?.restore ? 'versionRestore' : 'versionRemove')" :description="t(action?.restore ? 'versionRestoreHint' : 'versionRemoveHint')"><p class="path-text">{{ action?.row.key }}</p><code>{{ action?.row.version_id }}</code><ErrorNotice v-if="error" :error="error"/><template #footer><button :disabled="busy" @click="confirming = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy" @click="apply">{{ t(action?.restore ? 'versionRestore' : 'versionRemove') }}</button></template></UiDialog>
  </div>
</template>
<style scoped>
.version-history{display:grid;gap:.8rem;min-width:0}.version-row{display:grid;gap:.55rem;padding:.8rem 0;border-bottom:1px solid var(--line)}.version-title,.version-actions{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}.version-title strong,.version-row code{overflow-wrap:anywhere}.version-row small{color:var(--muted)}.version-actions .button,.version-actions button{min-height:2rem;padding:.3rem .65rem}
</style>
