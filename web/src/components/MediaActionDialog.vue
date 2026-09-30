<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { onBeforeRouteLeave } from 'vue-router'
import { ArrowRight, Check, CircleAlert, LoaderCircle, Plus, Trash2 } from 'lucide-vue-next'
import { api, ApiError, params, queries, uuid, write, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import type { MediaAction, MediaItem } from '../app/media'
import { errorText } from '../app/feedback'
import { t } from '../app/i18n'
import UiDialog from './ui/UiDialog.vue'
import UiSelect from './ui/UiSelect.vue'
import UiCheckbox from './ui/UiCheckbox.vue'
import FileIcon from './FileIcon.vue'

type Metadata = components['schemas']['ObjectMetadata']
type Entry = { item: components['schemas']['Item']; outcome?: components['schemas']['ItemResult'] }
const props = defineProps<{ bucket: Bucket; buckets: Bucket[]; items: MediaItem[]; action: MediaAction; prefix: string }>()
const emit = defineEmits<{ changed: [] }>()
const open = defineModel<boolean>('open', { required: true })
const busy = ref(false), loading = ref(false), attempted = ref(false), error = ref<unknown>(), rows = ref<Entry[]>([])
const target = ref(''), path = ref(''), overwrite = ref(false), publicRead = ref(false)
const meta = reactive<Record<string, string>>({}), custom = ref<{ key: string; value: string }[]>([])
const fields = ['content_type', 'cache_control', 'content_disposition', 'content_encoding', 'content_language', 'expires'] as const
const snapshot = ref(''), initialized = ref(false), discardOpen = ref(false)
let pendingLeave: ((value: boolean) => void) | undefined
const copyMove = computed(() => ['copy', 'move'].includes(props.action))
const current = computed(() => props.buckets.find(bucket => bucket.id === target.value))
const options = computed(() => props.buckets.filter(bucket => bucket.state === 'active' && bucket.actions.includes('object.write') && (props.action !== 'move' || !props.items.some(item => item.public_read) || bucket.actions.includes('object.acl'))).map(bucket => ({ value: bucket.id, label: bucket.name })))
const state = () => JSON.stringify([target.value, path.value, overwrite.value, publicRead.value, meta, custom.value])
const dirty = computed(() => open.value && initialized.value && !attempted.value && state() !== snapshot.value)
const completed = computed(() => rows.value.filter(row => row.outcome?.status === 200).length)
const failures = computed(() => rows.value.filter(row => row.outcome && row.outcome.status !== 200).length)
const unconfirmed = computed(() => rows.value.some(row => !row.outcome))
function closing(value: boolean) {
  if (value || busy.value) return
  if (dirty.value) discardOpen.value = true; else open.value = false
}
function discard(value: boolean) { if (value && !pendingLeave) open.value = false; pendingLeave?.(value); pendingLeave = undefined; discardOpen.value = false }
watch(discardOpen, value => { if (!value && pendingLeave) discard(false) })
onBeforeRouteLeave(() => busy.value ? false : !dirty.value || new Promise<boolean>(resolve => { pendingLeave = resolve; discardOpen.value = true }))
function unloading(event: BeforeUnloadEvent) { if (busy.value || dirty.value) { event.preventDefault(); event.returnValue = '' } }
window.addEventListener('beforeunload', unloading)
onBeforeUnmount(() => { window.removeEventListener('beforeunload', unloading); pendingLeave?.(false) })
watch(open, async (value, _, cleanup) => {
  if (!value) return
  const controller = new AbortController(); cleanup(() => controller.abort())
  initialized.value = false; attempted.value = false; rows.value = []; error.value = undefined
  target.value = options.value.some(option => option.value === props.bucket.id) ? props.bucket.id : options.value[0]?.value || ''
  path.value = props.items.length === 1 ? props.items[0]!.object_key : props.prefix; overwrite.value = false; publicRead.value = false
  for (const field of fields) meta[field] = ''
  custom.value = []
  if (props.action === 'metadata' && props.items[0]) {
    loading.value = true
    try {
      const detail = await api<components['schemas']['ObjectDetail']>(`/api/buckets/${props.bucket.id}/object?` + params({ key: props.items[0].object_key, version: props.items[0].id }), { signal: controller.signal })
      for (const field of fields) meta[field] = detail.metadata[field] || ''
      custom.value = Object.entries(detail.metadata.user || {}).map(([key, value]) => ({ key, value }))
    } catch (failure) { if (!controller.signal.aborted) error.value = failure; return }
    finally { loading.value = false }
  }
  if (!controller.signal.aborted) { snapshot.value = state(); initialized.value = true }
}, { immediate: true })
watch(target, () => { if (!current.value?.actions.includes('object.acl')) publicRead.value = false; if (!current.value?.actions.includes('bucket.list')) overwrite.value = false })
function destination(item: MediaItem) { return props.items.length === 1 ? path.value : (path.value && !path.value.endsWith('/') ? path.value + '/' : path.value) + item.object_key.slice(props.prefix.length) }
function metadata(): Metadata {
  const names = custom.value.map(pair => pair.key.trim())
  if (names.some(name => !name) || new Set(names).size !== names.length) throw new ApiError(400, 'InvalidArgument')
  return { ...Object.fromEntries(fields.map(field => [field, meta[field] || null])), user: Object.fromEntries(custom.value.map((pair, index) => [names[index]!, pair.value])) }
}
async function execute(retryFailed = false) {
  busy.value = true; error.value = undefined
  try {
    const value = props.action === 'metadata' ? metadata() : undefined
    if (!attempted.value) {
      const entries: Entry[] = []
      for (const source of props.items) {
        const item: components['schemas']['Item'] = { client_id: uuid(), key: source.object_key, version: source.id }
        if (copyMove.value) {
          item.target_key = destination(source)
          if (!target.value || !item.target_key || target.value === props.bucket.id && item.target_key === source.object_key) throw new ApiError(400, 'InvalidArgument')
          if (overwrite.value) {
            const page = await api<components['schemas']['MediaPage']>(`/api/buckets/${target.value}/objects?` + params({ q: item.target_key, mode: 'exact' }))
            item.target_version = page.objects[0]?.id || null
          }
        }
        entries.push({ item })
      }
      if (copyMove.value && new Set(entries.map(entry => entry.item.target_key)).size !== entries.length) throw new ApiError(400, 'InvalidArgument')
      rows.value = entries; attempted.value = true
    }
    if (retryFailed) for (const row of rows.value) if (row.outcome && row.outcome.status !== 200) { row.item.client_id = uuid(); row.outcome = undefined }
    const pending = rows.value.filter(row => !row.outcome)
    for (let start = 0; start < pending.length; start += 20) {
      const group = pending.slice(start, start + 20)
      const result = await write<components['schemas']['BatchResult']>('/api/media/actions', { bucket: props.bucket.id, action: props.action,
        ...(copyMove.value ? { target_bucket: target.value } : {}), ...(props.action === 'copy' ? { public_read: publicRead.value } : {}),
        ...(value ? { metadata: value } : {}), objects: group.map(row => row.item) })
      for (const row of group) row.outcome = result.results.find(outcome => outcome.client_id === row.item.client_id)
    }
  } catch (failure) { error.value = failure }
  finally {
    busy.value = false
    if (attempted.value) { await Promise.all([queries.invalidateQueries({ queryKey: ['objects'] }), queries.invalidateQueries({ queryKey: ['quota'] })]); emit('changed') }
  }
}
function explain(row: Entry) { return row.outcome ? errorText(new ApiError(row.outcome.status, row.outcome.code || 'RequestFailed')) : t('actionUnconfirmed') }
</script>
<template>
  <UiDialog :open="open" @update:open="closing" :title="t('mediaAction_' + action)" :description="t('actionHint_' + action)" :busy="busy || loading">
    <p v-if="error" class="error" role="alert">{{ errorText(error) }}</p>
    <div v-if="loading" class="skeleton-row" role="status" :aria-label="t('opening')" />
    <template v-else>
      <fieldset v-if="copyMove && !attempted" class="action-fields" :disabled="busy || attempted"><label class="field">{{ t('targetBucket') }}<UiSelect v-model="target" :disabled="attempted" :label="t('targetBucket')" :options="options" /></label><label class="field">{{ t(items.length === 1 ? 'targetPath' : 'targetFolder') }}<input v-model="path" maxlength="1024" :aria-label="t(items.length === 1 ? 'targetPath' : 'targetFolder')"></label><UiCheckbox v-if="current?.actions.includes('bucket.list')" v-model="overwrite" :disabled="attempted" :label="t('actionOverwrite')" /><UiCheckbox v-if="action === 'copy' && current?.actions.includes('object.acl')" v-model="publicRead" :disabled="attempted" :label="t('actionPublicCopy')" /><p v-if="action === 'move'" class="field-help">{{ t('moveAccessHint') }}</p></fieldset>
      <fieldset v-if="action === 'metadata' && !attempted" class="action-fields" :disabled="busy || attempted"><div class="metadata-editor"><label v-for="field in fields" :key="field" class="field">{{ t('metadata_' + field) }}<input v-model="meta[field]" maxlength="2048"></label></div><h3>{{ t('customMetadata') }}</h3><div v-for="(pair, index) in custom" :key="index" class="metadata-pair"><input v-model="pair.key" :aria-label="t('metadataName')" maxlength="256"><input v-model="pair.value" :aria-label="t('metadataValue')" maxlength="2048"><button class="icon-button" :aria-label="t('removeField')" @click="custom.splice(index, 1)"><Trash2 :size="15" /></button></div><button @click="custom.push({ key: '', value: '' })"><Plus :size="15" />{{ t('addField') }}</button><p class="field-help">{{ t('metadataHint') }}</p></fieldset>
      <p v-if="copyMove && attempted" class="field-help break-anywhere">{{ current?.name }} · {{ path }}</p><div v-if="attempted" class="action-progress" role="status"><span class="status-orb" :class="{ warning: failures }"><LoaderCircle v-if="busy" class="spinning" :size="22" /><CircleAlert v-else-if="failures || unconfirmed" :size="22" /><Check v-else :size="22" /></span><div><strong>{{ t('actionProgress', { completed, total: rows.length }) }}</strong><p>{{ t(unconfirmed ? 'actionRetryHint' : failures ? 'actionFailureHint' : 'actionDoneHint') }}</p></div></div>
      <div class="action-object-list"><div v-for="(item, index) in items" :key="item.id" class="action-object"><FileIcon :name="item.object_key" /><div><strong>{{ item.object_key }}</strong><span v-if="copyMove"><ArrowRight :size="12" />{{ destination(item) }}</span><p v-if="rows[index]?.outcome?.status !== 200 && attempted" class="field-help">{{ explain(rows[index]!) }}</p></div><Check v-if="rows[index]?.outcome?.status === 200" class="success-text" :size="17" /><CircleAlert v-else-if="rows[index]?.outcome" class="danger" :size="17" /></div></div>
    </template>
    <template #footer><button :disabled="busy" @click="closing(false)">{{ t(attempted ? 'close' : 'cancel') }}</button><button v-if="!attempted" :class="action === 'delete' ? 'danger' : 'primary'" :disabled="busy || loading || !initialized || (copyMove && !target)" @click="execute()">{{ t(busy ? 'saving' : 'mediaAction_' + action) }}</button><button v-else-if="unconfirmed" class="primary" :disabled="busy" @click="execute()">{{ t('resumeAction') }}</button><button v-else-if="failures" class="primary" :disabled="busy" @click="execute(true)">{{ t('retryFailed') }}</button></template>
  </UiDialog>
  <UiDialog v-model:open="discardOpen" :title="t('discardTitle')" :description="t('actionDiscardHint')"><template #footer><button @click="discard(false)">{{ t('keepEditing') }}</button><button class="primary" @click="discard(true)">{{ t('discard') }}</button></template></UiDialog>
</template>
