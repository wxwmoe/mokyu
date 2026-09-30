<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { localValue, saveLocal } from '../app/theme'
import { RefreshCw, ArrowUpRight, ChevronRight, LockKeyhole, Globe, Home, Heart, UploadCloud, LayoutGrid, List, X } from 'lucide-vue-next'
import { api, bytes, params, session, type Bucket } from '../api/client'
import { notify, report, errorText } from '../app/feedback'
import { t, date } from '../app/i18n'
import UiSelect from '../components/ui/UiSelect.vue'
import ObjectViewer from '../components/ObjectViewer.vue'
import MediaThumbnail from '../components/MediaThumbnail.vue'
import FileIcon from '../components/FileIcon.vue'
import EmptyState from '../components/EmptyState.vue'
import QuotaPanel from '../components/QuotaPanel.vue'
import UploadDialog from '../components/UploadDialog.vue'
import MediaFilters from '../components/MediaFilters.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import MediaActionsMenu from '../components/MediaActionsMenu.vue'
import MediaActionDialog from '../components/MediaActionDialog.vue'
import type { MediaAction } from '../app/media'
import type { components } from '../api/schema'
import CreateBucket from '../components/CreateBucket.vue'

type MediaObject = components['schemas']['MediaItem']
type ObjectPage = components['schemas']['MediaPage']
const route = useRoute(), router = useRouter()
const bucket = computed(() => String(route.params.bucket || ''))
const prefix = computed(() => String(route.query.prefix || ''))
const cursor = computed(() => String(route.query.cursor || ''))
const pageSize = computed({ get: () => ['25', '50', '100'].includes(String(route.query.limit)) ? String(route.query.limit) : '100',
  set: value => { router.replace({ query: { ...route.query, limit: value, cursor: undefined } }) } })
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const current = computed(() => buckets.data.value?.find(item => item.id === bucket.value))
const query = computed(() => {
  const values: Record<string, string | undefined> = { prefix: prefix.value, after: cursor.value || undefined, limit: pageSize.value }
  for (const key of ['q', 'mode', 'search_in', 'kind', 'public', 'min_size', 'max_size', 'recursive', 'sort', 'order']) if (route.query[key]) values[key] = String(route.query[key])
  for (const [key, apiKey] of [['from', 'since'], ['to', 'until']] as const) {
    const value = String(route.query[key] || '')
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) { const time = new Date(value + (key === 'from' ? 'T00:00:00' : 'T23:59:59.999')); if (Number.isFinite(time.getTime())) values[apiKey] = time.toISOString() }
  }
  return params(values)
})
const objects = useQuery({ queryKey: ['objects', bucket, query], enabled: computed(() => !!bucket.value),
  queryFn: ({ signal }) => api<ObjectPage>(`/api/buckets/${bucket.value}/objects?${query.value}`, { signal }) })
const catalog = useQuery({ queryKey: ['catalog', bucket], enabled: computed(() => !!bucket.value), queryFn: ({ signal }) => api<components['schemas']['CatalogStatus']>(`/api/buckets/${bucket.value}/catalog`, { signal }), refetchInterval: query => query.state.data?.phase === 'ready' ? false : 5000 })
const ready = computed(() => catalog.data.value?.phase === 'ready' || objects.data.value?.index_ready === true)
watch(ready, value => { if (value) objects.refetch() })
const selected = ref<MediaObject | null>(null)
const checked = ref(new Set<string>()), selectionShift = ref(false)
let anchor = -1
const picks = computed(() => objects.data.value?.objects.filter(item => checked.value.has(item.id)) || [])
const all = computed({ get: () => !!objects.data.value?.objects.length && picks.value.length === objects.data.value.objects.length, set: value => { checked.value = new Set(value ? objects.data.value?.objects.map(item => item.id) : []); anchor = -1 } })
const actionOpen = ref(false), action = ref<MediaAction>('copy'), actionItems = ref<MediaObject[]>([])
function pick(item: MediaObject, value: boolean) {
  const items = objects.data.value?.objects || [], index = items.indexOf(item), next = new Set(checked.value)
  const range = selectionShift.value && anchor >= 0 ? items.slice(Math.min(anchor, index), Math.max(anchor, index) + 1) : [item]
  for (const entry of range) { if (value) next.add(entry.id); else next.delete(entry.id) }
  checked.value = next; anchor = index; selectionShift.value = false
}
function operate(kind: MediaAction, items = picks.value) { action.value = kind; actionItems.value = [...items]; selected.value = null; actionOpen.value = true }
function changed() { checked.value = new Set(); objects.refetch() }
const uploading = ref(false)
const layout = ref(localValue('mokyu.mediaLayout') === 'gallery' ? 'gallery' : 'list')
watch(layout, value => saveLocal('mokyu.mediaLayout', value))
const selectedIndex = computed(() => objects.data.value?.objects.findIndex(item => item.id === selected.value?.id) ?? -1)
watch(() => [bucket.value, route.query.object, route.query.version], async (_, __, cleanup) => {
  if (!bucket.value || !route.query.object) return
  const controller = new AbortController(); cleanup(() => controller.abort())
  try {
    const result = await api<ObjectPage>(`/api/buckets/${bucket.value}/objects?` + params({ q: String(route.query.object), mode: 'exact' }), { signal: controller.signal })
    if (result.objects[0]) selected.value = { ...result.objects[0], id: String(route.query.version || result.objects[0].id) }
    else report(new Error(t('objectMissing')))
  } catch (error) { if (!controller.signal.aborted) report(error) }
}, { immediate: true })
function close() { selected.value = null; if (route.query.object) router.replace({ query: { ...route.query, object: undefined, version: undefined } }) }
const sizes = computed(() => ['25', '50', '100'].map(value => ({ value, label: t('perPage', { count: value }) })))
watch([bucket, query], () => { selected.value = null; checked.value = new Set(); anchor = -1 })
function folder(value: string) { selected.value = null; router.push({ query: { prefix: value || undefined } }) }
function step(delta: number) { selected.value = objects.data.value?.objects[selectedIndex.value + delta] || selected.value }
async function refresh() {
  const result = await (bucket.value ? objects.refetch() : buckets.refetch())
  if (result.error) report(result.error); else notify(t('refreshed'))
}
</script>

<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('collection') }}</p><h1>{{ current?.name || t('media') }}</h1><p>{{ t('everything') }}</p></div><div class="heading-actions"><CreateBucket v-if="!bucket" @created="router.push('/media/' + $event.id)" /><RouterLink v-if="current?.actions.includes('bucket.settings')" class="button" :to="'/buckets/' + bucket">{{ t('buckets') }}</RouterLink><button :disabled="buckets.isFetching.value || (bucket !== '' && objects.isFetching.value)" @click="refresh"><RefreshCw :size="16" />{{ t('refresh') }}</button><button v-if="current?.state === 'active' && current.actions.includes('object.write')" :disabled="current?.uploads_paused || current?.state !== 'active'" class="primary" @click="uploading = true"><UploadCloud :size="16" />{{ t('uploadTitle') }}</button></div></div>
  <ErrorNotice v-if="buckets.error.value || objects.error.value" :error="buckets.error.value || objects.error.value" />
  <QuotaPanel v-if="current?.actions.includes('storage.inspect')" kind="bucket" :id="current.id" />
  <div v-if="!bucket" class="bucket-grid">
    <RouterLink v-for="item in buckets.data.value" :key="item.id" class="bucket-card" :to="`/media/${item.id}`"><img src="/assets/mokyu-pack.svg" alt="" width="64" height="64"><h2>{{ item.name }}</h2><span>{{ t(item.state) }}</span><ArrowUpRight class="bucket-arrow" :size="19" /></RouterLink>
    <EmptyState v-if="!buckets.isPending.value && !buckets.error.value && !buckets.data.value?.length" :title="t('fresh')" :description="t(session?.role === 'admin' ? 'firstBucket' : 'askForBucket')" />
  </div>
  <section v-else class="surface">
    <div class="media-toolbar"><nav class="breadcrumbs" :aria-label="t('folderPath')"><RouterLink to="/media"><Home :size="15" />{{ t('media') }}</RouterLink><ChevronRight :size="13" /><button @click="folder('')">{{ current?.name }}</button><span v-if="prefix" class="path-text">/ {{ prefix }}</span></nav>
      <div class="media-view-options"><div class="view-switch" :aria-label="t('mediaLayout')"><button :aria-label="t('listView')" :aria-pressed="layout === 'list'" @click="layout = 'list'"><List :size="17" /></button><button :aria-label="t('galleryView')" :aria-pressed="layout === 'gallery'" @click="layout = 'gallery'"><LayoutGrid :size="17" /></button></div><UiSelect v-model="pageSize" :label="t('pageSize')" :options="sizes" /></div>
    </div>
    <MediaFilters :ready="ready" /><div v-if="objects.data.value?.objects.length" class="selection-heading"><UiCheckbox v-model="all" :label="t('selectPage')" /><span>{{ t('selectionHint') }}</span></div>
    <p v-if="!ready && catalog.data.value" class="info-callout">{{ t('catalogBuilding') }}</p>
    <p v-if="objects.data.value?.search_mode === 'prefix' && route.query.q && (!route.query.mode || route.query.mode === 'contains')" class="field-help catalog-note">{{ t('shortSearchHint') }}</p>
    <p v-else-if="objects.data.value?.layout === 'flat'" class="field-help catalog-note">{{ t('flatResults') }}</p>
    <div v-if="objects.isPending.value" role="status"><span class="sr-only">{{ t('opening') }}</span><div v-for="i in 4" :key="i" class="skeleton-row" /></div>
    <div v-else-if="layout === 'list'" class="table-scroll"><table><thead><tr><th class="selection-column"><span class="sr-only">{{ t('selectObjects') }}</span></th><th>{{ t('name') }}</th><th>{{ t('size') }}</th><th>{{ t('access') }}</th><th>{{ t('updated') }}</th></tr></thead><tbody>
      <tr v-for="name in objects.data.value?.prefixes" :key="name"><td colspan="5"><button class="text-button file-name" @click="folder(name)"><FileIcon :name="name" folder /><span>{{ name.slice(prefix.length) }}</span></button></td></tr>
      <tr v-for="item in objects.data.value?.objects" :key="item.id" :class="{ selected: item.id === selected?.id || checked.has(item.id) }"><td class="selection-column"><UiCheckbox class="item-check" :model-value="checked.has(item.id)" :label="t('selectObject', { name: item.object_key })" @click.capture="selectionShift = $event.shiftKey" @keydown.capture="selectionShift = $event.shiftKey" @update:model-value="pick(item, $event)" /></td><td><button class="text-button file-name" @click="selected = item"><FileIcon :name="item.object_key" /><span>{{ item.object_key.slice(prefix.length) }}</span></button></td><td>{{ bytes(Number(item.size)) }}</td><td><span class="badge" :class="{ public: item.public_read }"><Globe v-if="item.public_read" :size="11" /><LockKeyhole v-else :size="11" />{{ t(item.public_read ? 'public' : 'private') }}</span></td><td>{{ date(item.modified_at) }}</td></tr>
    </tbody></table></div>
    <div v-else class="media-gallery"><button v-for="name in objects.data.value?.prefixes" :key="name" class="gallery-card gallery-folder" @click="folder(name)"><FileIcon :name="name" folder large /><strong>{{ name.slice(prefix.length) }}</strong><span>{{ t('folder') }}</span></button><div v-for="item in objects.data.value?.objects" :key="item.id" class="gallery-entry" :class="{ checked: checked.has(item.id) }"><UiCheckbox class="item-check gallery-check" :model-value="checked.has(item.id)" :label="t('selectObject', { name: item.object_key })" @click.capture="selectionShift = $event.shiftKey" @keydown.capture="selectionShift = $event.shiftKey" @update:model-value="pick(item, $event)" /><button class="gallery-card" @click="selected = item"><div class="gallery-preview"><MediaThumbnail :bucket="bucket" :item="item" :allowed="current?.actions.includes('object.read') || false" large /><span class="gallery-access" :aria-label="t(item.public_read ? 'public' : 'private')"><Globe v-if="item.public_read" :size="13" /><LockKeyhole v-else :size="13" /></span></div><strong>{{ item.object_key.slice(prefix.length) }}</strong><span>{{ bytes(Number(item.size)) }}</span></button></div></div>
    <EmptyState v-if="objects.data.value && !objects.data.value.objects.length && !objects.data.value.prefixes.length" compact :title="t('emptyFolder')" :description="t('folderReady')" />
    <div v-if="objects.data.value" class="table-footer"><span>{{ t('pageItems', { count: objects.data.value.objects.length + objects.data.value.prefixes.length }) }}</span><button v-if="cursor" @click="router.push({ query: { ...route.query, cursor: undefined } })">{{ t('firstPage') }}</button><button v-if="objects.data.value.next" @click="router.push({ query: { ...route.query, cursor: objects.data.value.next } })">{{ t('nextPage') }}<ChevronRight :size="16" /></button><Heart v-else :size="14" /></div>
  </section>
  <div v-if="picks.length && current" class="selection-bar"><span class="selection-count">{{ picks.length }}</span><strong>{{ t('selectedObjects', { count: picks.length }) }}</strong><MediaActionsMenu :bucket="current" :items="picks" @action="operate($event)" /><button class="icon-button" :aria-label="t('clearSelection')" @click="checked = new Set()"><X :size="17" /></button></div>
  <ObjectViewer v-if="current" :bucket="current" :item="selected" :previous="selectedIndex > 0" :next="selectedIndex >= 0 && selectedIndex < (objects.data.value?.objects.length || 0) - 1" @close="close" @step="step" @action="operate($event, selected ? [selected] : [])" />
  <MediaActionDialog v-if="current" v-model:open="actionOpen" :bucket="current" :buckets="buckets.data.value || []" :items="actionItems" :action="action" :prefix="prefix" @changed="changed" />
  <UploadDialog v-model:open="uploading" :buckets="buckets.data.value || []" :bucket="bucket" :prefix="prefix" />
</template>
