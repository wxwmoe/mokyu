<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { TabsRoot, TabsList, TabsTrigger, TabsContent } from 'reka-ui'
import { RefreshCw, ArrowUpRight, ChevronRight, ChevronLeft, Download, Copy, LockKeyhole, Globe, Home, Heart, UploadCloud } from 'lucide-vue-next'
import { api, bytes, params, session, type Bucket } from '../api/client'
import { copy, notify, report, errorText } from '../app/feedback'
import { t, date } from '../app/i18n'
import UiSelect from '../components/ui/UiSelect.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiTip from '../components/ui/UiTip.vue'
import FileIcon from '../components/FileIcon.vue'
import EmptyState from '../components/EmptyState.vue'
import QuotaPanel from '../components/QuotaPanel.vue'
import UploadDialog from '../components/UploadDialog.vue'
import MediaFilters from '../components/MediaFilters.vue'
import type { components } from '../api/schema'

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
const uploading = ref(false)
const opened = computed({ get: () => !!selected.value, set: value => { if (!value) selected.value = null } })
const selectedIndex = computed(() => objects.data.value?.objects.findIndex(item => item.id === selected.value?.id) ?? -1)
const download = computed(() => '/api/download?' + params({ bucket: bucket.value, key: selected.value?.object_key }))
const sizes = computed(() => ['25', '50', '100'].map(value => ({ value, label: t('perPage', { count: value }) })))
watch([bucket, query], () => { selected.value = null })
function folder(value: string) { selected.value = null; router.push({ query: { prefix: value || undefined } }) }
function step(delta: number) { selected.value = objects.data.value?.objects[selectedIndex.value + delta] || selected.value }
async function refresh() {
  const result = await (bucket.value ? objects.refetch() : buckets.refetch())
  if (result.error) report(result.error); else notify(t('refreshed'))
}
</script>

<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('collection') }}</p><h1>{{ current?.name || t('media') }}</h1><p>{{ t('everything') }}</p></div><div class="heading-actions"><button :disabled="buckets.isFetching.value || (bucket !== '' && objects.isFetching.value)" @click="refresh"><RefreshCw :size="16" />{{ t('refresh') }}</button><button v-if="current?.state === 'active' && current.actions.includes('object.write')" class="primary" @click="uploading = true"><UploadCloud :size="16" />{{ t('uploadTitle') }}</button></div></div>
  <p v-if="buckets.error.value || objects.error.value" class="error" role="alert">{{ errorText(buckets.error.value || objects.error.value) }}</p>
  <QuotaPanel v-if="current?.actions.includes('storage.inspect')" kind="bucket" :id="current.id" />
  <div v-if="!bucket" class="bucket-grid">
    <RouterLink v-for="item in buckets.data.value" :key="item.id" class="bucket-card" :to="`/media/${item.id}`"><img src="/assets/mokyu-pack.svg" alt="" width="64" height="64"><h2>{{ item.name }}</h2><span>{{ t(item.state) }}</span><ArrowUpRight class="bucket-arrow" :size="19" /></RouterLink>
    <EmptyState v-if="!buckets.isPending.value && !buckets.error.value && !buckets.data.value?.length" :title="t('fresh')" :description="t(session?.role === 'admin' ? 'firstBucket' : 'askForBucket')" />
  </div>
  <section v-else class="surface">
    <div class="media-toolbar"><nav class="breadcrumbs" :aria-label="t('folderPath')"><RouterLink to="/media"><Home :size="15" />{{ t('media') }}</RouterLink><ChevronRight :size="13" /><button @click="folder('')">{{ current?.name }}</button><span v-if="prefix" class="path-text">/ {{ prefix }}</span></nav>
      <UiSelect v-model="pageSize" :label="t('pageSize')" :options="sizes" />
    </div>
    <MediaFilters :ready="ready" />
    <p v-if="!ready && catalog.data.value" class="info-callout">{{ t('catalogBuilding') }}</p>
    <p v-if="objects.data.value?.search_mode === 'prefix' && route.query.q && (!route.query.mode || route.query.mode === 'contains')" class="field-help catalog-note">{{ t('shortSearchHint') }}</p>
    <p v-else-if="objects.data.value?.layout === 'flat'" class="field-help catalog-note">{{ t('flatResults') }}</p>
    <div v-if="objects.isPending.value" role="status"><span class="sr-only">{{ t('opening') }}</span><div v-for="i in 4" :key="i" class="skeleton-row" /></div>
    <div v-else class="table-scroll"><table><thead><tr><th>{{ t('name') }}</th><th>{{ t('size') }}</th><th>{{ t('access') }}</th><th>{{ t('updated') }}</th></tr></thead><tbody>
      <tr v-for="name in objects.data.value?.prefixes" :key="name"><td colspan="4"><button class="text-button file-name" @click="folder(name)"><FileIcon :name="name" folder /><span>{{ name.slice(prefix.length) }}</span></button></td></tr>
      <tr v-for="item in objects.data.value?.objects" :key="item.id" :class="{ selected: item.id === selected?.id }"><td><button class="text-button file-name" @click="selected = item"><FileIcon :name="item.object_key" /><span>{{ item.object_key.slice(prefix.length) }}</span></button></td><td>{{ bytes(Number(item.size)) }}</td><td><span class="badge" :class="{ public: item.public_read }"><Globe v-if="item.public_read" :size="11" /><LockKeyhole v-else :size="11" />{{ t(item.public_read ? 'public' : 'private') }}</span></td><td>{{ date(item.modified_at) }}</td></tr>
    </tbody></table></div>
    <EmptyState v-if="objects.data.value && !objects.data.value.objects.length && !objects.data.value.prefixes.length" compact :title="t('emptyFolder')" :description="t('folderReady')" />
    <div v-if="objects.data.value" class="table-footer"><span>{{ t('pageItems', { count: objects.data.value.objects.length + objects.data.value.prefixes.length }) }}</span><button v-if="cursor" @click="router.push({ query: { ...route.query, cursor: undefined } })">{{ t('firstPage') }}</button><button v-if="objects.data.value.next" @click="router.push({ query: { ...route.query, cursor: objects.data.value.next } })">{{ t('nextPage') }}<ChevronRight :size="16" /></button><Heart v-else :size="14" /></div>
  </section>
  <UiDialog v-model:open="opened" :title="selected?.object_key.split('/').pop() || t('objectDetails')" drawer>
    <template v-if="selected"><div class="preview-heading"><span class="badge" :class="{ public: selected.public_read }">{{ t(selected.public_read ? 'public' : 'private') }}</span><div><button class="icon-button" :aria-label="t('previousObject')" :disabled="selectedIndex <= 0" @click="step(-1)"><ChevronLeft :size="17" /></button><button class="icon-button" :aria-label="t('nextObject')" :disabled="selectedIndex >= (objects.data.value?.objects.length || 0) - 1" @click="step(1)"><ChevronRight :size="17" /></button></div></div>
    <div class="preview-placeholder"><FileIcon :name="selected.object_key" large /></div>
    <div class="actions"><a v-if="current?.actions.includes('object.read')" class="button primary" :href="download"><Download :size="16" />{{ t('download') }}</a><UiTip :text="t('copyPath')"><button class="icon-button" :aria-label="t('copyPath')" @click="copy(selected.object_key)"><Copy :size="18" /></button></UiTip></div>
    <TabsRoot default-value="information"><TabsList class="tabs-list" :aria-label="t('objectDetails')"><TabsTrigger class="tab-trigger" value="information">{{ t('information') }}</TabsTrigger><TabsTrigger class="tab-trigger" value="access">{{ t('access') }}</TabsTrigger></TabsList>
      <TabsContent value="information"><dl class="facts"><dt>{{ t('size') }}</dt><dd>{{ bytes(Number(selected.size)) }}</dd><dt>{{ t('updated') }}</dt><dd>{{ date(selected.modified_at) }}</dd><dt>{{ t('bucket') }}</dt><dd>{{ current?.name }}</dd></dl><details><summary>{{ t('fullPath') }}</summary><p class="object-path">{{ selected.object_key }}</p></details></TabsContent>
      <TabsContent value="access"><dl class="facts"><dt>{{ t('publicAccess') }}</dt><dd>{{ t(selected.public_read ? 'anyone' : 'authorized') }}</dd></dl><p class="field-help">{{ t('sessionDownload') }}</p></TabsContent>
    </TabsRoot></template>
  </UiDialog>
  <UploadDialog v-model:open="uploading" :buckets="buckets.data.value || []" :bucket="bucket" :prefix="prefix" />
</template>
