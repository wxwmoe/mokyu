<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { DialogRoot, DialogPortal, DialogOverlay, DialogContent, DialogTitle, DialogDescription, DialogClose, TabsRoot, TabsList, TabsTrigger, TabsContent } from 'reka-ui'
import { X, ChevronLeft, ChevronRight, Maximize2, Minimize2, Download, Copy, Globe, LockKeyhole, Info, ShieldCheck, Play, ZoomIn, ZoomOut, Image, PanelRightClose, PanelRightOpen, Link } from 'lucide-vue-next'
import { api, bytes, params, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { mediaUrl, type MediaItem, type MediaAction } from '../app/media'
import { copy, errorText } from '../app/feedback'
import { t, date } from '../app/i18n'
import MediaThumbnail from './MediaThumbnail.vue'
import FileIcon from './FileIcon.vue'
import MediaActionsMenu from './MediaActionsMenu.vue'

const props = defineProps<{ bucket: Bucket; item: MediaItem | null; previous: boolean; next: boolean }>()
const emit = defineEmits<{ close: []; step: [delta: number]; action: [kind: MediaAction] }>()
const opened = computed({ get: () => !!props.item, set: value => { if (!value) emit('close') } })
const wide = ref(false), original = ref(false), zoom = ref(false), inspector = ref(true), failed = ref(false)
const canRead = computed(() => props.bucket.actions.includes('object.read'))
const query = computed(() => params({ key: props.item?.object_key, version: props.item?.id }))
const detail = useQuery({ queryKey: ['object', () => props.bucket.id, query], enabled: computed(() => !!props.item), queryFn: ({ signal }) => api<components['schemas']['ObjectDetail']>(`/api/buckets/${props.bucket.id}/object?${query.value}`, { signal }), gcTime: 0 })
const text = useQuery({ queryKey: ['object-text', () => props.bucket.id, query], enabled: computed(() => !!props.item && canRead.value && detail.data.value?.preview === 'text'), queryFn: ({ signal }) => api<components['schemas']['TextPreview']>(mediaUrl(props.bucket.id, props.item!, 'text'), { signal }), gcTime: 0 })
const preview = computed(() => detail.data.value?.preview || 'none')
const name = computed(() => props.item?.object_key.split('/').pop() || props.item?.object_key || t('objectDetails'))
let returnFocus: HTMLElement | null = null
watch(() => props.item, (value, previous) => {
  if (value && !previous) returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null
  original.value = false; zoom.value = false; failed.value = false
  if (!value) { wide.value = false; inspector.value = true }
}, { immediate: true, flush: 'sync' })
function focusBack(event: Event) { event.preventDefault(); if (returnFocus?.isConnected) returnFocus.focus({ preventScroll: true }) }
function expand() { wide.value = !wide.value; if (wide.value && preview.value === 'image' && canRead.value) original.value = true }
function permalink() { const url = new URL(location.href); url.searchParams.set('object', props.item!.object_key); url.searchParams.set('version', props.item!.id); copy(url.toString()) }
</script>
<template>
  <DialogRoot v-model:open="opened"><DialogPortal><DialogOverlay class="dialog-overlay object-overlay" />
    <DialogContent class="object-viewer" :class="{ expanded: wide, 'inspector-hidden': !inspector }" @close-auto-focus="focusBack">
      <header class="object-viewer-bar"><span class="viewer-title"><Image :size="17" /><span>{{ t('quickLook') }}</span><DialogTitle class="sr-only">{{ name }}</DialogTitle></span><DialogDescription class="sr-only">{{ t('objectDetails') }}</DialogDescription><div class="viewer-controls"><button class="icon-button" :aria-label="t('previousObject')" :disabled="!previous" @click="emit('step', -1)"><ChevronLeft :size="17" /></button><button class="icon-button" :aria-label="t('nextObject')" :disabled="!next" @click="emit('step', 1)"><ChevronRight :size="17" /></button><span class="viewer-separator" /><button class="icon-button" :aria-label="t(wide ? 'compactView' : 'expandView')" @click="expand"><Minimize2 v-if="wide" :size="16" /><Maximize2 v-else :size="16" /></button><button v-if="wide" class="icon-button" :aria-label="t(inspector ? 'hideInspector' : 'showInspector')" @click="inspector = !inspector"><PanelRightClose v-if="inspector" :size="16" /><PanelRightOpen v-else :size="16" /></button><DialogClose class="icon-button" :aria-label="t('close')"><X :size="18" /></DialogClose></div></header>
      <div v-if="item" class="object-viewer-body">
        <section class="object-stage" :class="{ 'stage-text': preview === 'text', 'stage-original': original, 'stage-zoom': zoom }" :aria-label="t('preview')">
          <p v-if="detail.error.value" class="error" role="alert">{{ errorText(detail.error.value) }}</p>
          <template v-else-if="canRead">
            <div v-if="preview === 'text'" class="text-preview"><pre v-if="text.data.value">{{ text.data.value.text }}</pre><p v-else class="field-help">{{ text.error.value ? errorText(text.error.value) : t('opening') }}</p><span v-if="text.data.value?.truncated" class="text-truncated">{{ t('textTruncated') }}</span></div>
            <template v-else-if="original && !failed && preview !== 'none'">
              <img v-if="preview === 'image'" class="original-image" :src="mediaUrl(bucket.id, item, 'content', true)" :alt="name" @error="failed = true">
              <video v-else-if="preview === 'video'" :key="item.id" :src="mediaUrl(bucket.id, item, 'content', true)" controls preload="metadata" @error="failed = true" />
              <audio v-else-if="preview === 'audio'" :key="item.id" :src="mediaUrl(bucket.id, item, 'content', true)" controls preload="metadata" @error="failed = true" />
              <button v-if="wide && preview === 'image'" class="zoom-button" :aria-label="t(zoom ? 'fitImage' : 'actualSize')" @click="zoom = !zoom"><ZoomOut v-if="zoom" :size="17" /><ZoomIn v-else :size="17" />{{ t(zoom ? 'fitImage' : 'actualSize') }}</button>
            </template>
            <template v-else><MediaThumbnail :bucket="bucket.id" :item="item" :allowed="canRead" large /><button v-if="!failed && preview !== 'none' && preview !== 'text'" class="preview-open" @click="original = true"><Play v-if="preview === 'video' || preview === 'audio'" :size="16" /><Maximize2 v-else :size="16" />{{ t(preview === 'image' ? 'loadOriginal' : 'loadMedia') }}<span>{{ bytes(Number(item.size)) }}</span></button><p v-else class="preview-message">{{ t(failed ? 'mediaUnsupported' : 'previewUnavailable') }}</p></template>
          </template>
          <template v-else><FileIcon :name="item.object_key" large /><p class="preview-message">{{ t('previewNeedsRead') }}</p></template>
        </section>
        <aside class="object-inspector"><div class="object-identity"><h2 :title="name">{{ name }}</h2><div class="object-tags"><span>{{ bytes(Number(item.size)) }}</span><span class="badge" :class="{ public: item.public_read }"><Globe v-if="item.public_read" :size="11" /><LockKeyhole v-else :size="11" />{{ t(item.public_read ? 'public' : 'private') }}</span></div></div>
          <div class="object-primary-actions"><a v-if="canRead" class="button primary" :href="mediaUrl(bucket.id, item)" :download="name"><Download :size="16" />{{ t('download') }}</a><button class="icon-button" :aria-label="t('copyPath')" @click="copy(item.object_key)"><Copy :size="17" /></button><button class="icon-button" :aria-label="t('copyViewLink')" @click="permalink"><Link :size="17" /></button><MediaActionsMenu :bucket="bucket" :items="[item]" @action="emit('action', $event)" /></div>
          <TabsRoot default-value="information"><TabsList class="tabs-list" :aria-label="t('objectDetails')"><TabsTrigger class="tab-trigger" value="information"><Info :size="14" />{{ t('information') }}</TabsTrigger><TabsTrigger class="tab-trigger" value="access"><ShieldCheck :size="14" />{{ t('access') }}</TabsTrigger></TabsList>
            <TabsContent value="information"><dl class="facts object-facts"><dt>{{ t('fileType') }}</dt><dd class="break-anywhere">{{ item.content_type }}</dd><dt>{{ t('updated') }}</dt><dd>{{ date(item.modified_at) }}</dd><dt>{{ t('bucket') }}</dt><dd>{{ bucket.name }}</dd></dl><details class="object-fold"><summary>{{ t('fullPath') }}</summary><p class="object-path">{{ item.object_key }}</p></details><details v-if="detail.data.value" class="object-fold"><summary>{{ t('technicalDetails') }}</summary><dl class="facts object-facts"><dt>{{ t('version') }}</dt><dd class="object-path">{{ item.id }}</dd><dt>ETag</dt><dd class="object-path">{{ detail.data.value.etag }}</dd></dl><dl class="metadata-fields"><template v-for="(value, key) in detail.data.value.metadata" :key="key"><template v-if="value && key !== 'user'"><dt>{{ key }}</dt><dd>{{ value }}</dd></template></template><template v-for="(value, key) in detail.data.value.metadata.user" :key="key"><dt>{{ key }}</dt><dd>{{ value }}</dd></template></dl></details></TabsContent>
            <TabsContent value="access"><div class="access-note"><Globe v-if="item.public_read" :size="24" /><LockKeyhole v-else :size="24" /><div><strong>{{ t(item.public_read ? 'anyone' : 'authorized') }}</strong><p>{{ t('sessionDownload') }}</p></div></div><p class="field-help">{{ t('yourCapabilities') }}</p><div class="capability-list"><span v-for="action in bucket.actions" :key="action" class="badge">{{ t('action_' + action) }}</span></div></TabsContent>
          </TabsRoot>
        </aside>
      </div>
    </DialogContent>
  </DialogPortal></DialogRoot>
</template>
