<script setup lang="ts">
import { onMounted, onUnmounted, ref, watch } from 'vue'
import { hasThumbnail, mediaUrl, thumbnail, type MediaItem } from '../app/media'
import { session } from '../api/client'
import FileIcon from './FileIcon.vue'
const props = defineProps<{ bucket: string; item: MediaItem; allowed: boolean; large?: boolean }>()
const root = ref<HTMLElement>(), visible = ref(false), source = ref(''), loading = ref(false)
let observer: IntersectionObserver | undefined
onMounted(() => {
  observer = new IntersectionObserver(entries => { if (entries.some(e => e.isIntersecting)) { visible.value = true; observer?.disconnect() } }, { rootMargin: '80px' })
  if (root.value) observer.observe(root.value)
})
onUnmounted(() => observer?.disconnect())
watch(() => [visible.value, props.bucket, props.item.id, props.allowed, session.value?.id], async (_, __, cleanup) => {
  const controller = new AbortController()
  let objectUrl = ''
  source.value = ''; loading.value = false
  cleanup(() => { controller.abort(); if (objectUrl) URL.revokeObjectURL(objectUrl) })
  if (!visible.value || !props.allowed || !session.value || !hasThumbnail(props.item)) return
  loading.value = true
  try {
    const blob = await thumbnail(mediaUrl(props.bucket, props.item, 'thumbnail'), controller.signal)
    if (!controller.signal.aborted) { objectUrl = URL.createObjectURL(blob); source.value = objectUrl }
  } catch { /* Unsupported or busy previews keep the file type card. */ }
  finally { if (!controller.signal.aborted) loading.value = false }
}, { immediate: true })
</script>
<template><span ref="root" class="media-thumbnail" :class="{ large, loading }"><img v-if="source" :src="source" alt="" decoding="async" @error="source = ''"><FileIcon v-else :name="item.object_key" :large="large" /></span></template>
