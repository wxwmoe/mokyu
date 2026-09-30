<script setup lang="ts">
import { computed } from 'vue'
import { File, Image, Film, Music2, FileText, Folder, FileArchive, Code2 } from 'lucide-vue-next'
const props = defineProps<{ name: string; folder?: boolean; large?: boolean }>()
const kind = computed(() => {
  if (props.folder) return { icon: Folder, tone: 'cream' }
  const ext = props.name.split('.').pop()?.toLowerCase() || ''
  if (['png', 'jpg', 'jpeg', 'gif', 'webp', 'avif', 'svg', 'heic'].includes(ext)) return { icon: Image, tone: 'rose' }
  if (['mp4', 'mkv', 'webm', 'mov', 'avi'].includes(ext)) return { icon: Film, tone: 'lilac' }
  if (['mp3', 'ogg', 'flac', 'wav', 'm4a', 'opus'].includes(ext)) return { icon: Music2, tone: 'sky' }
  if (['txt', 'md', 'pdf', 'doc', 'docx'].includes(ext)) return { icon: FileText, tone: 'mint' }
  if (['zip', 'gz', 'zst', '7z', 'tar'].includes(ext)) return { icon: FileArchive, tone: 'cream' }
  if (['json', 'html', 'css', 'js', 'xml', 'yaml'].includes(ext)) return { icon: Code2, tone: 'sky' }
  return { icon: File, tone: 'muted' }
})
</script>
<template><span class="file-icon" :class="[kind.tone, { large }]" aria-hidden="true"><component :is="kind.icon" :size="large ? 42 : 20" :stroke-width="1.7" /></span></template>
