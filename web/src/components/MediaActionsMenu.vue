<script setup lang="ts">
import { computed } from 'vue'
import { DropdownMenuItem, DropdownMenuSeparator } from 'reka-ui'
import { MoreHorizontal, CopyPlus, FolderInput, Globe, LockKeyhole, SlidersHorizontal, Trash2 } from 'lucide-vue-next'
import type { Bucket } from '../api/client'
import type { MediaAction, MediaItem } from '../app/media'
import { t } from '../app/i18n'
import UiMenu from './ui/UiMenu.vue'
const props = defineProps<{ bucket: Bucket; items: MediaItem[] }>()
const emit = defineEmits<{ action: [action: MediaAction] }>()
const choices = computed(() => {
  const can = (action: Bucket['actions'][number]) => props.bucket.actions.includes(action)
  return [
    { action: 'copy', icon: CopyPlus, allowed: can('object.read') },
    { action: 'move', icon: FolderInput, allowed: can('object.read') && can('object.delete') },
    { action: 'metadata', icon: SlidersHorizontal, allowed: props.items.length === 1 && can('object.read') && can('object.write') && (!props.items[0]?.public_read || can('object.acl')) },
    { action: 'private', icon: LockKeyhole, allowed: can('object.acl') },
    { action: 'public-read', icon: Globe, allowed: can('object.acl') },
    { action: 'delete', icon: Trash2, allowed: can('object.delete') },
  ].filter(item => item.allowed)
})
</script>
<template><UiMenu v-if="items.length && choices.length && bucket.status === 'active'" :label="t('mediaActions')"><template #trigger><MoreHorizontal :size="18" /></template><template v-for="choice in choices" :key="choice.action"><DropdownMenuSeparator v-if="choice.action === 'delete'" class="menu-separator" /><DropdownMenuItem class="menu-item" :class="{ danger: choice.action === 'delete' }" @select="emit('action', choice.action as MediaAction)"><component :is="choice.icon" :size="16" />{{ t('mediaAction_' + choice.action) }}</DropdownMenuItem></template></UiMenu></template>
