<script setup lang="ts">
import { computed } from 'vue'
import { DropdownMenuItem, DropdownMenuSeparator } from 'reka-ui'
import { MoreHorizontal, CopyPlus, FolderInput, Globe, LockKeyhole, SlidersHorizontal, Trash2 } from 'lucide-vue-next'
import type { Bucket } from '../api/client'
import type { MediaAction, MediaItem } from '../app/media'
import { t } from '../app/i18n'
import UiMenu from './ui/UiMenu.vue'
const props = defineProps<{ bucket: Bucket; items: MediaItem[] }>()
const emit = defineEmits<{ action: [kind: MediaAction] }>()
const choices = computed(() => {
  const can = (action: Bucket['actions'][number]) => props.bucket.actions.includes(action)
  return [
    { kind: 'copy', icon: CopyPlus, allowed: can('object.read') },
    { kind: 'move', icon: FolderInput, allowed: can('object.read') && can('object.delete') },
    { kind: 'metadata', icon: SlidersHorizontal, allowed: props.items.length === 1 && can('object.read') && can('object.write') && (!props.items[0]?.public_read || can('object.acl')) },
    { kind: 'private', icon: LockKeyhole, allowed: can('object.acl') },
    { kind: 'public-read', icon: Globe, allowed: can('object.acl') },
    { kind: 'delete', icon: Trash2, allowed: can('object.delete') },
  ].filter(item => item.allowed)
})
</script>
<template><UiMenu v-if="items.length && choices.length && bucket.state === 'active'" :label="t('mediaActions')"><template #trigger><MoreHorizontal :size="18" /></template><template v-for="choice in choices" :key="choice.kind"><DropdownMenuSeparator v-if="choice.kind === 'delete'" class="menu-separator" /><DropdownMenuItem class="menu-item" :class="{ danger: choice.kind === 'delete' }" @select="emit('action', choice.kind as MediaAction)"><component :is="choice.icon" :size="16" />{{ t('mediaAction_' + choice.kind) }}</DropdownMenuItem></template></UiMenu></template>
