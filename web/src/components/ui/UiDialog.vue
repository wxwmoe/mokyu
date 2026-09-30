<script setup lang="ts">
import { watch } from 'vue'
import { DialogRoot, DialogPortal, DialogOverlay, DialogContent, DialogTitle, DialogDescription, DialogClose } from 'reka-ui'
import { X } from 'lucide-vue-next'
withDefaults(defineProps<{ title: string; description?: string; drawer?: boolean; wide?: boolean; busy?: boolean }>(), { description: '' })
const open = defineModel<boolean>('open', { required: true })
let returnFocus: HTMLElement | null = null
watch(open, value => { if (value) returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null }, { flush: 'sync', immediate: true })
function restoreFocus(event: Event) { event.preventDefault(); if (returnFocus?.isConnected) returnFocus.focus({ preventScroll: true }) }
</script>
<template>
  <DialogRoot v-model:open="open"><DialogPortal><DialogOverlay class="dialog-overlay" />
    <DialogContent class="dialog-panel" :class="{ drawer, wide }" @close-auto-focus="restoreFocus" @escape-key-down="busy && $event.preventDefault()" @pointer-down-outside="busy && $event.preventDefault()">
      <header class="dialog-header"><div><DialogTitle class="dialog-title">{{ title }}</DialogTitle><DialogDescription :class="{ 'sr-only': !description }">{{ description || title }}</DialogDescription></div><DialogClose class="icon-button" aria-label="Close" :disabled="busy"><X :size="18" /></DialogClose></header>
      <div class="dialog-body"><slot /></div>
      <footer v-if="$slots.footer" class="dialog-footer"><slot name="footer" /></footer>
    </DialogContent>
  </DialogPortal></DialogRoot>
</template>
