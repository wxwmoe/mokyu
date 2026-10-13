<script setup lang="ts">
import { onBeforeUnmount } from 'vue'
import { ToastProvider, ToastRoot, ToastTitle, ToastDescription, ToastClose, ToastViewport } from 'reka-ui'
import { Check, CircleAlert, Heart, X, Copy } from 'lucide-vue-next'
import { notices, dismiss, copy } from '../app/feedback'
import { t } from '../app/i18n'

const closing = new Map<number, number>()
function finish(id: number) {
  const timer = closing.get(id)
  if (timer !== undefined) window.clearTimeout(timer)
  closing.delete(id)
  dismiss(id)
}
function schedule(id: number) {
  if (closing.has(id)) return
  closing.set(id, window.setTimeout(() => finish(id), 240))
}
function animationEnd(id: number, event: AnimationEvent) {
  if (event.target === event.currentTarget && event.animationName === 'popup-out') finish(id)
}
function animationCancel(id: number) { schedule(id) }
onBeforeUnmount(() => closing.forEach(timer => window.clearTimeout(timer)))
</script>
<template><ToastProvider swipe-direction="right"><ToastRoot v-for="notice in notices" :key="notice.id" force-mount :duration="notice.duration" class="toast" :class="notice.tone" @update:open="!$event && schedule(notice.id)" @animationend="animationEnd(notice.id, $event)" @animationcancel="animationCancel(notice.id)">
  <span class="toast-symbol"><CircleAlert v-if="notice.tone === 'error'" :size="20" /><Check v-else-if="notice.tone === 'success'" :size="20" /><Heart v-else :size="20" /></span>
  <div><ToastTitle class="toast-title">{{ notice.title }}</ToastTitle><ToastDescription v-if="notice.description" class="toast-description">{{ notice.description }}</ToastDescription><button v-if="notice.requestId" class="request-id text-button" @click="copy(notice.requestId)"><Copy :size="12" />{{ notice.requestId }}</button></div>
  <ToastClose class="icon-button" :aria-label="t('dismiss')"><X :size="16" /></ToastClose>
</ToastRoot><ToastViewport class="toast-viewport" :label="t('notifications')" /></ToastProvider></template>
