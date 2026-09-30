<script setup lang="ts">
import { ToastProvider, ToastRoot, ToastTitle, ToastDescription, ToastClose, ToastViewport } from 'reka-ui'
import { Check, CircleAlert, Heart, X, Copy } from 'lucide-vue-next'
import { notices, dismiss, copy } from '../app/feedback'
</script>
<template><ToastProvider swipe-direction="right"><ToastRoot v-for="notice in notices" :key="notice.id" :duration="notice.tone === 'error' ? 20000 : 6000" class="toast" :class="notice.tone" @update:open="!$event && dismiss(notice.id)">
  <span class="toast-symbol"><CircleAlert v-if="notice.tone === 'error'" :size="20" /><Check v-else-if="notice.tone === 'success'" :size="20" /><Heart v-else :size="20" /></span>
  <div><ToastTitle class="toast-title">{{ notice.title }}</ToastTitle><ToastDescription v-if="notice.description" class="toast-description">{{ notice.description }}</ToastDescription><button v-if="notice.requestId" class="request-id text-button" @click="copy(notice.requestId)"><Copy :size="12" />{{ notice.requestId }}</button></div>
  <ToastClose class="icon-button" aria-label="Dismiss notification"><X :size="16" /></ToastClose>
</ToastRoot><ToastViewport class="toast-viewport" label="Notifications (F8)" /></ToastProvider></template>
