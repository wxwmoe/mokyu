<script setup lang="ts">
import { computed } from 'vue'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import UiTip from './ui/UiTip.vue'

const props = defineProps<{ value?: components['schemas']['NonceCapacity'] | null }>()
const tone = computed(() => {
  const value = props.value
  if (value?.remaining == null) return 'gray'
  const remaining = Number(value.remaining)
  if (value.state === 'exhausted' || remaining <= 20 || value.counters.some(counter => counter.reason === 'forecast')) return 'rose'
  return remaining <= 40 || value.state === 'warning' ? 'cream' : 'mint'
})
const description = computed(() => {
  const value = props.value
  const lines = [t('nonceCapacityHint')]
  for (const counter of value?.counters || []) {
    lines.push(t(counter.type_tag === 0 ? 'nonceChunks' : 'noncePacks') + ': ' + (counter.remaining ? t('nonceRemaining', { value: counter.remaining }) : t('nonceUnknown')))
    if (counter.reason) lines.push(t('nonceReason_' + counter.reason))
    if (counter.estimated_days != null) lines.push(t('nonceEstimate', { days: counter.estimated_days }))
    else lines.push(t('nonceEstimateUnknown'))
    if (counter.estimated_days != null && counter.observed_seconds < 86400) lines.push(t('nonceShortSample'))
  }
  if (value?.sampled_at) lines.push(t('nonceSampledAt', { time: date(value.sampled_at) }))
  if (value?.stale) lines.push(t('nonceStale'))
  return lines.join('\n')
})
</script>

<template><UiTip :text="description"><span class="badge node-state" :data-tone="tone" tabindex="0">{{ value?.remaining ? t('nonceRemaining', { value: value.remaining }) : t('nonceUnknown') }}</span></UiTip></template>
