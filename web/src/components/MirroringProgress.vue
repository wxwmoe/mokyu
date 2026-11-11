<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { bytes } from '../api/client'
import { t } from '../app/i18n'
import { mirroringPhase, type Job } from '../app/maintenance'
const props = defineProps<{ job: Job }>()
const detail = computed(() => props.job.detail as Record<string, any>)
const done = computed(() => Number(detail.value.source_bytes || 0))
const total = computed(() => detail.value.source_size == null ? null : Number(detail.value.source_size))
const phase = computed(() => mirroringPhase(props.job))
const tone = computed(() => ({ saved: 'mint', failed: 'rose', paused: 'cream', queued: 'gray', reading: 'sky', publishing: 'sky' })[phase.value as string] || 'gray')
const speed = ref<number | null>(null)
let sample: { id: string; bytes: number; at: number } | undefined, stale: ReturnType<typeof setTimeout> | undefined
watch(() => [props.job.id, props.job.updated_at, done.value, phase.value], () => {
  const now = performance.now(), previous = sample
  if (phase.value !== 'reading' || previous?.id !== props.job.id || done.value < previous.bytes) {
    speed.value = null
    sample = { id: props.job.id, bytes: done.value, at: now }
    clearTimeout(stale)
  }
  else if (done.value > previous.bytes && now > previous.at) {
    const rate = (done.value - previous.bytes) * 1000 / (now - previous.at)
    speed.value = speed.value == null ? rate : speed.value * .7 + rate * .3
    sample = { id: props.job.id, bytes: done.value, at: now }
    clearTimeout(stale)
    stale = setTimeout(() => { speed.value = null }, 15000)
  }
}, { immediate: true })
onBeforeUnmount(() => clearTimeout(stale))
const remaining = computed(() => speed.value && total.value != null ? Math.max(0, Math.ceil((total.value - done.value) / speed.value)) : null)
</script>
<template>
  <div class="mirror-progress">
    <div class="mirror-progress-line"><span class="badge node-state" :data-tone="tone">{{ t('mirrorPhase_' + phase) }}</span><span>{{ bytes(done) }}<template v-if="total != null"> / {{ bytes(total) }}</template></span></div>
    <div v-if="total != null && total > 0" class="transfer-progress" role="progressbar" :aria-valuenow="Math.min(100, Math.round(done / total * 100))" aria-valuemin="0" aria-valuemax="100" :aria-label="t('mirrorReadBytes')"><span :style="{ width: Math.min(100, done / total * 100) + '%' }" /></div>
    <div v-if="phase === 'reading'" class="mirror-progress-line mirror-rate"><span>{{ speed == null ? t('mirrorMeasuring') : bytes(speed) + '/s' }}</span><span v-if="remaining != null">{{ t('mirrorEta', { seconds: remaining }) }}</span></div>
    <p v-if="phase === 'publishing'" class="field-help">{{ t('mirrorPublishingHint') }}</p>
    <p v-if="props.job.error" class="error" role="alert">{{ props.job.error === 'origin_fill_failed' ? t('mirrorFillFailed') : props.job.error }}</p>
    <small v-if="detail.range_requests" class="field-help">{{ t('mirrorRangeTraffic', { count: detail.range_requests, size: bytes(Number(detail.range_bytes || 0)) }) }}</small>
  </div>
</template>
<style scoped>
.mirror-progress { display: grid; gap: 8px; min-width: 0; }
.mirror-progress-line { display: flex; align-items: center; justify-content: space-between; gap: 8px 16px; flex-wrap: wrap; font-variant-numeric: tabular-nums; }
.mirror-rate { color: var(--muted); font-size: var(--small-size, 12px); min-height: 20px; }
.transfer-progress { margin: 0; }
.mirror-progress p { margin: 0; }
</style>
