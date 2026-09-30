<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { bytes } from '../api/client'
import { t, date, locale } from '../app/i18n'
const props = defineProps<{ points: { at: string; value: number | null; epoch?: string }[]; label: string; unit?: 'bytes' | 'rate' }>()
const selected = ref(-1), chart = ref<SVGElement>()
const values = computed(() => props.points.filter(p => p.value != null))
const maximum = computed(() => Math.max(1, ...values.value.map(p => p.value!)))
const first = computed(() => new Date(props.points[0]?.at || 0).getTime()), last = computed(() => new Date(props.points.at(-1)?.at || 0).getTime())
const x = (at: string) => 16 + (new Date(at).getTime() - first.value) / Math.max(1, last.value - first.value) * 568
const y = (value: number) => 132 - value / maximum.value * 104
const line = computed(() => {
  let value = '', previous: typeof props.points[number] | undefined
  for (const point of props.points) {
    if (point.value == null) { previous = undefined; continue }
    value += `${previous && previous.epoch === point.epoch ? ' L' : ' M'}${x(point.at)},${y(point.value)}`
    previous = point
  }
  return value
})
const point = computed(() => props.points[selected.value] || values.value.at(-1))
function format(value: number) { return props.unit === 'bytes' ? bytes(value) : new Intl.NumberFormat(locale.value, { maximumFractionDigits: 2 }).format(value) + (props.unit === 'rate' ? '/s' : '') }
function move(event: PointerEvent) {
  const rect = chart.value!.getBoundingClientRect(), at = first.value + ((event.clientX - rect.left) / rect.width * 600 - 16) / 568 * (last.value - first.value)
  selected.value = props.points.reduce((best, p, i) => Math.abs(new Date(p.at).getTime() - at) < Math.abs(new Date(props.points[best]!.at).getTime() - at) ? i : best, 0)
}
function key(event: KeyboardEvent) { if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) { event.preventDefault(); selected.value = event.key === 'Home' ? 0 : event.key === 'End' ? props.points.length - 1 : Math.max(0, Math.min(props.points.length - 1, (selected.value < 0 ? props.points.length - 1 : selected.value) + (event.key === 'ArrowLeft' ? -1 : 1))) } }
watch(() => props.points.length, () => { if (selected.value >= props.points.length) selected.value = -1 })
</script>
<template><figure class="trend-chart"><figcaption><span>{{ label }}</span><span v-if="point && point.value != null"><strong>{{ format(point.value) }}</strong><small>{{ date(point.at) }}</small></span></figcaption>
  <template v-if="values.length >= 2"><svg ref="chart" viewBox="0 0 600 150" role="img" tabindex="0" :aria-label="t('chartKeyboard', { label })" @pointermove="move" @pointerleave="selected = -1" @keydown="key"><path class="chart-grid" d="M16 28H584 M16 80H584 M16 132H584" /><path class="chart-line" :d="line" /><template v-if="point?.value != null"><path class="chart-guide" :d="`M${x(point.at)} 20V140`" /><circle class="chart-point" :cx="x(point.at)" :cy="y(point.value)" r="4" /></template></svg><div class="chart-axis"><span>{{ new Date(first).toLocaleDateString(locale) }}</span><span>{{ new Date(last).toLocaleDateString(locale) }}</span></div></template>
  <div v-else class="chart-empty"><span class="chart-seed" /><p>{{ t('trendCollecting') }}</p><small>{{ t('trendHonest') }}</small></div>
</figure></template>
