<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { bytes } from '../api/client'
import { t, date, locale } from '../app/i18n'
const props = defineProps<{ points: { at: string; value: number | null; epoch?: string }[]; label: string; unit?: 'bytes' | 'rate' }>()
const selected = ref(-1), chart = ref<SVGSVGElement>()
const width = ref(0), inset = 6
watch(chart, (svg, _, cleanup) => {
  if (!svg) return
  const resize = () => { width.value = svg.clientWidth }
  resize()
  const observer = new ResizeObserver(resize)
  observer.observe(svg)
  cleanup(() => observer.disconnect())
}, { flush: 'post' })
const values = computed(() => props.points.filter(p => p.value != null))
const maximum = computed(() => Math.max(1, ...values.value.map(p => p.value!)))
const first = computed(() => new Date(props.points[0]?.at || 0).getTime()), last = computed(() => new Date(props.points.at(-1)?.at || 0).getTime())
const plotWidth = computed(() => Math.max(1, width.value - inset * 2))
const x = (at: string) => inset + (new Date(at).getTime() - first.value) / Math.max(1, last.value - first.value) * plotWidth.value
const axis = computed(() => {
  const sameDay = new Date(first.value).toDateString() === new Date(last.value).toDateString()
  return new Intl.DateTimeFormat(locale.value, sameDay
    ? { hour: '2-digit', minute: '2-digit', second: last.value - first.value < 60_000 ? '2-digit' : undefined }
    : { year: 'numeric', month: 'short', day: 'numeric' })
})
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
  const rect = chart.value!.getBoundingClientRect()
  const at = first.value + ((event.clientX - rect.left) / rect.width * width.value - inset) / plotWidth.value * (last.value - first.value)
  selected.value = props.points.reduce((best, p, i) => Math.abs(new Date(p.at).getTime() - at) < Math.abs(new Date(props.points[best]!.at).getTime() - at) ? i : best, 0)
}
function key(event: KeyboardEvent) { if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) { event.preventDefault(); selected.value = event.key === 'Home' ? 0 : event.key === 'End' ? props.points.length - 1 : Math.max(0, Math.min(props.points.length - 1, (selected.value < 0 ? props.points.length - 1 : selected.value) + (event.key === 'ArrowLeft' ? -1 : 1))) } }
watch(() => props.points.length, () => { if (selected.value >= props.points.length) selected.value = -1 })
</script>
<template><figure class="trend-chart"><figcaption><span>{{ label }}</span><span class="trend-reading" :class="{ 'is-empty': !point || point.value == null }"><template v-if="point && point.value != null"><strong>{{ format(point.value) }}</strong><small>{{ date(point.at) }}</small></template></span></figcaption>
  <template v-if="values.length >= 2">
    <svg ref="chart" height="150" role="img" tabindex="0" :aria-label="t('chartKeyboard', { label })" @pointermove="move" @pointerleave="selected = -1" @keydown="key">
      <path v-for="level in [28, 80, 132]" :key="level" class="chart-grid" :d="`M${inset} ${level}h${plotWidth}`" />
      <path class="chart-line" :d="line" />
      <template v-if="point?.value != null"><path class="chart-guide" :d="`M${x(point.at)} 20V140`" /><circle class="chart-point" :cx="x(point.at)" :cy="y(point.value)" r="4" /></template>
    </svg>
    <div class="chart-axis"><time :datetime="points[0]?.at">{{ axis.format(first) }}</time><time :datetime="points.at(-1)?.at">{{ axis.format(last) }}</time></div>
  </template>
  <div v-else class="chart-empty"><span class="chart-seed" /><p>{{ t('trendCollecting') }}</p><small>{{ t('trendHonest') }}</small></div>
</figure></template>
