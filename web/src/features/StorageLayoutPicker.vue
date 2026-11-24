<script setup lang="ts">
import { computed, ref, useId, watch } from 'vue'
import { ArrowDown, ArrowUp, ChevronsUpDown, Check, Layers } from 'lucide-vue-next'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiDialog from '../components/ui/UiDialog.vue'
import UiListFooter from '../components/ui/UiListFooter.vue'

type Preset = components['schemas']['StorageLayoutPreset']
type Column = 'data' | 'parity' | 'copies' | 'members' | 'factor' | 'tolerance'
const props = defineProps<{ rows: Preset[]; layout: components['schemas']['StorageClusterLayout'] }>()
const open = defineModel<boolean>('open', { required: true })
const emit = defineEmits<{ select: [row: Preset] }>()
const columns: Column[] = ['data','parity','copies','members','factor','tolerance']
const choice = ref(''), group = useId(), viewport = ref<HTMLDivElement>()
const moreColumns = ref(false)
function measureColumns() {
  const el = viewport.value
  moreColumns.value = !!el && el.scrollWidth - el.clientWidth - el.scrollLeft > 1
}
watch(viewport, (el, _, cleanup) => {
  if (!el) return
  const observer = new ResizeObserver(measureColumns)
  observer.observe(el)
  if (el.firstElementChild) observer.observe(el.firstElementChild)
  cleanup(() => observer.disconnect())
})
const keyOf = (row: Preset) => [row.data, row.parity, row.copies].join('-')
const selected = computed(() => props.rows.find(row => keyOf(row) === choice.value))
const order = ref<Column>(), direction = ref(1), page = ref(1)
const pageSize = 25
const sorted = computed(() => {
  const key = order.value
  return [...props.rows].sort(key ? (a,b) => direction.value * (a[key] - b[key]) : (a,b) => Number(b.featured) - Number(a.featured))
})
const pages = computed(() => Math.max(1, Math.ceil(sorted.value.length / pageSize)))
const visible = computed(() => sorted.value.slice((page.value - 1) * pageSize, page.value * pageSize))
watch(open, value => {
  if (value) {
    order.value = undefined; page.value = 1
    choice.value = [props.layout.data_shards, props.layout.parity_shards, props.layout.extra_copies].join('-')
  }
})
watch([page, order, direction], () => {
  if (viewport.value) viewport.value.scrollTop = 0
}, { flush: 'post' })
function sort(key: Column) {
  if (key !== order.value) { order.value = key; direction.value = 1 }
  else if (direction.value === 1) direction.value = -1
  else order.value = undefined
  page.value = 1
}
function apply() { if (selected.value) { emit('select', selected.value); open.value = false } }
</script>
<template>
  <UiDialog v-model:open="open" :title="t('scPickLayout')" :description="t('scLayoutGuide')" wide>
    <div ref="viewport" class="layout-scroll" :class="{ 'more-columns': moreColumns }" @scroll.passive="measureColumns">
      <table class="layout-presets" :aria-label="t('scPickLayout')">
        <thead><tr>
          <th scope="col" class="choice-column"><span class="sr-only">{{ t('scPickLayout') }}</span></th>
          <th v-for="col in columns" :key="col" scope="col" :aria-sort="order === col ? (direction === 1 ? 'ascending' : 'descending') : 'none'">
            <button type="button" @click="sort(col)">{{ t('scColumn_' + col) }}<component :is="order === col ? (direction === 1 ? ArrowUp : ArrowDown) : ChevronsUpDown" :size="13" aria-hidden="true" /></button>
          </th>
        </tr></thead>
        <tbody>
          <tr v-for="row in visible" :key="keyOf(row)" :class="{ chosen: choice === keyOf(row) }">
            <td class="choice-column"><label :for="group + keyOf(row)">
              <input :id="group + keyOf(row)" v-model="choice" type="radio" :name="group" :value="keyOf(row)" :aria-label="t('scLayoutValue', { data: row.data, parity: row.parity, copies: row.copies })" />
            </label></td>
            <td v-for="col in columns" :key="col"><label :for="group + keyOf(row)">{{ col === 'factor' ? Number(row.factor.toFixed(3)) + '×' : row[col] }}</label></td>
          </tr>
        </tbody>
      </table>
    </div>
    <UiListFooter :previous="page > 1" :next="page < pages" :first="page > 2" :summary="t('scLayoutPage', { page, pages, count: rows.length, first: rows.length ? (page-1)*pageSize+1 : 0, last: Math.min(page*pageSize, rows.length) })" @previous="page--" @next="page++" @first="page=1" />
    <template #footer>
      <span v-if="selected" class="layout-selection" aria-live="polite"><Layers :size="16" aria-hidden="true" /><span class="layout-shards">{{ t('scLayoutValue', { data: selected.data, parity: selected.parity, copies: selected.copies }) }}</span><span class="layout-protection">{{ t('scLayoutSummary', { disks: selected.members, factor: Number(selected.factor.toFixed(3)), tolerance: selected.tolerance }) }}</span></span>
      <button @click="open=false">{{ t('cancel') }}</button><button class="primary" :disabled="!selected" @click="apply"><Check :size="16" />{{ t('scApplyLayout') }}</button>
    </template>
  </UiDialog>
</template>

<style scoped>
:global(.dialog-body:has(> .layout-scroll)) { display: flex; flex-direction: column; }
.layout-scroll + :deep(.list-footer) { flex-shrink: 0; }
.layout-scroll { min-height: 8rem; max-height: min(54dvh, 34rem); overflow: auto; overscroll-behavior: contain; border: 1px solid var(--line); border-radius: 14px; scrollbar-width: thin; scrollbar-color: var(--line) transparent; }
.layout-presets { width: 100%; border-spacing: 0; font-variant-numeric: tabular-nums; }
.layout-presets th, .layout-presets td { padding: 0; text-align: end; }
.layout-presets th { position: sticky; top: 0; z-index: 1; background: var(--surface-soft); box-shadow: inset 0 -1px var(--line); }
.layout-presets th button { width: 100%; justify-content: flex-end; padding: 12px; font-size: var(--caption-size, 12px); color: var(--muted); border: 0; border-radius: 0; background: transparent; box-shadow: none; white-space: nowrap; }
.layout-presets th button svg { flex: none; }
.layout-presets th button:hover, .layout-presets th[aria-sort='ascending'] button, .layout-presets th[aria-sort='descending'] button { color: var(--rose); }
.layout-presets td { border-bottom: 1px solid var(--line-soft); background: var(--surface); }
.layout-presets tr:last-child td { border-bottom: 0; }
.layout-presets td label { display: flex; align-items: center; justify-content: flex-end; min-height: 44px; padding: 6px 14px; margin: 0; cursor: pointer; }
.layout-presets .choice-column { position: sticky; left: 0; width: 3.25rem; min-width: 3.25rem; }
.layout-presets th.choice-column { z-index: 2; }
.layout-presets .choice-column label { justify-content: center; }
.layout-presets tbody tr:hover td { background: var(--surface-soft); }
.layout-presets .chosen td { background: var(--rose-soft); color: var(--rose); font-weight: var(--ui-weight, 700); }
.layout-presets tr.chosen:hover td { background: var(--rose-soft); }
.layout-presets .chosen .choice-column { box-shadow: inset 3px 0 var(--rose); }
.layout-presets input { appearance: none; width: 17px; height: 17px; margin: 0; border: 1.5px solid var(--muted); border-radius: 50%; background: var(--surface); cursor: pointer; }
.layout-presets input:checked { border: 5px solid var(--rose); }
.layout-presets input:focus-visible { outline: 2px solid var(--rose); outline-offset: 3px; }
.layout-presets th button:focus-visible { outline-offset: -4px; }
.layout-selection { display: inline-flex; align-items: center; gap: 8px; margin-inline-end: auto; font-size: var(--small-size, 12px); color: var(--muted); }
.layout-selection svg { flex-shrink: 0; color: var(--lilac); }
.layout-protection { display: none; }
@media (max-width: 560px) {
  .layout-shards { display: none; }
  .layout-protection { display: inline; }
  .layout-scroll.more-columns { mask-image: linear-gradient(to right, black calc(100% - 16px), transparent); }
  .layout-presets th button { white-space: normal; word-break: keep-all; min-width: 5.2rem; line-height: 1.4; gap: 4px; padding: 10px 8px; }
  .layout-presets .choice-column { width: 2.75rem; min-width: 2.75rem; }
  .layout-selection { width: 100%; }
}
@media (forced-colors: active) {
  .layout-presets input { appearance: auto; }
}
</style>
