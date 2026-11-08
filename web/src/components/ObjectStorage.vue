<script setup lang="ts">
import ErrorNotice from './ErrorNotice.vue'
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Package } from 'lucide-vue-next'
import { api, params, bytes } from '../api/client'
import type { components } from '../api/schema'
import { locale, t } from '../app/i18n'
import ObjectExtent from './ObjectExtent.vue'
import { completePack, extentGroups } from '../app/object-storage'
import UiListFooter from './ui/UiListFooter.vue'
const props = defineProps<{ bucket: string; item: components['schemas']['MediaItem'] }>()
const pageSize = 100
const cursors = ref([''])
const list = ref<HTMLElement>()
const total = ref<number>()
const query = computed(() => params({ bucket: props.bucket, key: props.item.object_key, version: props.item.id, limit: String(pageSize), after: cursors.value.at(-1) || undefined }))
const chunks = useQuery({ queryKey: ['object-chunks', query], queryFn: ({ signal }) => api<components['schemas']['ChunkPage']>(`/api/v1/buckets/${props.bucket}/object/chunks?` + query.value, { signal }) })
const groups = computed(() => extentGroups(chunks.data.value?.items ?? []))
watch(() => chunks.data.value?.total_items, value => { if (value != null) total.value = Number(value) }, { immediate: true })
const summary = computed(() => total.value == null ? undefined : t('extentPagination', {
  page: cursors.value.length.toLocaleString(locale.value),
  pages: Math.max(1, Math.ceil(total.value / pageSize)).toLocaleString(locale.value),
  count: total.value.toLocaleString(locale.value)
}))
function navigate(to: 'first' | 'previous' | 'next') {
  if (to === 'next') {
    const cursor = chunks.data.value?.next_cursor
    if (!cursor) return
    cursors.value.push(cursor)
  } else {
    cursors.value.splice(to === 'first' ? 1 : cursors.value.length - 1)
  }
  list.value?.closest('[role="tabpanel"]')?.scrollTo({ top: 0 })
  list.value?.focus({ preventScroll: true })
}
</script>

<template>
  <div ref="list" class="object-storage" tabindex="-1">
    <ErrorNotice v-if="chunks.error.value" :error="chunks.error.value" />
    <div v-if="chunks.isPending.value" class="skeleton-row" />
    <template v-for="group in groups" :key="group.start">
      <details v-if="group.pack" class="object-pack">
        <summary>
          <Package :size="17" />
          <span>{{ t('source_pack') }}<small>{{ t(completePack(group.members) ? 'packContains' : group.members.length === 1 ? 'packRefersOne' : 'packRefers', { count: group.members.length }) }} · {{ bytes(group.end - group.start) }}</small></span>
        </summary>
        <ObjectExtent v-for="row in group.members" :key="row.offset_bytes" :row="row" />
      </details>
      <ObjectExtent v-else :row="group.members[0]" :raw="chunks.data.value?.payload_type === 'raw'" />
    </template>
    <UiListFooter :summary="summary" :first="cursors.length > 2" :previous="cursors.length > 1" :next="!!chunks.data.value?.next_cursor"
      @first="navigate('first')" @previous="navigate('previous')" @next="navigate('next')" />
  </div>
</template>

<style scoped>
.object-pack { border-bottom: 1px solid var(--line-soft); }
.object-pack > summary { display: flex; align-items: baseline; gap: 8px; padding: 13px 0; cursor: pointer; }
.object-pack > summary > svg { align-self: flex-start; color: var(--accent); }
.object-pack > summary::after { content: '▸'; margin-inline-start: auto; }
.object-pack[open] > summary::after { content: '▾'; }
.object-pack small { display: block; color: var(--muted); }
.object-pack > .object-chunk { margin-inline-start: 24px; }
.object-pack > .object-chunk:last-child { border: 0; }
</style>
