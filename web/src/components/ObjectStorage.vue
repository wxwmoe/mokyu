<script setup lang="ts">
import ErrorNotice from './ErrorNotice.vue'
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Package } from 'lucide-vue-next'
import { api, params, bytes } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import ObjectExtent from './ObjectExtent.vue'
import { completePack, extentGroups } from '../app/object-storage'
import UiListFooter from './ui/UiListFooter.vue'
const props = defineProps<{ bucket: string; item: components['schemas']['MediaItem'] }>()
const after = ref('')
const query = computed(() => params({ bucket: props.bucket, key: props.item.object_key, version: props.item.id, after: after.value || undefined }))
const chunks = useQuery({ queryKey: ['object-chunks', query], queryFn: ({ signal }) => api<components['schemas']['ChunkPage']>(`/api/v1/buckets/${props.bucket}/object/chunks?` + query.value, { signal }) })
const groups = computed(() => extentGroups(chunks.data.value?.items ?? []))
</script>

<template>
  <div class="object-storage">
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
      <ObjectExtent v-else :row="group.members[0]" />
    </template>
    <UiListFooter :first="!!after" :next="!!chunks.data.value?.next_cursor" @first="after = ''" @next="after = chunks.data.value!.next_cursor || ''" />
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
