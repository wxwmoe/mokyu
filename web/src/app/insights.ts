import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import { api, params } from '../api/client'
import type { components } from '../api/schema'
export type Usage = components['schemas']['StorageUsage']
export type Runtime = components['schemas']['RuntimeInsights']
export function useInsights() {
  const route = useRoute()
  const scope = computed(() => ({ bucket: route.query.bucket ? String(route.query.bucket) : undefined, project: route.query.project ? String(route.query.project) : undefined }))
  const query = computed(() => params(scope.value))
  const statistics = useQuery({ queryKey: ['insights', query], queryFn: ({ signal }) => api<components['schemas']['StorageInsights']>('/api/insights?' + query.value, { signal }), refetchInterval: q => q.state.data?.usage ? 30_000 : 5000 })
  return { scope, query, statistics }
}
export const metric = (value: string | null | undefined) => value == null ? null : Number(value)
export function taskTone(kind: string) {
  if (['gc', 'cleanup', 'purge', 'sweep', 'reclaim', 'pack_reclaim'].includes(kind)) return 'mint'
  if (['integrity', 'range', 'pack_range'].includes(kind)) return 'sky'
  if (['pack', 'reuse', 'pack_reuse'].includes(kind)) return 'rose'
  if (['upload', 'cache_flush'].includes(kind)) return 'cream'
  return 'lilac'
}
