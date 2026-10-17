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
  const statistics = useQuery({ queryKey: ['insights', query], queryFn: ({ signal }) => api<components['schemas']['StorageInsights']>('/api/v1/usage/summary?' + query.value, { signal }), refetchInterval: q => q.state.data?.usage ? 30_000 : 5000 })
  return { scope, query, statistics }
}
export const metric = (value: string | null | undefined) => value == null ? null : Number(value)
export function requestHistory(runtime: Runtime | undefined) {
  if (runtime?.scope === 'cluster') {
    let segment = 0
    return [...(runtime.cluster?.history || []), ...(runtime.cluster?.current ? [runtime.cluster.current] : [])].map((p, i, all) => { if (i && new Date(p.at).getTime() - new Date(all[i - 1]!.at).getTime() > (p.interval_seconds ?? 60) * 3000) segment++; return { at: p.at, epoch: `${segment}:${p.rate_nodes}/${p.expected_nodes}`, value: p.rates?.requests ?? null } })
  }
  return (runtime?.history || []).map((p, i, all) => { const previous = all[i - 1], seconds = previous ? (new Date(p.at).getTime() - new Date(previous.at).getTime()) / 1000 : 0; return { at: p.at, epoch: p.epoch, value: previous?.epoch === p.epoch && seconds > 0 && Number(p.requests) >= Number(previous.requests) ? (Number(p.requests) - Number(previous.requests)) / seconds : null } })
}
export function taskTone(taskType: string) {
  if (['gc', 'cleanup', 'purge', 'sweep', 'reclaim', 'pack_reclaim'].includes(taskType)) return 'mint'
  if (['integrity', 'range', 'pack_range'].includes(taskType)) return 'sky'
  if (['pack', 'reuse', 'pack_reuse'].includes(taskType)) return 'rose'
  if (['upload', 'cache_flush'].includes(taskType)) return 'cream'
  return 'lilac'
}
