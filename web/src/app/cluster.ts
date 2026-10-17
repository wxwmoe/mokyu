import { computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import { api, session } from '../api/client'
import type { components } from '../api/schema'
import { t } from './i18n'

export type NodeView = components['schemas']['NodeView']
export type ClusterOverview = components['schemas']['ClusterOverview']
export function useDeployment(interval = 15_000) {
  const route = useRoute(), router = useRouter()
  const status = useQuery({ queryKey: ['service-status'], enabled: computed(() => session.value?.role === 'admin'), queryFn: ({ signal }) => api<components['schemas']['ServiceStatus']>('/api/v1/system/status', { signal }), refetchInterval: interval })
  watch([() => status.data.value?.deployment_mode, () => route.fullPath], ([mode]) => { if (mode === 'single' && (route.query.node || route.query.view)) void router.replace({ query: { ...route.query, node: undefined, view: undefined, configuration: undefined } }) }, { immediate: true })
  const cluster = computed(() => session.value?.role === 'admin' && status.data.value?.deployment_mode === 'cluster')
  const overview = useQuery({ queryKey: ['cluster-status'], enabled: cluster, queryFn: ({ signal }) => api<ClusterOverview>('/api/v1/cluster/status', { signal }), refetchInterval: () => cluster.value ? interval : false })
  return { status, cluster, overview }
}
export function nodeTone(state: string) {
  if (['online', 'starting', 'equivalent'].includes(state)) return 'sky'
  if (['normal', 'applied'].includes(state)) return 'mint'
  if (state === 'maintenance') return 'lilac'
  if (['unresponsive', 'recovery'].includes(state)) return 'rose'
  if (['not_reported', 'restricted', 'restart_required'].includes(state)) return 'cream'
  return 'gray'
}
export function rollout(overview: ClusterOverview | undefined, section: string) {
  const rows = overview?.settings.filter(row => row.section === section && row.state !== 'not_applicable') || []
  const total = rows.reduce((n, row) => n + Number(row.count), 0)
  const equivalent = rows.filter(row => row.state === 'equivalent').reduce((n, row) => n + Number(row.count), 0)
  const applied = rows.filter(row => row.state === 'applied').reduce((n, row) => n + Number(row.count), 0) + equivalent
  const pending = rows.filter(row => row.state === 'restart_required').reduce((n, row) => n + Number(row.count), 0)
  const unknown = rows.filter(row => row.state === 'unknown').reduce((n, row) => n + Number(row.count), 0)
  return {
    state: pending ? 'restart_required' : !total || unknown ? 'unknown' : equivalent ? 'equivalent' : 'applied',
    text: total ? [t('clusterRollout', { applied, total }), ...(pending ? [t('clusterPending', { count: pending })] : []), ...(unknown ? [t('clusterUnknown', { count: unknown })] : [])].join(' · ') : t('clusterNoApplicableNodes'),
  }
}
