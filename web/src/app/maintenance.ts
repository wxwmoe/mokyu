import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ArchiveRestore, BrushCleaning, CloudDownload, CloudUpload, Copy, MoveRight, FolderMinus, Package, Recycle, ScanHeart, Unplug, Wrench } from 'lucide-vue-next'
import { t } from './i18n'
import { api, session } from '../api/client'
import type { components } from '../api/schema'
export type Job = components['schemas']['Job']
export const policies = ['pack', 'reclaim', 'repack', 'gc', 'cleanup']
export const taskTypes = [...policies, 'integrity', 'external_test', 'quarantine', 'recheck', 'sweep', 'unpack', 'purge', 'upload', 'cache_flush', 'storage_migration', 'mirroring']
export function mirroringPhase(job: Job) {
  const detail = job.detail as Record<string, any>
  return job.status === 'failed' ? 'failed' : job.status === 'paused' ? 'paused' : detail.published ? 'saved' : detail.phase || 'queued'
}
type ReplicationSummary = { action: 'copy' | 'move'; source: string; target: string }
export function taskReplication(job: Job) {
  return (job.detail as { replication?: ReplicationSummary } | null)?.replication
}
export function replicationReason(code: string) {
  const key = 'replicationError_' + code, message = t(key)
  return message === key ? code : message
}
export function taskIcon(job: Job) {
  const replication = taskReplication(job)
  if (replication) return replication.action === 'move' ? MoveRight : Copy
  return jobIcon(job.policy)
}
export function taskContext(job: Job) {
  const replication = taskReplication(job)
  return replication ? replication.source + ' → ' + replication.target : job.bucket_name || job.created_by
}
export function taskTitle(job: Job) {
  const replication = taskReplication(job)
  if (replication) return t(replication.action === 'move' ? 'taskMoveObjects' : 'taskCopyObjects')
  return t('task_' + job.policy)
}
export function taskState(job: Job) {
  if (job.policy === 'mirroring') return t('mirrorPhase_' + mirroringPhase(job))
  if (taskReplication(job) && (job.detail as { phase: string }).phase === 'cancelled') return t('replicationPhase_cancelled')
  return t(job.status)
}
export function jobIcon(taskType: string) { return ({ pack: Package, reclaim: Recycle, repack: ArchiveRestore, gc: Recycle, cleanup: BrushCleaning, integrity: ScanHeart, sweep: Wrench, unpack: Unplug, purge: FolderMinus, upload: CloudUpload, cache_flush: CloudUpload, storage_migration: CloudUpload, mirroring: CloudDownload })[taskType] || Wrench }
export function useMaintenance() {
  return useQuery({ queryKey: ['maintenance'], enabled: computed(() => session.value?.role === 'admin'), queryFn: ({ signal }) => api<components['schemas']['MaintenanceStatus']>('/api/v1/maintenance', { signal }), refetchInterval: 5000 })
}
