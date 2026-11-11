import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ArchiveRestore, BrushCleaning, CloudDownload, CloudUpload, FolderMinus, Package, Recycle, ScanHeart, Unplug, Wrench } from 'lucide-vue-next'
import { api, session } from '../api/client'
import type { components } from '../api/schema'
export type Job = components['schemas']['Job']
export const policies = ['pack', 'reclaim', 'repack', 'gc', 'cleanup']
export const taskTypes = [...policies, 'integrity', 'external_test', 'quarantine', 'recheck', 'sweep', 'unpack', 'purge', 'upload', 'cache_flush', 'storage_migration', 'mirroring']
export function mirroringPhase(job: Job) {
  const detail = job.detail as Record<string, any>
  return job.status === 'failed' ? 'failed' : job.status === 'paused' ? 'paused' : detail.published ? 'saved' : detail.phase || 'queued'
}
export function jobIcon(taskType: string) { return ({ pack: Package, reclaim: Recycle, repack: ArchiveRestore, gc: Recycle, cleanup: BrushCleaning, integrity: ScanHeart, sweep: Wrench, unpack: Unplug, purge: FolderMinus, upload: CloudUpload, cache_flush: CloudUpload, storage_migration: CloudUpload, mirroring: CloudDownload })[taskType] || Wrench }
export function useMaintenance() {
  return useQuery({ queryKey: ['maintenance'], enabled: computed(() => session.value?.role === 'admin'), queryFn: ({ signal }) => api<components['schemas']['MaintenanceStatus']>('/api/v1/maintenance', { signal }), refetchInterval: 5000 })
}
