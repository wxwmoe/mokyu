import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ArchiveRestore, Boxes, BrushCleaning, CircleGauge, CloudUpload, FolderMinus, Package, Recycle, ScanHeart, Unplug, Wrench } from 'lucide-vue-next'
import { api, session } from '../api/client'
import type { components } from '../api/schema'
export type Job = components['schemas']['Job']
export const policies = ['pack', 'reuse', 'reclaim', 'repack', 'range', 'gc', 'cleanup']
export const taskTypes = [...policies, 'integrity', 'external_test', 'quarantine', 'recheck', 'sweep', 'unpack', 'purge', 'upload', 'cache_flush', 'storage_migration']
export function jobIcon(taskType: string) { return ({ pack: Package, reuse: Boxes, reclaim: Recycle, repack: ArchiveRestore, range: CircleGauge, gc: Recycle, cleanup: BrushCleaning, integrity: ScanHeart, sweep: Wrench, unpack: Unplug, purge: FolderMinus, upload: CloudUpload, cache_flush: CloudUpload, storage_migration: CloudUpload })[taskType] || Wrench }
export function useMaintenance() {
  return useQuery({ queryKey: ['maintenance'], enabled: computed(() => session.value?.role === 'admin'), queryFn: ({ signal }) => api<components['schemas']['MaintenanceStatus']>('/api/v1/maintenance', { signal }), refetchInterval: 5000 })
}
