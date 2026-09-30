import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ArchiveRestore, Boxes, BrushCleaning, CircleGauge, CloudUpload, FolderMinus, Package, Recycle, ScanHeart, Unplug, Wrench } from 'lucide-vue-next'
import { api, session } from '../api/client'
import type { components } from '../api/schema'
export type Job = components['schemas']['Job']
export const policies = ['pack', 'reuse', 'reclaim', 'repack', 'range', 'gc', 'cleanup']
export const kinds = [...policies, 'integrity', 'sweep', 'unpack', 'purge', 'upload', 'cache_flush']
export function jobIcon(kind: string) { return ({ pack: Package, reuse: Boxes, reclaim: Recycle, repack: ArchiveRestore, range: CircleGauge, gc: Recycle, cleanup: BrushCleaning, integrity: ScanHeart, sweep: Wrench, unpack: Unplug, purge: FolderMinus, upload: CloudUpload, cache_flush: CloudUpload })[kind] || Wrench }
export function useMaintenance() {
  return useQuery({ queryKey: ['maintenance'], enabled: computed(() => session.value?.role === 'admin'), queryFn: ({ signal }) => api<components['schemas']['MaintenanceStatus']>('/api/maintenance', { signal }), refetchInterval: 5000 })
}
