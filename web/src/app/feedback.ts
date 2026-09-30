import { ref } from 'vue'
import { ApiError } from '../api/client'
export interface Notice { id: number; title: string; description?: string; requestId?: string; tone: 'success' | 'error' | 'info' }
export const notices = ref<Notice[]>([])
let id = 0
export function notify(title: string, description?: string, tone: Notice['tone'] = 'success') {
  notices.value = [...notices.value.slice(-2), { id: ++id, title, description, tone }]
}
export function dismiss(id: number) { notices.value = notices.value.filter(item => item.id !== id) }
export function report(error: unknown) {
  const failure = error instanceof ApiError
  notices.value = [...notices.value.slice(-2), { id: ++id, tone: 'error', title: 'Something needs a little attention',
    description: failure ? error.code : error instanceof Error ? error.message : String(error), requestId: failure ? error.requestId : undefined }]
}
export async function copy(value: string, title = 'Copied to clipboard') {
  try { await navigator.clipboard.writeText(value); notify(title) }
  catch { notify('Copy this value', value, 'info') }
}
