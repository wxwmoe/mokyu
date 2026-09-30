import { ref } from 'vue'
import { ApiError } from '../api/client'
import { t } from './i18n'
export interface Notice { id: number; title: string; description?: string; requestId?: string; tone: 'success' | 'error' | 'info' }
export const notices = ref<Notice[]>([])
let id = 0
export function notify(title: string, description?: string, tone: Notice['tone'] = 'success') {
  notices.value = [...notices.value.slice(-2), { id: ++id, title, description, tone }]
}
export function dismiss(id: number) { notices.value = notices.value.filter(item => item.id !== id) }
export function report(error: unknown) {
  if (error instanceof ApiError && error.code === 'Cancelled') return
  const failure = error instanceof ApiError
  notices.value = [...notices.value.slice(-2), { id: ++id, tone: 'error', title: t('attention'),
    description: errorText(error), requestId: failure ? error.requestId : undefined }]
}
export function errorText(error: unknown) {
  if (error instanceof ApiError) {
    const translated = t(`error_${error.code}`)
    return translated.startsWith('error_') ? `${error.status} ${error.code}` : translated
  }
  return error instanceof Error ? error.message : String(error)
}
export async function copy(value: string, title = t('copied')) {
  try { await navigator.clipboard.writeText(value); notify(title) }
  catch { notify(t('copyValue'), value, 'info') }
}
