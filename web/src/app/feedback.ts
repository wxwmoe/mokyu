import { ref } from 'vue'
import { ApiError } from '../api/client'
import { t } from './i18n'
export interface Notice { id: number; title: string; description?: string; requestId?: string; tone: 'success' | 'error' | 'info' }
export const notices = ref<Notice[]>([])
export const manualCopy = ref('')
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
export async function copy(value: string, title = t('copied')): Promise<boolean> {
  try { await navigator.clipboard.writeText(value); notify(title); return true }
  catch { /* HTTP previews need the user-initiated legacy clipboard path. */ }
  const input = document.createElement('textarea'), previous = document.activeElement
  input.value = value; input.setAttribute('readonly', ''); input.style.position = 'fixed'; input.style.left = '-10000px'; input.style.top = '0'; input.style.width = '1px'; input.style.height = '1px'
  const owner = previous instanceof HTMLElement ? previous.closest('[role="dialog"]') : null
  ;(owner || document.body).append(input); input.select()
  let copied = false
  try {
    input.addEventListener('copy', event => { event.clipboardData?.setData('text/plain', value); event.preventDefault() }, { once: true })
    input.focus(); input.select(); copied = document.execCommand('copy')
  } catch { /* use the selectable fallback */ }
  finally { input.remove(); if (previous instanceof HTMLElement) previous.focus() }
  if (copied) { notify(title); return true }
  manualCopy.value = value
  return false
}
