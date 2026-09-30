import { shallowRef, ref } from 'vue'
import { localValue, saveLocal } from './theme'
export type Locale = 'en' | 'zh-CN' | 'ja'
export const locales: Locale[] = ['en', 'zh-CN', 'ja']
const loaders = { en: () => import('../locales/i18n.en.js'), 'zh-CN': () => import('../locales/i18n.zh-CN.js'), ja: () => import('../locales/i18n.ja.js') }
export const locale = ref<Locale>('en')
const messages = shallowRef<Record<string, string>>({})
let generation = 0
export function browserLocale(languages = navigator.languages): Locale {
  for (const language of languages) {
    const code = language.toLowerCase()
    if (code === 'en' || code.startsWith('en-')) return 'en'
    if (code === 'zh' || code.startsWith('zh-')) return 'zh-CN'
    if (code === 'ja' || code.startsWith('ja-')) return 'ja'
  }
  return 'en'
}
export async function setLocale(value: Locale, persist = true) {
  const request = ++generation
  const dictionary = await loaders[value]()
  if (request !== generation) return
  messages.value = dictionary.default
  locale.value = value
  document.documentElement.lang = value
  if (persist) saveLocal('mokyu.locale', value)
}
export async function initializeLocale() {
  const saved = localValue('mokyu.locale') as Locale
  try { await setLocale(locales.includes(saved) ? saved : browserLocale(), false) }
  catch { await setLocale('en', false) }
}
export function t(key: string, values: Record<string, string | number> = {}) {
  return (messages.value[key] || key).replace(/\{(\w+)\}/g, (match, name) => String(values[name] ?? match))
}
export function date(value: string) { return new Date(value).toLocaleString(locale.value) }
