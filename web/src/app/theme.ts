import { ref, watch } from 'vue'
export type Theme = 'auto' | 'light' | 'dark'
export function localValue(key: string) { try { return localStorage.getItem(key) } catch { return null } }
export function saveLocal(key: string, value: string) { try { localStorage.setItem(key, value) } catch { /* Preferences still work for this page. */ } }
const saved = localValue('mokyu.theme')
export const theme = ref<Theme>(saved === 'light' || saved === 'dark' ? saved : 'auto')
export const appearance = ref<'light' | 'dark'>('light')
const preference = matchMedia('(prefers-color-scheme: dark)')
function apply() {
  appearance.value = theme.value === 'auto' ? (preference.matches ? 'dark' : 'light') : theme.value
  document.documentElement.dataset.theme = appearance.value
  document.querySelector('meta[name="theme-color"]')?.setAttribute('content', appearance.value === 'dark' ? '#211e2a' : '#fff9f8')
}
preference.addEventListener('change', apply)
watch(theme, apply, { immediate: true })
export function setTheme(value: Theme) { theme.value = value; saveLocal('mokyu.theme', value) }
