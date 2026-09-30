import { browserLocale, locales, setLocale, type Locale } from './i18n'
import { saveLocal, setTheme, type Theme } from './theme'

export async function applyPreferences(profile: { locale?: string | null; theme?: string | null }) {
  const language = locales.includes(profile.locale as Locale) ? profile.locale as Locale : browserLocale()
  await setLocale(language, false)
  saveLocal('mokyu.locale', profile.locale || 'auto')
  setTheme(['light', 'dark'].includes(profile.theme || '') ? profile.theme as Theme : 'auto')
}
