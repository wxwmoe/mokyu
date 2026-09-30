<script setup lang="ts">
import { ref } from 'vue'
import { DropdownMenuItem } from 'reka-ui'
import { Check } from 'lucide-vue-next'
import UiMenu from './ui/UiMenu.vue'
import { locale, locales, setLocale, t, type Locale } from '../app/i18n'
import { savePreferences, session } from '../api/client'
import { notify, report } from '../app/feedback'
const busy = ref(false)
async function choose(value: Locale) {
  busy.value = true
  try { if (session.value) await savePreferences({ locale: value }); else await setLocale(value); notify(t('languageSaved')) }
  catch (error) { report(error) }
  finally { busy.value = false }
}
</script>
<template><UiMenu :label="t('language')"><template #trigger><span class="locale-short">{{ t('short_' + locale) }}</span></template><DropdownMenuItem v-for="language in locales" :key="language" class="menu-item language-item" :disabled="busy" @select="choose(language)"><span :lang="language">{{ t('language_' + language) }}</span><Check v-if="locale === language" :size="15" /></DropdownMenuItem></UiMenu></template>
