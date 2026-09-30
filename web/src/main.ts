import { createApp } from 'vue'
import { VueQueryPlugin } from '@tanstack/vue-query'
import App from './app/App.vue'
import { router } from './app/router'
import { queries } from './api/client'
import '@fontsource-variable/nunito'
import './fonts/chinese.css'
import './fonts/japanese.css'
import './app/styles.css'
import { initializeLocale } from './app/i18n'

await initializeLocale()
createApp(App).use(router).use(VueQueryPlugin, { queryClient: queries }).mount('#app')
