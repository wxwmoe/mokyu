import { createApp } from 'vue'
import { VueQueryPlugin } from '@tanstack/vue-query'
import App from './app/App.vue'
import { router } from './app/router'
import { queries } from './api/client'
import '@fontsource-variable/nunito'
import './app/styles.css'

createApp(App).use(router).use(VueQueryPlugin, { queryClient: queries }).mount('#app')
