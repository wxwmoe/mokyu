import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  build: { manifest: true, target: 'es2022', sourcemap: false, assetsInlineLimit: 0 },
  server: {
    proxy: { '/api': { target: process.env.MOKYU_API_ORIGIN || 'http://127.0.0.1:9002' } },
  },
})
