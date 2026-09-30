import { defineConfig } from '@playwright/test'

if (process.env.MOKYU_TEST_ALLOW_STATE_CHANGES !== 'isolated-only' || !process.env.MOKYU_TEST_WEB) {
  throw new Error('UI tests require an explicitly isolated Mokyu service and MOKYU_TEST_WEB')
}
export default defineConfig({
  testDir: './tests', workers: 1, retries: 0,
  outputDir: process.env.MOKYU_TEST_RESULTS || '/results/web-tests',
  use: { baseURL: process.env.MOKYU_TEST_WEB, screenshot: 'only-on-failure', trace: 'off', reducedMotion: 'reduce' },
})
