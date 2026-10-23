import type { Locator } from '@playwright/test'

export async function openStorageGroup(editor: Locator, group = 'storeGroupIo') {
  const details = editor.locator(`[data-profile-group="${group}"]`)
  if (await details.getAttribute('open') === null) await details.locator('summary').click()
}
