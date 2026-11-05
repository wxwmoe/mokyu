import { shallowRef } from 'vue'

export const passwordPrompt = shallowRef<((confirmed: boolean) => void) | null>(null)
let pending: Promise<boolean> | undefined

export function confirmPassword() {
  return pending ??= new Promise<boolean>(resolve => {
    passwordPrompt.value = confirmed => {
      pending = undefined
      passwordPrompt.value = null
      resolve(confirmed)
    }
  })
}
