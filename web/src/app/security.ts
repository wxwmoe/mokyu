import { ref } from 'vue'
import { ApiError } from '../api/client'
export const verifyOpen = ref(false)
let pending: Promise<boolean> | undefined
let answer: ((value: boolean) => void) | undefined
export function verified(value: boolean) { answer?.(value); answer = undefined; pending = undefined; verifyOpen.value = false }
export async function secure<T>(operation: () => Promise<T>): Promise<T> {
  try { return await operation() }
  catch (error) {
    if (!(error instanceof ApiError) || error.code !== 'ReauthenticationRequired') throw error
    if (!pending) { pending = new Promise(resolve => { answer = resolve }); verifyOpen.value = true }
    if (!await pending) throw new ApiError(0, 'Cancelled')
    return operation()
  }
}
