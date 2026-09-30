import { QueryClient } from '@tanstack/vue-query'
import { shallowRef } from 'vue'
import type { components } from './schema'
import { applyPreferences } from '../app/preferences'
import { locale } from '../app/i18n'
import { saveLocal } from '../app/theme'

export type Session = components['schemas']['SessionView']
export type Bucket = components['schemas']['BucketView']
export type Profile = components['schemas']['Profile']
type Preferences = components['schemas']['Preferences']
export const session = shallowRef<Session | null>(null)
export function uuid() {
  const value = crypto.getRandomValues(new Uint8Array(16)); value[6] = (value[6]! & 15) | 64; value[8] = (value[8]! & 63) | 128
  const hex = Array.from(value, v => v.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
export const queries = new QueryClient({ defaultOptions: { queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: false } } })

export class ApiError extends Error {
  constructor(public status: number, public code: string, public requestId?: string) {
    super(code)
  }
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  if (init.method && !['GET', 'HEAD'].includes(init.method) && session.value) {
    headers.set('X-CSRF-Token', session.value.csrf_token)
  }
  let reply: Response
  try { reply = await fetch(path, { credentials: 'same-origin', ...init, headers }) }
  catch (error) { if (error instanceof DOMException && error.name === 'AbortError') throw error; throw new ApiError(0, 'Network') }
  if (!reply.ok) {
    const error = await reply.json().catch(() => ({}))
    throw new ApiError(reply.status, error.code || 'RequestFailed', error.request_id || reply.headers.get('x-request-id'))
  }
  return reply.status === 204 ? undefined as T : reply.json()
}

export function write<T>(path: string, body: unknown, method = 'POST'): Promise<T> {
  return api(path, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
}

export async function refreshSession() {
  try { const current = await api<Session>('/api/session'); if (session.value?.id !== current.id) queries.clear(); session.value = current; await applyPreferences(current) }
  catch (error) {
    if (!(error instanceof ApiError) || ![401, 403].includes(error.status)) throw error
    session.value = null
    queries.clear()
  }
  return session.value
}

let preferenceQueue: Promise<unknown> = Promise.resolve()
export function savePreferences(changes: Partial<Preferences>) {
  const owner = session.value?.id
  const operation = preferenceQueue.catch(() => {}).then(async () => {
    const current = session.value
    if (!current || current.id !== owner) throw new ApiError(401, 'Unauthorized')
    const input: Preferences = { display_name: current.display_name, locale: current.locale ?? null,
      theme: current.theme ?? null, avatar_email: current.avatar_email, avatar_enabled: current.avatar_enabled, ...changes }
    const profile = await write<Profile>('/api/me', input, 'PUT')
    if (session.value?.id === owner) { session.value = { ...session.value, ...profile }; await applyPreferences(profile) }
    saveLocal('mokyu.preferences-change', uuid())
    return profile
  })
  preferenceQueue = operation
  return operation
}

export async function signOut() {
  await api('/api/logout', { method: 'POST' })
  session.value = null
  queries.clear()
  identityChanged()
}

export function identityChanged() { saveLocal('mokyu.identity-change', uuid()) }

export function params(values: Record<string, string | undefined>) {
  return new URLSearchParams(Object.entries(values).filter((entry): entry is [string, string] => entry[1] !== undefined)).toString()
}

export function bytes(value: number) {
  if (!Number.isFinite(value)) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
  const unit = Math.min(5, Math.floor(Math.log2(Math.max(1, Math.abs(value))) / 10))
  return `${new Intl.NumberFormat(locale.value, { maximumFractionDigits: 1 }).format(value / 1024 ** unit)} ${units[unit]}`
}

export function parseBytes(value: string, factor = '1'): string | null {
  if (!value.trim()) return null
  if (!/^\d+(\.\d{1,3})?$/.test(value)) throw new Error('Invalid size')
  const [whole, fraction = ''] = value.split('.'), scale = 10n ** BigInt(fraction.length)
  const n = BigInt(whole! + fraction) * BigInt(factor)
  if (n % scale || n / scale > 9223372036854775807n) throw new Error('Invalid size')
  return String(n / scale)
}
