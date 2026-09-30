import { QueryClient } from '@tanstack/vue-query'
import { shallowRef } from 'vue'
import type { components } from './schema'
import { applyPreferences } from '../app/preferences'
import { locale } from '../app/i18n'

export type Session = components['schemas']['SessionView']
export type Bucket = components['schemas']['BucketView']
export type Profile = components['schemas']['Profile']
type Preferences = components['schemas']['Preferences']
export const session = shallowRef<Session | null>(null)
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
  try { session.value = await api<Session>('/api/session'); await applyPreferences(session.value) }
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
    return profile
  })
  preferenceQueue = operation
  return operation
}

export async function signOut() {
  await api('/api/logout', { method: 'POST' })
  session.value = null
  queries.clear()
}

export function params(values: Record<string, string | undefined>) {
  return new URLSearchParams(Object.entries(values).filter((entry): entry is [string, string] => entry[1] !== undefined)).toString()
}

export function bytes(value: number) {
  if (!Number.isFinite(value)) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
  const unit = Math.min(5, Math.floor(Math.log2(Math.max(1, Math.abs(value))) / 10))
  return `${new Intl.NumberFormat(locale.value, { maximumFractionDigits: 1 }).format(value / 1024 ** unit)} ${units[unit]}`
}
