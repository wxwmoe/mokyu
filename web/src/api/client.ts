import { QueryClient } from '@tanstack/vue-query'
import { shallowRef } from 'vue'
import type { components } from './schema'

export type Session = components['schemas']['SessionView']
export type Bucket = components['schemas']['BucketView']
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
  const reply = await fetch(path, { credentials: 'same-origin', ...init, headers })
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
  try { session.value = await api<Session>('/api/session') }
  catch (error) {
    if (!(error instanceof ApiError) || ![401, 403].includes(error.status)) throw error
    session.value = null
    queries.clear()
  }
  return session.value
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
  return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(value / 1024 ** unit)} ${units[unit]}`
}
