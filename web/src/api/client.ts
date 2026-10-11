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

const refreshPath = '/api/v1/auth/refresh'
const refreshStorageKey = 'mokyu.auth.refresh'
let volatileRefresh = ''
let refreshFlight: Promise<boolean> | undefined
let forcedRefresh = false
let deadline = 0, retryAt = 0, generation = 0
let recoveryNeeded = false
const channel = typeof BroadcastChannel === 'undefined' ? undefined : new BroadcastChannel('mokyu.auth')
export function csrfToken() { return session.value?.csrf_token }
function storedRefresh() {
  try { return localStorage.getItem(refreshStorageKey) || volatileRefresh } catch { return volatileRefresh }
}
function saveRefresh(value: unknown) {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) return
  volatileRefresh = value
  try { localStorage.setItem(refreshStorageKey, value) } catch { /* private browsing may reject storage */ }
}
function clearRefresh() {
  volatileRefresh = ''
  try { localStorage.removeItem(refreshStorageKey) } catch { /* storage is optional */ }
}

async function request<T>(path: string, init: RequestInit = {}, format: 'json' | 'blob' = 'json'): Promise<T> {
  const expected = generation
  let reply: Response
  try { reply = await fetch(path, { credentials: 'same-origin', ...init }) }
  catch (error) { if (error instanceof DOMException && error.name === 'AbortError') throw error; throw new ApiError(0, 'Network') }
  if (!reply.ok) {
    const error = await reply.json().catch(() => ({}))
    throw new ApiError(reply.status, error.code || 'RequestFailed', error.request_id || reply.headers.get('x-request-id'))
  }
  const rotated = reply.headers.get('x-mokyu-refresh-token')
  if (rotated && generation === expected) saveRefresh(rotated)
  if (format === 'blob') return await reply.blob() as T
  if (reply.status === 204) return undefined as T
  const body = await reply.json()
  if (body && typeof body === 'object' && 'refresh_token' in body) {
    if (generation === expected) saveRefresh(body.refresh_token)
    delete body.refresh_token
  }
  return body as T
}

async function applySession(current: Session, expected: number) {
  if (generation !== expected) return
  if (session.value && session.value.session_id !== current.session_id) queries.clear()
  session.value = current
  deadline = Date.now() + Math.max(0, current.refresh_after) * 1000
  await applyPreferences(current)
}
export function resetSession(clear = true) { generation++; session.value = null; queries.clear(); recoveryNeeded = false; deadline = 0; retryAt = 0; if (clear) clearRefresh(); else volatileRefresh = '' }
const denied = (error: unknown) => error instanceof ApiError && [401, 403].includes(error.status)

async function refreshLock<T>(run: () => Promise<T>): Promise<T> {
  if (navigator.locks) return navigator.locks.request('mokyu.auth.refresh', run)
  // HTTP previews lack Web Locks; a short lease coordinates tabs there too.
  const key = 'mokyu.auth.lock', owner = uuid()
  for (;;) {
    try {
      const lock = JSON.parse(localStorage.getItem(key) || 'null') as { owner: string; expires: number } | null
      if (!lock || lock.expires <= Date.now()) {
        localStorage.setItem(key, JSON.stringify({ owner, expires: Date.now() + 20_000 }))
        await new Promise(resolve => setTimeout(resolve, 40))
        if (JSON.parse(localStorage.getItem(key) || 'null')?.owner === owner) break
      }
    } catch { return run() }
    await new Promise(resolve => setTimeout(resolve, 100))
  }
  const release = () => { try { if (JSON.parse(localStorage.getItem(key) || 'null')?.owner === owner) localStorage.removeItem(key) } catch { /* storage is optional */ } }
  window.addEventListener('pagehide', release, { once: true })
  try { return await run() }
  finally { window.removeEventListener('pagehide', release); release() }
}

export async function refreshAuth(force = false): Promise<boolean> {
  if (refreshFlight) {
    const wasForced = forcedRefresh
    const refreshed = await refreshFlight
    return force && !wasForced && refreshed ? refreshAuth(true) : refreshed
  }
  if (!refreshFlight) {
    const expected = generation
    forcedRefresh = force
    refreshFlight = refreshLock(async () => {
      if (generation !== expected) return false
      try {
        // A sibling tab may have renewed while this tab waited for the lock.
        try {
          const current = await request<Session>('/api/v1/auth/session', { cache: 'no-store', signal: AbortSignal.timeout(8_000) })
          await applySession(current, expected)
          if (!force && current.refresh_after > 30 && !recoveryNeeded) return generation === expected
        } catch (error) { if (!denied(error)) throw error }
        const refresh = storedRefresh()
        if (!refresh) {
          recoveryNeeded = !!session.value
          if (!session.value && generation === expected) resetSession()
          return false
        }
        const current = await request<Session>(refreshPath, {
          method: 'POST', cache: 'no-store', signal: AbortSignal.timeout(8_000),
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ refresh_token: refresh }),
        })
        await applySession(current, expected)
        if (generation !== expected) return false
        recoveryNeeded = false; retryAt = 0
        channel?.postMessage('refreshed')
        return true
      } catch (error) {
        if (!denied(error)) { retryAt = Date.now() + 30_000; throw error }
        if (generation !== expected) return false
        clearRefresh()
        // A valid access session can still recover with a password.
        try { await applySession(await request<Session>('/api/v1/auth/session', { cache: 'no-store' }), expected); recoveryNeeded = true }
        catch (currentError) {
          if (!denied(currentError)) throw currentError
          if (expected === generation) resetSession()
        }
        return false
      }
    }).finally(() => { refreshFlight = undefined; forcedRefresh = false })
  }
  return refreshFlight
}

export async function ensureAuth() {
  if (refreshFlight) await refreshFlight
  else if (session.value && !recoveryNeeded && Date.now() >= Math.max(deadline, retryAt)) await refreshAuth()
}

export async function api<T>(path: string, init: RequestInit = {}, format: 'json' | 'blob' = 'json'): Promise<T> {
  const managed = !path.startsWith('/api/v1/auth/') && path !== '/api/v1/account/reauth'
  if (managed) await ensureAuth()
  const send = () => {
    const headers = new Headers(init.headers)
    if (!['GET', 'HEAD'].includes((init.method || 'GET').toUpperCase()) && path !== refreshPath) {
      const csrf = csrfToken()
      if (csrf) headers.set('X-CSRF-Token', csrf)
    }
    return request<T>(path, { ...init, headers }, format)
  }
  try { return await send() }
  catch (error) {
    if (managed && error instanceof ApiError && error.code === 'AccessDenied') {
      let expired = false
      try { await request<Session>('/api/v1/auth/session', { cache: 'no-store' }) }
      catch (currentError) { expired = denied(currentError) }
      if (expired && await refreshAuth()) return send()
    }
    // This code is emitted only by the pre-handler gate, so no mutation ran.
    if (!managed || !(error instanceof ApiError) || !['CsrfTokenMismatch', 'Unauthorized'].includes(error.code) || !await refreshAuth()) throw error
    return send()
  }
}

export function write<T>(path: string, body: unknown, method = 'POST'): Promise<T> {
  return api(path, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
}

export async function refreshSession() {
  const expected = generation
  try { await applySession(await request<Session>('/api/v1/auth/session', { cache: 'no-store' }), expected); await ensureAuth() }
  catch (error) { if (!denied(error)) throw error; await refreshAuth() }
  return session.value
}

export function startAuthManager() {
  const tick = () => { void ensureAuth().catch(() => {}) }
  const online = () => { retryAt = 0; tick() }
  const refreshed = () => {
    const expected = generation
    if (session.value) void request<Session>('/api/v1/auth/session', { cache: 'no-store' }).then(current => applySession(current, expected)).catch(() => {})
  }
  const timer = window.setInterval(tick, 60_000)
  channel?.addEventListener('message', refreshed)
  window.addEventListener('focus', tick)
  window.addEventListener('online', online)
  document.addEventListener('visibilitychange', tick)
  return () => {
    window.clearInterval(timer); channel?.removeEventListener('message', refreshed)
    window.removeEventListener('focus', tick); window.removeEventListener('online', online)
    document.removeEventListener('visibilitychange', tick)
  }
}
let preferenceQueue: Promise<unknown> = Promise.resolve()
export function savePreferences(changes: Partial<Preferences>) {
  const owner = session.value?.id
  const operation = preferenceQueue.catch(() => {}).then(async () => {
    const current = session.value
    if (!current || current.id !== owner) throw new ApiError(401, 'Unauthorized')
    const input: Preferences = { display_name: current.display_name, locale: current.locale ?? null,
      theme: current.theme ?? null, avatar_email: current.avatar_email, avatar_enabled: current.avatar_enabled, ...changes }
    const profile = await write<Profile>('/api/v1/account', input, 'PUT')
    if (session.value?.id === owner) { session.value = { ...session.value, ...profile }; await applyPreferences(profile) }
    saveLocal('mokyu.preferences-change', uuid())
    return profile
  })
  preferenceQueue = operation
  return operation
}

export async function signOut() {
  try {
    if (refreshFlight) await refreshFlight.catch(() => {})
    await api('/api/v1/auth/logout', { method: 'POST' })
  } catch (error) {
    if (!denied(error)) throw error
  } finally {
    resetSession()
    identityChanged()
  }
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
