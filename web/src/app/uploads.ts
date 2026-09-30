import { markRaw, reactive, watch } from 'vue'
import { api, ApiError, queries, session, uuid } from '../api/client'
import type { components } from '../api/schema'

export type Transfer = components['schemas']['Transfer']
type Part = components['schemas']['PartReceipt']
type Parts = components['schemas']['PartsPage']
type Input = components['schemas']['UploadInput']
export interface UploadJob {
  client: string; owner: string; bucket: string; input: Input; file?: File; transfer?: Transfer
  state: 'queued' | 'checking' | 'uploading' | 'paused' | 'completing' | 'completed' | 'failed' | 'aborted'
  accepted: number; sending: number; speed: number; verified: number; error?: unknown
  controller: AbortController; running: boolean; paused: boolean
}
export const uploads = reactive<UploadJob[]>([])
let worker: Worker | undefined, sequence = 0
const hashes = new Map<number, { resolve: (value: string) => void; reject: (error: unknown) => void; cleanup: () => void }>()
function hash(blob: Blob, signal: AbortSignal): Promise<string> {
  signal.throwIfAborted()
  if (!worker) {
    worker = new Worker(new URL('./upload-hash.worker.ts', import.meta.url), { type: 'module' })
    worker.onmessage = ({ data }) => {
      const task = hashes.get(data.id); if (!task) return
      hashes.delete(data.id); task.cleanup()
      if (data.error) task.reject(new ApiError(0, 'FileReadFailed')); else task.resolve(data.hash)
    }
    worker.onerror = () => {
      for (const task of hashes.values()) { task.cleanup(); task.reject(new ApiError(0, 'FileReadFailed')) }
      hashes.clear(); worker?.terminate(); worker = undefined
    }
  }
  return new Promise((resolve, reject) => {
    const id = ++sequence
    const abort = () => { hashes.delete(id); worker?.postMessage({ id, cancel: true }); reject(signal.reason) }
    signal.addEventListener('abort', abort, { once: true })
    hashes.set(id, { resolve, reject, cleanup: () => signal.removeEventListener('abort', abort) })
    worker!.postMessage({ id, blob })
  })
}
function valid(job: UploadJob) { job.controller.signal.throwIfAborted(); if (session.value?.id !== job.owner) throw new ApiError(401, 'Unauthorized') }
function json<T>(job: UploadJob, path: string, body: unknown): Promise<T> {
  valid(job)
  return api(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: job.controller.signal })
}
function send(job: UploadJob, part: number, blob: Blob, digest: string): Promise<Part> {
  valid(job)
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest(), started = performance.now()
    let loaded = 0
    const abort = () => xhr.abort()
    xhr.open('PUT', `/api/uploads/${job.transfer!.id}/parts/${part}`)
    xhr.timeout = 300_000
    xhr.setRequestHeader('X-CSRF-Token', session.value!.csrf_token)
    xhr.setRequestHeader('X-Content-SHA256', digest)
    xhr.setRequestHeader('Content-Type', 'application/octet-stream')
    xhr.upload.onprogress = event => {
      job.sending += event.loaded - loaded; loaded = event.loaded
      job.speed = loaded / Math.max(0.1, (performance.now() - started) / 1000)
    }
    xhr.onload = () => {
      let reply: any
      try { reply = JSON.parse(xhr.responseText) } catch { reject(new ApiError(xhr.status, 'RequestFailed')); return }
      if (xhr.status >= 200 && xhr.status < 300) resolve(reply)
      else reject(new ApiError(xhr.status, reply.code || 'RequestFailed', reply.request_id))
    }
    xhr.onerror = () => reject(new ApiError(0, 'Network'))
    xhr.ontimeout = () => reject(new ApiError(408, 'RequestTimeout'))
    xhr.onabort = () => reject(new DOMException('Aborted', 'AbortError'))
    xhr.onloadend = () => { job.sending -= loaded; job.speed = 0; job.controller.signal.removeEventListener('abort', abort) }
    job.controller.signal.addEventListener('abort', abort, { once: true })
    xhr.send(blob)
  })
}
async function receiptList(job: UploadJob) {
  const parts: Parts['parts'] = []
  let after = 0
  do {
    const page = await api<Parts>(`/api/uploads/${job.transfer!.id}/parts?after=${after}`, { signal: job.controller.signal })
    parts.push(...page.parts); after = page.next || 0
  } while (after)
  return parts
}
function refresh() { queries.invalidateQueries({ queryKey: ['transfers'] }); queries.invalidateQueries({ queryKey: ['objects'] }); queries.invalidateQueries({ queryKey: ['quota'] }) }

// One file at a time, with two bounded part workers shared by the whole page.
async function run(job: UploadJob) {
  job.running = true; job.error = undefined; job.controller = markRaw(new AbortController())
  try {
    valid(job)
    if (!job.transfer) job.transfer = await json<Transfer>(job, `/api/buckets/${job.bucket}/uploads`, job.input)
    const transfer = await api<Transfer>(`/api/uploads/${job.transfer.id}`, { signal: job.controller.signal })
    job.transfer = transfer
    if (transfer.state === 'completed') { job.state = 'completed'; job.file = undefined; return }
    if (transfer.state !== 'active') throw new ApiError(409, 'OperationAborted')
    const file = job.file!, size = Number(transfer.part_size), total = Math.max(1, Math.ceil(file.size / size))
    const received = await receiptList(job), receipts = new Map<number, Part>()
    job.accepted = 0; job.verified = 0; job.state = 'checking'
    for (const part of received) {
      if (job.paused) break
      valid(job)
      const blob = file.slice((part.number - 1) * size, part.number * size)
      if (blob.size !== Number(part.size) || !part.sha256 || await hash(blob, job.controller.signal) !== part.sha256) throw new ApiError(409, 'ResumeMismatch')
      receipts.set(part.number, { number: part.number, etag: part.etag, sha256: part.sha256 }); job.accepted += blob.size; job.verified++
    }
    if (job.paused) { job.state = 'paused'; return }
    job.state = 'uploading'
    let next = 1, failure: unknown
    async function partWorker() {
      while (!job.paused && !failure) {
        const number = next++; if (number > total) return; if (receipts.has(number)) continue
        try {
          valid(job)
          const blob = file.slice((number - 1) * size, number * size), digest = await hash(blob, job.controller.signal)
          if (job.paused) return
          for (let attempt = 0; ; attempt++) {
            try { receipts.set(number, await send(job, number, blob, digest)); break }
            catch (error) {
              if (!(error instanceof ApiError) || ![0, 408, 429, 503].includes(error.status) || attempt >= 2 || job.paused) throw error
              await new Promise(resolve => setTimeout(resolve, 1000 * 2 ** attempt)); valid(job)
            }
          }
          job.accepted += blob.size
        } catch (error) { failure = error }
      }
    }
    await Promise.all([partWorker(), partWorker()])
    if (failure) throw failure
    if (job.paused) { job.state = 'paused'; return }
    job.state = 'completing'
    job.transfer = await json<Transfer>(job, `/api/uploads/${job.transfer.id}/complete`, { parts: [...receipts.values()].sort((a, b) => a.number - b.number) })
    job.state = 'completed'; job.file = undefined
  } catch (error) {
    if (job.state !== 'aborted') { job.state = job.paused ? 'paused' : 'failed'; job.error = error }
  } finally { job.running = false; refresh(); pump() }
}
function pump() {
  if (uploads.some(job => job.running)) return
  const job = uploads.find(job => job.state === 'queued' && job.file && !job.paused)
  if (job) void run(job)
}
export function enqueue(files: File[], bucket: string, prefix: string, publicRead: boolean, overwrite: boolean) {
  if (!session.value) return
  for (const file of files) {
    const client = uuid()
    uploads.push({ client, owner: session.value.id, bucket, file: markRaw(file),
      input: { client_id: client, key: prefix + file.name, file_name: file.name, size: String(file.size), modified_at: file.lastModified, content_type: file.type, public_read: publicRead, overwrite },
      state: 'queued', accepted: 0, sending: 0, speed: 0, verified: 0, controller: markRaw(new AbortController()), running: false, paused: false })
  }
  pump()
}
export function pause(job: UploadJob) { job.paused = true; if (!job.running) job.state = 'paused' }
export function resume(job: UploadJob) { if (job.running || !job.file) return; job.paused = false; job.state = 'queued'; pump() }
export function reselect(transfer: Transfer, file: File) {
  if (file.size !== Number(transfer.expected_size) || file.name !== transfer.file_name) throw new ApiError(409, 'ResumeMismatch')
  let job = uploads.find(job => job.transfer?.id === transfer.id)
  if (job?.running) return
  if (!job) {
    enqueuePaused(transfer, file)
    job = uploads.at(-1)!
  } else { job.file = markRaw(file); job.transfer = transfer }
  resume(job)
}
function enqueuePaused(transfer: Transfer, file: File) {
  uploads.push({ client: uuid(), owner: session.value!.id, bucket: transfer.bucket_id, transfer, file: markRaw(file),
    input: { client_id: uuid(), key: transfer.object_key, file_name: file.name, size: String(file.size), content_type: file.type, public_read: false, overwrite: false },
    state: 'paused', accepted: Number(transfer.received_bytes), sending: 0, speed: 0, verified: 0, controller: markRaw(new AbortController()), running: false, paused: true })
}
export async function cancel(job?: UploadJob, transfer?: Transfer) {
  if (job) { job.paused = true; job.state = 'aborted'; job.controller.abort() }
  const id = transfer?.id || job?.transfer?.id
  try { if (id) await api(`/api/uploads/${id}`, { method: 'DELETE' }); if (job) job.file = undefined }
  catch (error) { if (job) { job.state = 'failed'; job.error = error }; throw error }
  finally { refresh(); pump() }
}
watch(() => session.value?.id, (owner, previous) => {
  if (owner === previous) return
  for (const job of uploads) { job.paused = true; job.state = 'aborted'; job.controller.abort(); job.file = undefined }
  uploads.splice(0); worker?.terminate(); worker = undefined
})
window.addEventListener('beforeunload', event => { if (uploads.some(job => job.running)) { event.preventDefault(); event.returnValue = '' } })
