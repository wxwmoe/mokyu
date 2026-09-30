import type { components } from '../api/schema'
import { params } from '../api/client'

export type MediaItem = components['schemas']['MediaItem']
export function mediaUrl(bucket: string, item: MediaItem, action = 'content', preview = false) {
  return `/api/buckets/${bucket}/object/${action}?` + params({ key: item.object_key, version: item.id, ...(action === 'content' ? { preview: String(preview) } : {}) })
}
export function hasThumbnail(item: MediaItem) {
  return ['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(item.content_type.split(';')[0]!.trim().toLowerCase()) && BigInt(item.size) <= 32n * 1024n * 1024n
}

let thumbnailQueue: Promise<unknown> = Promise.resolve()
export function thumbnail(url: string, signal: AbortSignal): Promise<Blob> {
  const task = thumbnailQueue.catch(() => {}).then(async () => {
    for (let attempt = 0; ; attempt++) {
      signal.throwIfAborted()
      const response = await fetch(url, { signal, credentials: 'same-origin', cache: 'no-store' })
      if (response.status === 503 && attempt < 2) {
        await new Promise<void>((resolve, reject) => {
          const done = () => { signal.removeEventListener('abort', abort); resolve() }
          const timer = setTimeout(done, 500 * (attempt + 1))
          const abort = () => { clearTimeout(timer); reject(signal.reason) }
          signal.addEventListener('abort', abort, { once: true })
        })
        continue
      }
      if (!response.ok) throw new Error('ThumbnailUnavailable')
      return response.blob()
    }
  })
  thumbnailQueue = task
  return task
}
