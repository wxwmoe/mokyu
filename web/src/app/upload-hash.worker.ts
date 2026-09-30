import { sha256 } from '@noble/hashes/sha2.js'

const readers = new Map<number, ReadableStreamDefaultReader<Uint8Array>>()
self.onmessage = async ({ data }: MessageEvent<{ id: number; blob?: Blob; cancel?: boolean }>) => {
  if (data.cancel) { await readers.get(data.id)?.cancel(); return }
  if (!data.blob) return
  const reader = data.blob.stream().getReader(), hash = sha256.create()
  readers.set(data.id, reader)
  try {
    for (;;) { const { value, done } = await reader.read(); if (done) break; hash.update(value) }
    self.postMessage({ id: data.id, hash: btoa(String.fromCharCode(...hash.digest())) })
  } catch { self.postMessage({ id: data.id, error: true }) }
  finally { readers.delete(data.id); reader.releaseLock(); hash.destroy() }
}
