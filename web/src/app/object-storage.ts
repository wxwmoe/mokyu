import { bytes } from '../api/client'
import type { components } from '../api/schema'
import { locale, t } from './i18n'

export type Extent = components['schemas']['ObjectChunk']

export function extentGroups(rows: Extent[]) {
  const groups: { pack?: string; start: number; end: number; members: Extent[] }[] = []
  for (const row of rows) {
    const start = Number(row.offset_bytes)
    const pack = row.source === 'pack' ? row.pack_id ?? undefined : undefined
    const previous = groups.at(-1)
    if (pack && previous?.pack === pack && previous.end === start) {
      previous.members.push(row)
      previous.end += row.length
    } else {
      groups.push({ pack, start, end: start + row.length, members: [row] })
    }
  }
  return groups
}

export function completePack(rows: Extent[]) {
  if (rows[0]?.pack_count !== rows.length) return false
  let next = 8
  for (const [ordinal, row] of rows.entries()) {
    if (row.pack_ordinal !== ordinal || row.source_offset !== 0 || row.length !== row.raw_size ||
      row.pack_offset !== next || !row.stored_size) return false
    next += row.stored_size
  }
  return next === rows[0].pack_size
}

export function extentRange(start: number, length: number) {
  const end = start + length
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  const unit = Math.min(units.length - 1, Math.floor(Math.log2(Math.max(1, end)) / 10))
  const scale = 1024 ** unit
  let digits = 1
  let format = new Intl.NumberFormat(locale.value, { maximumFractionDigits: digits })
  while (length > 0 && format.format(start / scale) === format.format(end / scale) && digits < 9) {
    format = new Intl.NumberFormat(locale.value, { maximumFractionDigits: ++digits })
  }
  const span = format.format(start / scale) === format.format(end / scale) && length > 0
    ? `${start}–${end} B`
    : `${format.format(start / scale)}–${format.format(end / scale)} ${units[unit]}`
  return t('extentRange', { span, length: bytes(length) })
}
