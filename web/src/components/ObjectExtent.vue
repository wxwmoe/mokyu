<script setup lang="ts">
import { CloudUpload, File, Layers3 } from 'lucide-vue-next'
import { bytes } from '../api/client'
import { locale, t } from '../app/i18n'
import { extentRange, type Extent } from '../app/object-storage'

defineProps<{ row: Extent }>()
</script>

<template>
  <div class="object-chunk">
    <CloudUpload v-if="row.source === 'pending'" :size="17" />
    <File v-else-if="row.source === 'raw'" :size="17" />
    <Layers3 v-else :size="17" />
    <div>
      <strong>{{ extentRange(Number(row.offset_bytes), row.length) }}</strong>
      <span>{{ t('source_' + (row.source === 'pack' ? 'chunk' : row.source)) }}<template v-if="row.source !== 'raw'"> · {{ row.compression === 'zstd' ? 'Zstd' : t('uncompressed') }}</template></span>
    </div>
    <details>
      <summary>{{ t('details') }}</summary>
      <dl class="facts">
        <dt>{{ t(row.source === 'raw' ? 'objectId' : 'chunkId') }}</dt><dd>{{ row.id }}</dd>
        <dt>{{ t('sourceOffset') }}</dt><dd>{{ bytes(row.source_offset) }}</dd>
        <dt>{{ t('rawContent') }}</dt><dd>{{ bytes(row.raw_size) }}</dd>
        <template v-if="row.source === 'pack'">
          <dt>{{ t('memberPack') }}</dt><dd>{{ row.pack_id }}</dd>
          <dt>{{ t('packOffset') }}</dt><dd>{{ row.pack_offset?.toLocaleString(locale) }} B</dd>
          <dt>{{ t('encodedLength') }}</dt><dd>{{ row.stored_size?.toLocaleString(locale) }} B</dd>
        </template>
        <template v-if="row.source !== 'raw'"><dt>{{ t('encryption') }}</dt><dd>{{ row.algorithm }}</dd></template>
        <template v-if="row.reads != null">
          <dt>{{ t('readCount') }}</dt><dd>{{ row.reads }}</dd>
          <dt>{{ t('rangeReadCount') }}</dt><dd>{{ row.range_reads }}</dd>
        </template>
      </dl>
    </details>
  </div>
</template>
