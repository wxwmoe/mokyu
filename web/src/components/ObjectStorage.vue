<script setup lang="ts">
import ErrorNotice from './ErrorNotice.vue'
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Layers3, Package, ChevronRight, CloudUpload } from 'lucide-vue-next'
import { api, params, bytes } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { errorText } from '../app/feedback'
const props = defineProps<{ bucket: string; item: components['schemas']['MediaItem'] }>()
const after = ref('')
const query = computed(() => params({ bucket: props.bucket, key: props.item.object_key, version: props.item.id, after: after.value || undefined }))
const chunks = useQuery({ queryKey: ['object-chunks', query], queryFn: ({ signal }) => api<components['schemas']['ChunkPage']>('/api/object/chunks?' + query.value, { signal }) })
</script>
<template><div class="object-storage"><p class="field-help">{{ t('objectStorageHint') }}</p><ErrorNotice v-if="chunks.error.value" :error="chunks.error.value" /><div v-for="chunk in chunks.data.value?.chunks" :key="chunk.offset_bytes" class="object-chunk"><Package v-if="chunk.source === 'pack'" :size="17" /><CloudUpload v-else-if="chunk.source === 'pending'" :size="17" /><Layers3 v-else :size="17" /><div><strong>{{ bytes(Number(chunk.offset_bytes)) }} + {{ bytes(chunk.length) }}</strong><span>{{ t('source_' + chunk.source) }} · {{ chunk.compression === 'zstd' ? 'Zstd' : t('uncompressed') }}</span></div><RouterLink v-if="chunk.source === 'pack' && chunk.pack_id" :to="{ path: '/storage', query: { bucket, pack: chunk.pack_id } }" :aria-label="t('packNumber', { id: chunk.pack_id })"><ChevronRight :size="17" /></RouterLink><details><summary>{{ t('details') }}</summary><dl class="facts"><dt>{{ t('chunkId') }}</dt><dd>{{ chunk.id }}</dd><dt>{{ t('sourceOffset') }}</dt><dd>{{ bytes(chunk.source_offset) }}</dd><dt>{{ t('rawContent') }}</dt><dd>{{ bytes(chunk.raw_size) }}</dd><dt>{{ t('encryption') }}</dt><dd>{{ chunk.algorithm }}</dd><template v-if="chunk.reads != null"><dt>{{ t('readCount') }}</dt><dd>{{ chunk.reads }}</dd><dt>{{ t('rangeReadCount') }}</dt><dd>{{ chunk.range_reads }}</dd></template></dl></details></div><div class="table-footer"><button v-if="after" @click="after = ''">{{ t('firstPage') }}</button><button v-if="chunks.data.value?.next_offset" @click="after = chunks.data.value.next_offset">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div></div></template>
