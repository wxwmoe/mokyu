<script setup lang="ts">
import ErrorNotice from './ErrorNotice.vue'
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { TabsRoot, TabsList, TabsTrigger, TabsContent } from 'reka-ui'
import { Layers3, Images, ArrowUpRight, ChevronRight } from 'lucide-vue-next'
import { api, params, bytes, session } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { errorText } from '../app/feedback'
import UiDialog from './ui/UiDialog.vue'
import FileIcon from './FileIcon.vue'
import MaintenanceConfirm from './MaintenanceConfirm.vue'
const confirm = ref(false)
const props = defineProps<{ id: string; bucket?: string; project?: string }>()
const open = defineModel<boolean>('open', { required: true })
const after = ref(''), cursor = ref(''), tab = ref('members')
watch(() => props.id, () => { after.value = ''; cursor.value = ''; tab.value = 'members' })
const query = computed(() => params({ bucket: props.bucket, project: props.project, after: after.value || undefined }))
const detail = useQuery({ queryKey: ['pack', () => props.id, query], enabled: computed(() => open.value && !!props.id), queryFn: ({ signal }) => api<components['schemas']['PackDetail']>(`/api/storage/packs/${props.id}?${query.value}`, { signal }) })
const objectQuery = computed(() => params({ bucket: props.bucket, project: props.project, cursor: cursor.value || undefined }))
const objects = useQuery({ queryKey: ['pack-objects', () => props.id, objectQuery], enabled: computed(() => open.value && !!props.id && tab.value === 'objects'), queryFn: ({ signal }) => api<components['schemas']['PackObjects']>(`/api/storage/packs/${props.id}/objects?${objectQuery.value}`, { signal }) })
</script>
<template><UiDialog v-model:open="open" :title="t('packNumber', { id })" :description="t('packDetailHint')" drawer>
  <ErrorNotice v-if="detail.error.value" :error="detail.error.value" /><template v-if="detail.data.value"><div class="pack-detail-intro"><img src="/assets/mokyu-pack.svg" alt="" width="90" height="90"><div><span class="badge">{{ t(detail.data.value.pack.state) }}</span><h3>{{ t(detail.data.value.pack.compressed ? 'zstdPacked' : 'uncompressedPack') }}</h3><span class="field-help">{{ date(detail.data.value.pack.created_at) }}</span></div></div><dl v-if="!detail.data.value.scoped" class="facts"><dt>{{ t('rawContent') }}</dt><dd>{{ bytes(Number(detail.data.value.pack.raw_size)) }}</dd><dt>{{ t('encodedSize') }}</dt><dd>{{ detail.data.value.pack.stored_size == null ? t('pendingMeasure') : bytes(Number(detail.data.value.pack.stored_size)) }}</dd><dt>{{ t('chunks') }}</dt><dd>{{ detail.data.value.pack.member_count }}</dd></dl><p v-else class="info-callout">{{ t('packScopedHint') }}</p>
    <TabsRoot v-model="tab"><TabsList class="tabs-list" :aria-label="t('packDetails')"><TabsTrigger value="members" class="tab-trigger"><Layers3 :size="15" />{{ t('chunks') }}</TabsTrigger><TabsTrigger value="objects" class="tab-trigger"><Images :size="15" />{{ t('referencingFiles') }}</TabsTrigger></TabsList><TabsContent value="members"><div class="pack-member-list"><div v-for="(member, index) in detail.data.value.members" :key="member.chunk_id" class="pack-member"><span class="chunk-bead" :class="['rose', 'lilac', 'sky', 'mint'][index % 4]"><Layers3 :size="16" /></span><div><strong>{{ t('chunkNumber', { id: member.chunk_id }) }}</strong><small>{{ t(member.current_source ? 'primaryPackSource' : 'alternativeSource') }}</small></div><span>{{ bytes(Number(member.visible_bytes)) }}<small>{{ t('visibleContent') }}</small></span></div></div><div class="table-footer"><button v-if="after" @click="after = ''">{{ t('firstPage') }}</button><button v-if="detail.data.value.next" @click="after = detail.data.value.next!">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div></TabsContent>
      <TabsContent value="objects"><p class="field-help">{{ t('packObjectsHint') }}</p><ErrorNotice v-if="objects.error.value" :error="objects.error.value" /><div class="pack-file-links"><RouterLink v-for="object in objects.data.value?.objects" :key="object.bucket_id + object.key" :to="{ path: '/media/' + object.bucket_id, query: { object: object.key, version: object.version } }" @click="open = false"><FileIcon :name="object.key" /><span><strong>{{ object.key }}</strong><small>{{ object.bucket_name }} · {{ bytes(Number(object.size)) }}</small></span><ArrowUpRight :size="15" /></RouterLink></div><p v-if="objects.data.value && !objects.data.value.objects.length" class="field-help">{{ t('noVisibleReferences') }}</p><div class="table-footer"><button v-if="cursor" @click="cursor = ''">{{ t('firstPage') }}</button><button v-if="objects.data.value?.next" @click="cursor = objects.data.value.next">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div></TabsContent></TabsRoot>
  </template>
  <template v-if="session?.role === 'admin' && detail.data.value?.pack.state === 'ready'" #footer><button @click="confirm = true">{{ t('unpackOne') }}</button></template>
</UiDialog><MaintenanceConfirm v-model:open="confirm" operation="unpack" :input="{ pack_id: id }" /></template>
