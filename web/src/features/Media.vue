<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { api, bytes, params, type Bucket } from '../api/client'

interface MediaObject { id: string; object_key: string; size: number; public_read: boolean; created_at: string }
interface ObjectPage { objects: MediaObject[]; prefixes: string[]; next_token: string | null }
const route = useRoute(), router = useRouter()
const bucket = computed(() => String(route.params.bucket || ''))
const prefix = computed(() => String(route.query.prefix || ''))
const cursor = computed(() => String(route.query.cursor || ''))
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const current = computed(() => buckets.data.value?.find(item => item.id === bucket.value))
const objects = useQuery({ queryKey: ['objects', bucket, prefix, cursor], enabled: computed(() => !!bucket.value),
  queryFn: ({ signal }) => api<ObjectPage>('/api/objects?' + params({ bucket: bucket.value, prefix: prefix.value, token: cursor.value || undefined }), { signal }) })
const selected = ref<MediaObject | null>(null)
const download = computed(() => '/api/download?' + params({ bucket: bucket.value, key: selected.value?.object_key }))
function folder(value: string) { selected.value = null; router.push({ query: { prefix: value || undefined } }) }
</script>

<template>
  <div class="page-heading"><div><p class="eyebrow">Your little collection</p><h1>{{ current?.name || 'Media library' }}</h1><p>Everything in its own lovely place.</p></div><button @click="bucket ? objects.refetch() : buckets.refetch()">Refresh</button></div>
  <p v-if="buckets.error.value || objects.error.value" class="error" role="alert">{{ buckets.error.value?.message || objects.error.value?.message }}</p>
  <div v-if="!bucket" class="bucket-grid">
    <RouterLink v-for="item in buckets.data.value" :key="item.id" class="bucket-card" :to="`/media/${item.id}`"><img src="/assets/mokyu-pack.svg" alt="" width="64" height="64"><h2>{{ item.name }}</h2><span>{{ item.state }}</span></RouterLink>
    <div v-if="!buckets.isPending.value && !buckets.data.value?.length" class="empty-state"><img src="/assets/mokyu-mochi.svg" alt=""><h2>A fresh little space</h2><p>Create a bucket to start your collection.</p></div>
  </div>
  <section v-else class="surface">
    <nav class="breadcrumbs" aria-label="Folder path"><RouterLink to="/media">Media library</RouterLink><span>/</span><button @click="folder('')">{{ current?.name }}</button><span v-if="prefix">/ {{ prefix }}</span></nav>
    <p v-if="objects.isPending.value" role="status">Opening your collection…</p>
    <div class="table-scroll"><table><thead><tr><th>Name</th><th>Size</th><th>Access</th><th>Updated</th></tr></thead><tbody>
      <tr v-for="name in objects.data.value?.prefixes" :key="name"><td colspan="4"><button class="text-button" @click="folder(name)">▱ {{ name.slice(prefix.length) }}</button></td></tr>
      <tr v-for="item in objects.data.value?.objects" :key="item.id"><td><button class="text-button" @click="selected = item">{{ item.object_key.slice(prefix.length) }}</button></td><td>{{ bytes(item.size) }}</td><td><span class="badge">{{ item.public_read ? 'Public' : 'Private' }}</span></td><td>{{ new Date(item.created_at).toLocaleDateString() }}</td></tr>
    </tbody></table></div>
    <p v-if="objects.data.value && !objects.data.value.objects.length && !objects.data.value.prefixes.length" class="empty-state">There is room for something lovely here.</p>
    <button v-if="objects.data.value?.next_token" @click="router.push({ query: { prefix, cursor: objects.data.value.next_token } })">Next page</button>
  </section>
  <section v-if="selected" class="surface object-summary"><h2>{{ selected.object_key }}</h2><p>{{ bytes(selected.size) }}</p><div class="actions"><a class="button primary" :href="download">Download original</a><button @click="selected = null">Close</button></div></section>
</template>
