<script setup lang="ts">
import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { useRoute, useRouter } from 'vue-router'
import { api, session, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiSelect from './ui/UiSelect.vue'
const route = useRoute(), router = useRouter()
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const projects = useQuery({ queryKey: ['projects'], enabled: computed(() => !!session.value?.project_management), queryFn: ({ signal }) => api<components['schemas']['Project'][]>('/api/projects', { signal }) })
const options = computed(() => [{ value: 'all', label: t(session.value?.role === 'admin' ? 'wholeDeployment' : 'yourBuckets') },
  ...(session.value?.project_management ? (projects.data.value || []).map(p => ({ value: 'project:' + p.id, label: p.name, description: t('project') })) : []),
  ...(buckets.data.value || []).filter(b => b.actions.includes('storage.inspect')).map(b => ({ value: 'bucket:' + b.id, label: b.name, description: t('bucket') }))])
const selected = computed({ get: () => route.query.bucket ? 'bucket:' + route.query.bucket : route.query.project ? 'project:' + route.query.project : 'all',
  set: value => { const [kind, id] = value.split(':'); router.push({ query: { bucket: kind === 'bucket' ? id : undefined, project: kind === 'project' ? id : undefined } }) } })
</script>
<template><div class="scope-picker"><UiSelect v-model="selected" :options="options" :label="t('insightScope')" :searchable="options.length > 10" /></div></template>
