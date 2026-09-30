<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import { RefreshCw, ChevronRight, Search, ArrowUpRight } from 'lucide-vue-next'
import { api, params, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { errorText } from '../app/feedback'
import { kinds, jobIcon } from '../app/maintenance'
import { taskTone } from '../app/insights'
import UiSelect from '../components/ui/UiSelect.vue'
import TaskDetails from '../components/TaskDetails.vue'
import EmptyState from '../components/EmptyState.vue'
const route = useRoute(), router = useRouter(), state = ref(''), kind = ref(''), bucket = ref(''), actor = ref(''), actorDraft = ref(''), cursor = ref('')
const query = computed(() => params({ state: state.value || undefined, kind: kind.value || undefined, bucket: bucket.value || undefined, actor: actor.value || undefined, token: cursor.value || undefined }))
const tasks = useQuery({ queryKey: ['jobs', query], queryFn: ({ signal }) => api<components['schemas']['JobPage']>('/api/tasks?' + query.value, { signal }), refetchInterval: 10_000 })
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const states = computed(() => [{ value: '', label: t('allTaskStates') }, ...['queued', 'running', 'paused', 'completed', 'failed'].map(value => ({ value, label: t(value) }))])
const types = computed(() => [{ value: '', label: t('allTaskKinds') }, ...kinds.map(value => ({ value, label: t('task_' + value) }))])
const homes = computed(() => [{ value: '', label: t('allBuckets') }, ...(buckets.data.value || []).map(b => ({ value: b.id, label: b.name }))])
watch([state, kind, bucket, actor], () => { cursor.value = '' })
const open = computed({ get: () => !!route.query.task, set: value => { if (!value) router.replace({ query: { ...route.query, task: undefined } }) } })
</script>
<template><div class="page-heading"><div><p class="eyebrow">{{ t('tasksEyebrow') }}</p><h1>{{ t('tasks') }}</h1><p>{{ t('tasksHint') }}</p></div><div class="heading-actions"><RouterLink to="/maintenance" class="button">{{ t('maintenance') }}<ArrowUpRight :size="16" /></RouterLink><button class="icon-button" :aria-label="t('refresh')" @click="tasks.refetch()"><RefreshCw :size="17" /></button></div></div><section class="surface task-board"><div class="task-filters"><UiSelect v-model="state" :options="states" :label="t('taskState')" /><UiSelect v-model="kind" :options="types" :label="t('taskType')" /><UiSelect v-model="bucket" :options="homes" :label="t('buckets')" /><form @submit.prevent="actor = actorDraft"><input v-model="actorDraft" :aria-label="t('taskActor')" :placeholder="t('taskActor')" maxlength="128"><button class="icon-button" :aria-label="t('search')"><Search :size="17" /></button></form></div><ErrorNotice v-if="tasks.error.value" :error="tasks.error.value" /><div v-if="tasks.isPending.value" class="skeleton-row" /><div class="task-list"><button v-for="job in tasks.data.value?.tasks" :key="job.id" class="task-row" @click="router.push({ query: { task: job.id } })"><span class="policy-icon" :class="taskTone(job.policy)"><component :is="jobIcon(job.policy)" :size="22" /></span><span class="task-name"><strong>{{ t('task_' + job.policy) }}</strong><small>{{ job.bucket_name || job.created_by }} · {{ date(job.created_at) }}</small></span><span class="task-progress"><strong>{{ job.processed }}</strong><small>{{ t('itemsProcessed') }}</small></span><span class="badge" :class="{ 'error-badge': job.state === 'failed' }">{{ t(job.state) }}</span><ChevronRight :size="16" /></button></div><EmptyState v-if="tasks.data.value && !tasks.data.value.tasks.length" :title="t('noTasks')" :description="t('noTasksHint')" /><div class="table-footer"><button v-if="cursor" @click="cursor = ''">{{ t('firstPage') }}</button><button v-if="tasks.data.value?.next_token" @click="cursor = tasks.data.value.next_token">{{ t('nextPage') }}<ChevronRight :size="16" /></button></div></section><TaskDetails v-model:open="open" :id="String(route.query.task || '')" /></template>
