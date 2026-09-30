<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Plus, FolderHeart } from 'lucide-vue-next'
import { api, write, queries, session, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { notify, report } from '../app/feedback'
import UiDialog from './ui/UiDialog.vue'
import UiSelect from './ui/UiSelect.vue'

const emit = defineEmits<{ created: [bucket: Bucket] }>()
const options = useQuery({ queryKey: ['bucket-projects'], queryFn: ({ signal }) => api<components['schemas']['BucketProject'][]>('/api/bucket-projects', { signal }) })
const choices = computed(() => options.data.value?.map(p => ({ value: p.id, label: p.name })) || [])
const open = ref(false), busy = ref(false), name = ref(''), project = ref('')
watch(open, value => { if (value) { name.value = ''; project.value = choices.value[0]?.value || '' } })
async function create() {
  busy.value = true
  try { const bucket = await write<Bucket>('/api/buckets', { name: name.value, project_id: project.value }); await queries.invalidateQueries({ queryKey: ['buckets'] }); await queries.invalidateQueries({ queryKey: ['quota'] }); open.value = false; notify(t('bucketCreated')); emit('created', bucket) }
  catch (error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <button v-if="choices.length" @click="open = true"><Plus :size="17" />{{ t('createBucket') }}</button>
  <UiDialog v-model:open="open" :busy="busy" :title="t('createBucket')" :description="t('createBucketHint')">
    <form id="create-bucket" @submit.prevent="create"><span class="project-symbol"><FolderHeart :size="27" /></span><label class="field"><span>{{ t('bucketName') }}</span><input v-model="name" required minlength="3" maxlength="63" pattern="[a-z0-9][a-z0-9.\-]*[a-z0-9]" autocomplete="off" :disabled="busy"></label><p class="field-help">{{ t('bucketNameHint') }}</p>
      <label v-if="session?.project_management || choices.length > 1" class="field"><span>{{ t('project') }}</span><UiSelect v-model="project" :options="choices" :label="t('project')" :disabled="busy" /></label>
    </form><template #footer><button type="submit" form="create-bucket" class="primary" :disabled="busy || !project">{{ t(busy ? 'saving' : 'createBucket') }}</button></template>
  </UiDialog>
</template>
