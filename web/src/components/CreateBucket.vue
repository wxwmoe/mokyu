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
const options = useQuery({ queryKey: ['bucket-projects'], queryFn: ({ signal }) => api<components['schemas']['BucketCreationOptions']>('/api/v1/bucket-projects', { signal }) })
const choices = computed(() => options.data.value?.projects.map(p => ({ value: p.id, label: p.name })) || [])
const storages = computed(() => options.data.value?.storages.map(storage => ({ value: storage.id, label: storage.name, description: storage.type })) || [])
const open = ref(false), busy = ref(false), name = ref(''), project = ref(''), storage = ref('')
watch(open, value => { if (value) { name.value = ''; project.value = choices.value[0]?.value || ''; storage.value = storages.value.length === 1 ? storages.value[0]!.value : '' } })
async function create() {
  busy.value = true
  try { const bucket = await write<Bucket>('/api/v1/buckets', { name: name.value, project_id: project.value, storage_id: storage.value || undefined }); await queries.invalidateQueries({ queryKey: ['buckets'] }); await queries.invalidateQueries({ queryKey: ['quota'] }); open.value = false; notify(t('bucketCreated')); emit('created', bucket) }
  catch (error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <button v-if="choices.length && storages.length" @click="open = true"><Plus :size="17" />{{ t('createBucket') }}</button>
  <UiDialog v-model:open="open" :busy="busy" :title="t('createBucket')" :description="t('createBucketHint')">
    <form id="create-bucket" @submit.prevent="create"><span class="project-symbol"><FolderHeart :size="27" /></span><label class="field"><span>{{ t('bucketName') }}</span><input v-model="name" required minlength="3" maxlength="63" pattern="[a-z0-9][a-z0-9.\-]*[a-z0-9]" autocomplete="off" :disabled="busy"></label><p class="field-help">{{ t('bucketNameHint') }}</p>
      <label v-if="session?.project_management || choices.length > 1" class="field"><span>{{ t('project') }}</span><UiSelect v-model="project" :options="choices" :label="t('project')" :disabled="busy" /></label>
      <label v-if="storages.length > 1" class="field"><span>{{ t('storageTarget') }}</span><UiSelect v-model="storage" :options="storages" :label="t('storageTarget')" :disabled="busy" /></label><p v-if="storages.length > 1" class="field-help">{{ t('storageTargetHint') }}</p>
    </form><template #footer><button type="submit" form="create-bucket" class="primary" :disabled="busy || !project">{{ t(busy ? 'saving' : 'createBucket') }}</button></template>
  </UiDialog>
</template>
