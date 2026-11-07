<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { api, bytes, queries, write } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { report } from '../app/feedback'
import UiCheckbox from './ui/UiCheckbox.vue'
import UiSelect from './ui/UiSelect.vue'

type Probe = components['schemas']['StorageRecoveryProbeResult']
type Job = components['schemas']['MetadataRecovery']
const props = defineProps<{ probe: Probe; connection: object; stores: components['schemas']['StorageView'][] }>()
const emit = defineEmits<{ back: []; close: [] }>()
const phase = ref(props.probe.state), secret = ref(''), busy = ref(false)
const job = ref<Job | null>(props.probe.recovery ?? null)
const names = reactive<Record<string,string>>({}), selected = reactive<Record<string,boolean>>({})
const projects = useQuery({ queryKey: ['projects'], queryFn: ({ signal }) => api<components['schemas']['Project'][]>('/api/v1/projects', { signal }) })
const available = computed(() => props.stores.filter(s => s.enabled && (s.type === 's3' || s.type === 'local') && s.id !== props.probe.storage))
const destination = ref(available.value[0]?.id ?? 'source'), project = ref('00000000-0000-0000-0000-000000000001')
const exclusive = ref(false), complete = ref(false)
const working = computed(() => !!job.value && ['queued','claiming','verifying','importing'].includes(job.value.state))
const states: Record<string,string> = { queued:'recoveryQueued', claiming:'recoveryClaiming', verifying:'recoveryVerifying', importing:'recoveryImporting', completed:'recoveryDone', failed:'recoveryFailed', cancelled:'recoveryCancelled' }
const current = useQuery({ queryKey: computed(() => ['metadata-recovery', job.value?.id]), enabled: working, refetchInterval: 1500, queryFn: ({ signal }) => api<Job>(`/api/v1/storage-recoveries/${job.value!.id}`, { signal }) })
watch(() => current.data.value, update => { if (update) { job.value = update; if (update.state === 'completed') { void queries.invalidateQueries({queryKey:['storages']}); void queries.invalidateQueries({queryKey:['buckets']}) } } })
watch(() => job.value?.id, () => { for (const b of job.value?.progress.buckets ?? []) { selected[b.id] = false; names[b.id] = b.name } }, { immediate: true })
const buckets = computed(() => Object.fromEntries(Object.entries(names).filter(([id]) => selected[id])))
async function unlock() {
  busy.value = true
  const key = secret.value
  secret.value = ''
  try {
    const found = await write<Probe>('/api/v1/storage-recoveries/probe', {storage:props.connection, deployment_key:key})
    phase.value = found.state; job.value = found.recovery ?? null
  } catch(e) { report(e) } finally { busy.value = false }
}
async function act(action: 'start'|'cancel'|'retry') {
  if (!job.value) return
  busy.value = true
  try {
    const take = destination.value === 'source'
    const body = action === 'start' ? { revision:job.value.revision, selection:{ project:project.value, buckets:buckets.value, destination:take?null:destination.value, exclusive_takeover:take && exclusive.value, complete_journal_upload:take && complete.value } } : {revision:job.value.revision,action}
    job.value = await write<Job>(`/api/v1/storage-recoveries/${job.value.id}/${action === 'start' ? 'start' : 'action'}`,body)
  } catch(e) { report(e) } finally { busy.value = false }
}
</script>

<template>
  <form v-if="phase === 'locked'" @submit.prevent="unlock">
    <label class="field"><span>{{ t('recoverySourceKey') }}</span><input v-model="secret" type="password" autocomplete="off" required></label>
    <div class="form-footer"><button type="button" @click="emit('back')">{{ t('recoveryBack') }}</button><button class="primary" :disabled="busy">{{ t('recoveryUnlock') }}</button></div>
  </form>
  <form v-else-if="job?.state === 'preview'" @submit.prevent="act('start')">
    <div class="store-form-grid">
      <label class="field"><span>{{ t('project') }}</span><UiSelect v-model="project" :label="t('project')" :options="(projects.data.value || []).map(p => ({value:p.id,label:p.name}))" /></label>
      <label class="field"><span>{{ t('recoveryTarget') }}</span><UiSelect v-model="destination" :label="t('recoveryTarget')" :options="[...available.map(s => ({value:s.id,label:s.name})),{value:'source',label:t('recoveryInPlace')}]" /></label>
    </div>
    <div v-for="b in job.progress.buckets" :key="b.id" class="recovery-bucket">
      <div><UiCheckbox v-model="selected[b.id]" :label="b.name" /><small>{{ t('recoveryCounts',{ count:b.objects, size:bytes(Number(b.logical_bytes)) }) }}<template v-if="b.stopped"> · {{ t('metadataJournalStopped') }}</template></small></div>
      <label v-if="selected[b.id]" class="field"><span>{{ t('recoveryBucketName') }}</span><input v-model="names[b.id]" required maxlength="63"></label>
    </div>
    <p v-if="!job.progress.buckets.length" class="field-help">{{ t('recoveryNoBuckets') }}</p>
    <p v-if="job.progress.conflicts.length" role="status">{{ t('recoveryConflicts',{ names:job.progress.conflicts.join(', ') }) }}</p>
    <div v-if="destination === 'source'" class="recovery-consent">
      <UiCheckbox v-model="exclusive" :label="t('recoveryExclusive')" />
      <UiCheckbox v-if="job.progress.pending_uploads" v-model="complete" :label="t('recoveryCompleteUpload')" />
    </div>
    <div class="form-footer"><button type="button" @click="emit('back')">{{ t('recoveryBack') }}</button><button class="primary" :disabled="busy || !Object.keys(buckets).length || (destination === 'source' && !exclusive)">{{ t('recoverBuckets') }}</button></div>
  </form>
  <div v-else>
    <p role="status">{{ job ? t(states[job.state] || 'recoveryImporting') : t(phase === 'registered' ? 'recoveryRegistered' : 'recoveryNoJournal') }}</p>
    <p v-if="job?.progress.recovered_buckets.length">{{ job.progress.recovered_buckets.join(', ') }}</p><p v-if="job?.error" role="alert">{{ job.error }}</p>
    <div class="form-footer"><button v-if="job?.state === 'failed'" @click="act('retry')">{{ t('recoveryRetry') }}</button><button v-if="job && ['queued','verifying','importing'].includes(job.state)" @click="act('cancel')">{{ t('cancel') }}</button><button @click="emit('close')">{{ t('close') }}</button></div>
  </div>
</template>

<style scoped>
.recovery-bucket { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,15rem),1fr)); gap:.75rem; align-items:center; padding:.85rem 0; border-bottom:1px solid var(--border); }
.recovery-bucket small { display:block; margin:.25rem 0 0 1.65rem; color:var(--muted); }
.recovery-consent { display:grid; gap:.7rem; margin:1rem 0; }
</style>
