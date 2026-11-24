<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { History } from 'lucide-vue-next'
import { t } from '../app/i18n'
import { api, bytes, write, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import ErrorNotice from './ErrorNotice.vue'
const props = defineProps<{ bucket: Bucket }>()
const base = computed(() => '/api/v1/buckets/' + props.bucket.id + '/versioning')
const mode = useQuery({ queryKey: ['versioning', base], queryFn: ({ signal }) => api<'off'|'enabled'|'suspended'>(base.value, { signal }) })
const busy = ref(false), error = ref<unknown>()
async function set() { busy.value = true; error.value = undefined; try { await write(base.value, mode.data.value === 'enabled' ? 'suspended' : 'enabled', 'PUT'); await mode.refetch() } catch (e) { error.value = e } finally { busy.value = false } }
const days = ref(30), keep = ref(3)
const filters = computed(() => [days.value, keep.value].every(v => Number.isSafeInteger(v) && v >= 0))
const preview = useQuery({
  queryKey: computed(() => ['version-retention', props.bucket.id, days.value, keep.value]),
  queryFn: ({ signal }) => api<components['schemas']['RetentionPreview']>(
    '/api/v1/buckets/' + props.bucket.id + '/versions/retention-preview?days=' + days.value + '&keep=' + keep.value, { signal }),
  enabled: false, retry: false,
})
const requested = ref(false)
watch([days, keep], () => { requested.value = false })
function estimate() {
  if (!filters.value) return
  requested.value = true
  void preview.refetch()
}
</script>
<template><section class="settings-section"><h3><History :size="18"/> {{ t('versionTitle') }}</h3><p class="field-help">{{ t('versionHint') }}</p><ErrorNotice v-if="mode.error.value || error" :error="mode.error.value || error"/><div v-if="mode.data.value" class="version-settings"><span class="badge">{{ t('versionMode_' + mode.data.value) }}</span><button :disabled="busy || bucket.status !== 'active'" @click="set">{{ t(mode.data.value === 'enabled' ? 'versionSuspend' : 'versionEnable') }}</button></div>
  <section v-if="mode.data.value && mode.data.value !== 'off' && bucket.actions.includes('object.versions.list')" class="retention">
    <h3>{{ t('retentionPreviewTitle') }}</h3>
    <form class="retention-form" @submit.prevent="estimate">
      <label class="field"><span>{{ t('retentionDays') }}</span><input v-model.number="days" type="number" min="0" step="1" required></label>
      <label class="field"><span>{{ t('retentionKeep') }}</span><input v-model.number="keep" type="number" min="0" step="1" required></label>
      <button :disabled="!filters || preview.isFetching.value">{{ t('retentionPreviewAction') }}</button>
    </form>
    <ErrorNotice v-if="requested && preview.error.value" :error="preview.error.value"/>
    <div v-else-if="requested && preview.data.value" role="status">
      <dl class="retention-totals">
        <div><dt>{{ t('retentionCandidates') }}</dt><dd>{{ preview.data.value.versions }}</dd></div>
        <div><dt>{{ t('retentionLogical') }}</dt><dd>{{ bytes(preview.data.value.logical_bytes) }}</dd></div>
        <div><dt>{{ t('retentionTime') }}</dt><dd>{{ bytes(preview.data.value.time_protected_bytes) }}</dd></div>
        <div><dt>{{ t('retentionShared') }}</dt><dd>{{ bytes(preview.data.value.shared_bytes) }}</dd></div>
      </dl>
      <p class="field-help">{{ t('retentionPhysicalHint') }}</p>
    </div>
  </section>
</section></template>
<style scoped>.version-settings{display:flex;align-items:center;gap:1rem;margin-top:1rem}h3{display:flex;align-items:center;gap:.6rem}
.retention{border-top:1px solid var(--line);margin-top:1.5rem;padding-top:1.25rem}
.retention-form{display:flex;align-items:end;gap:1rem;flex-wrap:wrap;margin:1rem 0}
.retention-form label{flex:1 1 12rem;margin:0}
.retention-totals{display:grid;grid-template-columns:repeat(auto-fit,minmax(9rem,1fr));gap:1rem}
.retention-totals dt{color:var(--muted);font-size:.85rem}.retention-totals dd{margin:.25rem 0 0;font-variant-numeric:tabular-nums}
</style>
