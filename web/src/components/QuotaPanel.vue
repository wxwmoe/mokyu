<script setup lang="ts">
import ErrorNotice from './ErrorNotice.vue'
import { computed, reactive, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Gauge, SlidersHorizontal, Sprout, Clock3 } from 'lucide-vue-next'
import { api, bytes, parseBytes, session, write } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { secure } from '../app/security'
import { notify, report, errorText } from '../app/feedback'
import UiDialog from './ui/UiDialog.vue'
import UiSelect from './ui/UiSelect.vue'
type Quota = components['schemas']['Quota']
const props = defineProps<{ kind: 'project' | 'bucket'; id: string }>()
const path = computed(() => `/api/quotas/${props.kind}/${props.id}`)
const quota = useQuery({ queryKey: ['quota', path], queryFn: ({ signal }) => api<Quota>(path.value, { signal }), refetchInterval: 30000 })
const row = computed(() => quota.data.value)
const used = computed(() => row.value?.used_bytes == null ? null : Number(row.value.used_bytes))
const limit = computed(() => row.value?.byte_limit == null ? null : Number(row.value.byte_limit))
const reserved = computed(() => Number(row.value?.reserved_bytes || 0))
const percent = computed(() => limit.value == null ? 0 : limit.value === 0 ? used.value || reserved.value ? 100 : 0 : Math.min(100, ((used.value || 0) + reserved.value) / limit.value * 100))
const over = computed(() => used.value != null && limit.value != null && used.value + reserved.value > limit.value)
const open = ref(false), busy = ref(false), invalid = ref(false)
const draft = reactive({ bytes: '', byteUnit: '1073741824', inflight: '', inflightUnit: '1073741824', buckets: '' })
const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'].map((label, index) => ({ label, value: String(1024 ** index) }))
function input(value: string | null | undefined) {
  if (value == null) return ['', '1073741824'] as const
  const size = BigInt(value), unit = [...units].reverse().find(unit => size > 0n && size % BigInt(unit.value) === 0n) || units[0]!
  return [String(size / BigInt(unit.value)), unit.value] as const
}
function edit() {
  [draft.bytes, draft.byteUnit] = input(row.value?.byte_limit)
  ;[draft.inflight, draft.inflightUnit] = input(row.value?.inflight_limit)
  draft.buckets = row.value?.bucket_limit || ''; invalid.value = false; open.value = true
}
async function save() {
  let limits
  try { limits = { byte_limit: parseBytes(draft.bytes, draft.byteUnit), inflight_limit: props.kind === 'project' ? parseBytes(draft.inflight, draft.inflightUnit) : null, bucket_limit: props.kind === 'project' ? parseBytes(draft.buckets) : null } }
  catch { invalid.value = true; return }
  busy.value = true
  try { await secure(() => write(path.value, limits, 'PUT')); open.value = false; await quota.refetch(); notify(t('quotaSaved')) }
  catch (error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <section class="quota-card" :class="{ exceeded: over }">
    <div class="section-heading"><h3><Sprout :size="18" />{{ t('quotaTitle') }}</h3><button v-if="session?.role === 'admin' && row" class="text-button" @click="edit"><SlidersHorizontal :size="15" />{{ t('quotaEdit') }}</button></div>
    <ErrorNotice v-if="quota.error.value" :error="quota.error.value" />
    <template v-if="row"><div v-if="used != null" class="quota-reading"><strong>{{ bytes(used) }}</strong><span>/ {{ limit == null ? t('quotaUnlimited') : bytes(limit) }}</span><span v-if="over" class="badge">{{ t('quotaExceeded') }}</span></div>
      <p v-else class="field-help">{{ t('quotaScoped') }}</p>
      <div v-if="used != null && limit != null" class="quota-meter" role="progressbar" :aria-label="t('quotaTitle')" :aria-valuenow="Math.round(percent)" aria-valuemin="0" aria-valuemax="100"><span :style="{ width: percent + '%' }" /></div>
      <div v-if="used != null" class="quota-details"><span v-if="reserved"><Clock3 :size="13" />{{ t('quotaReserved', { value: bytes(reserved) }) }}</span><span v-if="Number(row.inflight_bytes)">{{ t('quotaInFlight', { value: bytes(Number(row.inflight_bytes)) }) }}</span><span v-if="kind === 'project' && row.bucket_limit != null">{{ t('quotaBuckets', { used: row.bucket_count || '0', limit: row.bucket_limit }) }}</span></div>
      <p class="field-help">{{ t(over ? 'quotaOverHint' : 'quotaHint') }}</p>
    </template>
  </section>
  <UiDialog v-model:open="open" :title="t('quotaEdit')" :description="t('quotaEditHint')" :busy="busy"><form @submit.prevent="save">
    <label>{{ t('quotaLogicalLimit') }}<span class="quota-input"><input v-model="draft.bytes" inputmode="decimal" :placeholder="t('quotaUnlimited')" :disabled="busy"><UiSelect v-model="draft.byteUnit" :options="units" :label="t('quotaUnit')" :disabled="busy" /></span></label>
    <template v-if="kind === 'project'"><label>{{ t('quotaInflightLimit') }}<span class="quota-input"><input v-model="draft.inflight" inputmode="decimal" :placeholder="t('quotaUnlimited')" :disabled="busy"><UiSelect v-model="draft.inflightUnit" :options="units" :label="t('quotaUnit')" :disabled="busy" /></span></label><p class="field-help">{{ t('quotaInflightHint') }}</p><label>{{ t('quotaBucketLimit') }}<input v-model="draft.buckets" inputmode="numeric" :placeholder="t('quotaUnlimited')" :disabled="busy"></label></template>
    <div class="info-callout"><Gauge :size="20" /><p>{{ t('quotaZeroHint') }}</p></div><p v-if="invalid" class="error" role="alert">{{ t('quotaInvalid') }}</p>
    <div class="form-footer"><button type="button" :disabled="busy" @click="open = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy">{{ t('save') }}</button></div>
  </form></UiDialog>
</template>
<style scoped>
.quota-card{background:var(--surface);border:1px solid var(--line);border-radius:20px;padding:18px 22px;margin:18px 0}.quota-card h3{display:flex;align-items:center;gap:8px;margin:0}.quota-card h3 svg{color:#66978a}.quota-reading{display:flex;align-items:baseline;flex-wrap:wrap;gap:8px;margin-top:15px}.quota-reading strong{font-size:24px;font-weight:850}.quota-reading>span{color:var(--muted)}.quota-meter{height:7px;background:var(--line);border-radius:9px;margin-top:12px;overflow:hidden}.quota-meter>span{display:block;height:100%;border-radius:9px;background:linear-gradient(90deg,#97c5b6,#c2d0ab)}.exceeded .quota-meter>span{background:#d997a4}.quota-details{display:flex;flex-wrap:wrap;gap:14px;margin-top:12px;font-size:var(--small-size,12px);color:var(--muted)}.quota-details>span{display:flex;align-items:center;gap:5px}.quota-input{display:grid;grid-template-columns:minmax(0,1fr) 105px;gap:8px}.quota-card .field-help{margin-bottom:0}
</style>
