<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useQueryClient } from '@tanstack/vue-query'
import { api, write, ApiError } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { secure } from '../app/security'
import { report } from '../app/feedback'
import { useDeployment } from '../app/cluster'
import UiDialog from './ui/UiDialog.vue'
import UiSelect from './ui/UiSelect.vue'
import KeyCapacity from './KeyCapacity.vue'

type Key = components['schemas']['ManagedKeyView']
type Preview = components['schemas']['KeyReplacementPreview']
type Result = components['schemas']['KeyReplacementResult']
type Applied = components['schemas']['KeyReplacementApplied']
const props = defineProps<{ source: Key | null; keys: Key[] }>()
const open = defineModel<boolean>('open', { required: true })
const route = useRoute(), router = useRouter(), queries = useQueryClient(), { cluster } = useDeployment()
const target = ref('new'), name = ref(''), busy = ref(false), preview = ref<Preview | null>(null), result = ref<Result | null>(null), applied = ref<Applied | null>(null)
const options = computed(() => [{ value: 'new', label: t('replaceCreate') }, ...props.keys.filter(key => key.purpose === 'block' && !key.revoked_at && key.id !== props.source?.id).map(key => ({ value: key.id, label: `${key.name} · ${key.algorithm}` }))])
const selected = computed(() => props.keys.find(key => key.id === target.value))
watch([open, () => props.source?.id], async ([value]) => {
  if (!value) return
  target.value = 'new'; name.value = ''; preview.value = null; result.value = null; applied.value = null
  const saved = route.query.replacement
  if (typeof saved !== 'string') return
  busy.value = true
  try {
    const status = await api<components['schemas']['KeyReplacementStatus']>('/api/v1/key-replacements/' + encodeURIComponent(saved))
    if (status.preview.source !== props.source?.id) throw new Error(t('replaceScopeChanged'))
    preview.value = status.preview; result.value = status.result || null
  } catch (error) { report(error) } finally { busy.value = false }
})
async function prepare() {
  if (!props.source) return
  busy.value = true
  try {
    preview.value = await secure(() => write<Preview>(`/api/v1/keys/${props.source!.id}/replace-preview`, target.value === 'new' ? { name: name.value.trim() || null, target: null } : { target: target.value, name: null }))
    await router.replace({ query: { ...route.query, replace_key: props.source.id, replacement: preview.value.id } })
  } catch (error) { report(error) } finally { busy.value = false }
}
async function apply() {
  if (!preview.value) return
  busy.value = true
  try {
    applied.value = await secure(() => write<Applied>('/api/v1/key-replacements/' + preview.value!.id, {}))
    result.value = applied.value.result
    await queries.invalidateQueries({ predicate: query => ['managed-keys', 'storages', 'storage-profile', 'service-status'].includes(String(query.queryKey[0])) })
  } catch (error) {
    if (error instanceof ApiError && error.status === 409) { preview.value = null; await router.replace({ query: { ...route.query, replacement: undefined } }) }
    report(error)
  } finally { busy.value = false }
}
async function edit() {
  preview.value = null
  await router.replace({ query: { ...route.query, replacement: undefined } })
}
</script>

<template>
  <UiDialog v-model:open="open" :title="t('replaceStorages')" :description="t('replaceHint')" :busy="busy">
    <dl class="facts"><dt>{{ t('replaceSource') }}</dt><dd>{{ source?.name }}</dd></dl>
    <form v-if="!preview" @submit.prevent="prepare">
      <label>{{ t('replaceTarget') }}<UiSelect v-model="target" :options="options" :label="t('replaceTarget')" :disabled="busy" /></label>
      <label v-if="target === 'new'">{{ t('keyName') }}<input v-model="name" maxlength="128" :placeholder="t('replaceAutomaticName')" :disabled="busy" autocomplete="off"></label>
      <p v-if="target === 'new'" class="field-help">{{ source?.algorithm }} · {{ t('replaceCreateHint') }}</p>
      <KeyCapacity v-else :value="selected?.nonce_capacity" />
      <div class="form-footer key-replacement-footer"><button type="button" :disabled="busy" @click="open = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy">{{ t('replacePreview') }}</button></div>
    </form>
    <template v-else>
      <dl class="facts"><dt>{{ t('replaceTarget') }}</dt><dd>{{ preview.target_name }}<br>{{ preview.algorithm }}</dd><dt>{{ t('replaceAssociated') }}</dt><dd>{{ preview.storages.length }}</dd></dl>
      <ul class="key-replacement-storages"><li v-for="storage in preview.storages" :key="storage.id"><span>{{ storage.name }}</span><span class="badge node-state" :data-tone="storage.enabled ? 'mint' : 'gray'">{{ t(storage.enabled ? 'storeEnabled' : 'disabled') }}</span></li></ul>
      <p class="field-help">{{ t('replaceRetainOld') }}</p>
      <template v-if="result">
        <p class="key-replacement-saved" role="status">{{ t('replaceSaved') }} · {{ date(result.committed_at) }}</p>
        <p v-if="!applied || applied.pending.length" class="field-help">{{ t('replacePending') }}<template v-if="applied?.pending.length"> ({{ applied.pending.length }})</template></p>
        <p v-if="cluster" class="field-help">{{ t('replaceClusterHint') }}</p>
      </template>
      <p v-else class="field-help">{{ t('replaceExpiry', { time: date(preview.expires_at) }) }}</p>
      <div class="form-footer key-replacement-footer"><button :disabled="busy" @click="result ? open = false : edit()">{{ t(result ? 'done' : 'replaceEdit') }}</button><button v-if="!result || !applied || applied.pending.length" class="primary" :disabled="busy" @click="apply">{{ t(result ? 'replaceRetry' : 'replaceConfirm') }}</button></div>
    </template>
  </UiDialog>
</template>
