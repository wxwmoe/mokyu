<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ShieldCheck } from 'lucide-vue-next'
import { write, queries, bytes } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { secure } from '../app/security'
import { errorText } from '../app/feedback'
import UiDialog from './ui/UiDialog.vue'
const props = defineProps<{ operation: 'sweep' | 'unpack' | 'quarantine' | 'recheck'; input: Record<string, unknown> }>()
const open = defineModel<boolean>('open', { required: true }), router = useRouter()
const preview = ref<components['schemas']['MaintenancePreview'] | null>(null), phrase = ref(''), busy = ref(false), error = ref('')
const impact = computed(() => preview.value?.impact as Record<string, any> | undefined)
const sourceAction = computed(() => props.operation === 'quarantine' || props.operation === 'recheck')
async function load() {
  busy.value = true; error.value = ''; preview.value = null; phrase.value = ''
  try { preview.value = await write(`/api/v1/maintenance/${props.operation}/preview`, props.input) }
  catch (e) { error.value = errorText(e) } finally { busy.value = false }
}
watch(open, value => { if (value) load(); else { preview.value = null; phrase.value = '' } })
async function execute() {
  if (!preview.value) return
  busy.value = true; error.value = ''
  try {
    const job = await secure(() => write<components['schemas']['TaskStarted']>(`/api/v1/maintenance/${props.operation}/execute`, { preview_id: preview.value!.id, confirmation: phrase.value }))
    open.value = false; await queries.invalidateQueries({ queryKey: ['maintenance'] }); await router.push({ path: '/tasks', query: { task: job.task_id } })
  } catch (e) { error.value = errorText(e) } finally { busy.value = false }
}
</script>
<template><UiDialog v-model:open="open" :title="t(sourceAction ? 'sourceConfirm_' + operation : operation === 'sweep' ? 'confirmSweep' : 'confirmUnpack')" :description="t(sourceAction ? 'sourceHint_' + operation : operation === 'sweep' ? 'sweepConfirmHint' : 'unpackConfirmHint')" :busy="busy">
  <div class="confirmation-emblem"><ShieldCheck :size="32" /></div><p v-if="error" class="error" role="alert">{{ error }}</p><div v-if="busy && !preview" class="skeleton-row" />
  <template v-if="preview && impact"><dl class="facts" v-if="operation === 'unpack'"><dt>{{ t('packs') }}</dt><dd>{{ impact.packs }}</dd><dt>{{ t('rawContent') }}</dt><dd>{{ bytes(Number(impact.raw_bytes)) }}</dd></dl><dl v-else-if="sourceAction" class="facts"><dt>{{ t('physicalSource') }}</dt><dd>{{ t('physical_' + impact.source) }} #{{ impact.id }}</dd><dt>{{ t('sourceRevision') }}</dt><dd>{{ impact.revision }}</dd><dt>{{ t('sourceLocation') }}</dt><dd class="break-path"><code>{{ impact.location_id }}</code></dd><dt>{{ t('sourceMembers') }}</dt><dd>{{ impact.members }}</dd></dl><dl v-else class="facts"><dt>{{ t('backendNamespace') }}</dt><dd><code>{{ impact.backend_prefix || '/' }}</code></dd><dt>{{ t('sweepCandidates') }}</dt><dd>{{ impact.summary?.candidates }}</dd><dt>{{ t('candidateSize') }}</dt><dd>{{ bytes(Number(impact.summary?.bytes)) }}</dd></dl><p class="field-help">{{ t('previewExpires', { time: date(preview.expires_at) }) }}</p><form @submit.prevent="execute"><label>{{ t('typeConfirmation', { value: preview.confirmation }) }}<input v-model="phrase" autocomplete="off" spellcheck="false" :disabled="busy"></label><div class="form-footer"><button type="button" :disabled="busy" @click="load">{{ t('refreshPreview') }}</button><button class="danger" :disabled="busy || phrase !== preview.confirmation">{{ t('startTask') }}</button></div></form></template><button v-else-if="error" @click="load">{{ t('tryAgain') }}</button>
</UiDialog></template>
