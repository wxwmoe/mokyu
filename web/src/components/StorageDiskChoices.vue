<script setup lang="ts">
import { CheckboxGroupRoot } from 'reka-ui'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiCheckbox from './ui/UiCheckbox.vue'

defineProps<{ devices: components['schemas']['StorageClusterDevice'][]; disabled: boolean }>()
const selected = defineModel<string[]>({ required: true })
</script>

<template>
  <CheckboxGroupRoot v-model="selected" class="disk-choices" role="group" :aria-label="t('scMembers')" :roving-focus="false" :disabled="disabled">
    <UiCheckbox v-for="{ media_id, announcement: { topology, manifest } } in devices" :key="media_id" class="disk-choice" :value="media_id" :label="topology.storage_group + ' · ' + topology.url">
      <span class="disk-copy">
        <strong>{{ topology.storage_group }}</strong>
        <span>{{ topology.url }}</span>
        <small>{{ topology.region || t('scUnset') }} · {{ t('sc_' + topology.purpose) }}<template v-if="manifest"> · {{ t('scRevision') }} {{ manifest.revision }}</template></small>
      </span>
    </UiCheckbox>
  </CheckboxGroupRoot>
</template>

<style scoped>
.disk-choices, .disk-copy { display: grid; min-width: 0; }
.disk-choices { gap: 8px; }
.disk-choice { align-items: flex-start; gap: 12px; margin: 0; padding: 12px 14px; border: 1px solid var(--line); border-radius: 12px; }
.disk-choice :deep(.checkbox) { margin-top: 2px; }
.disk-choice :deep(.checkbox[aria-checked='false']) { border-color: var(--muted); }
.disk-choice:hover, .disk-choice:focus-within { border-color: var(--rose-line); }
.disk-choice:has([aria-checked='true']) { border-color: var(--rose); background: var(--rose-soft); }
.disk-choice:has(:disabled) { cursor: not-allowed; }
.disk-copy { gap: 2px; overflow-wrap: anywhere; }
.disk-copy > span, .disk-copy small { color: var(--muted); font-weight: 400; }
.disk-copy small { font-size: var(--caption-size, 11px); }
</style>
