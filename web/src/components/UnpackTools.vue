<script setup lang="ts">
import { nextTick, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { PackageOpen } from 'lucide-vue-next'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import MaintenanceConfirm from './MaintenanceConfirm.vue'
import UiSwitch from './ui/UiSwitch.vue'

const props = defineProps<{ status?: components['schemas']['MaintenanceStatus']; busy: boolean }>()
const emit = defineEmits<{ change: [enabled: boolean] }>()
const route = useRoute(), panel = ref<HTMLElement>(), highlighted = ref(false)
const pack = ref(String(route.query.pack || '')), confirm = ref(false), confirmInput = ref<Record<string, unknown>>({ all: true })
let focusConsumed = false, focusTimer: ReturnType<typeof setTimeout> | undefined
watch([() => route.query.focus, () => !!props.status], async ([focus, ready]) => {
  if (focus !== 'pack-creation') { focusConsumed = false; return }
  if (!ready || focusConsumed) return
  await nextTick()
  const target = panel.value?.querySelector<HTMLButtonElement>('[role="switch"]')
  if (!target) return
  focusConsumed = true; highlighted.value = true
  target.scrollIntoView({ behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'center' })
  target.focus({ preventScroll: true })
  clearTimeout(focusTimer)
  focusTimer = setTimeout(() => { highlighted.value = false }, 2400)
}, { immediate: true, flush: 'post' })
onBeforeUnmount(() => clearTimeout(focusTimer))
function unpack(all: boolean) { confirmInput.value = all ? { all: true } : { pack_id: pack.value }; confirm.value = true }
function packStatusKey(status?: components['schemas']['MaintenanceStatus']) {
  if (!status?.pack_configured) return 'packCreationUnconfigured'
  return status.pack_creation_enabled ? 'packCreationOn' : 'packCreationOff'
}
function packStatusClass(status?: components['schemas']['MaintenanceStatus']) {
  if (!status?.pack_configured) return 'is-unconfigured'
  return status.pack_creation_enabled ? 'is-enabled' : 'is-stopped'
}
</script>

<template>
  <section ref="panel" class="surface recovery-card">
    <div class="section-heading"><h2><PackageOpen :size="21" />{{ t('unpackTools') }}</h2></div>
    <p class="field-help">{{ t('unpackWorkflow') }}</p>
    <div class="unpack-controls">
      <div class="pack-setting">
        <div class="unpack-row-copy">
          <div class="unpack-row-title"><strong>{{ t('packCreationToggle') }}</strong><span v-if="status" class="badge pack-creation-status" :class="packStatusClass(status)" role="status">{{ t(packStatusKey(status)) }}</span></div>
          <span id="pack-creation-scope" class="field-help">{{ t('packCreationScope') }}</span>
        </div>
        <UiSwitch v-if="status" :model-value="status.pack_creation_enabled" :highlighted="highlighted" data-maintenance-focus="pack-creation" :disabled="busy || !status.pack_configured" :aria-label="t('packCreationToggle')" aria-describedby="pack-creation-scope" @update:model-value="emit('change', $event)" />
      </div>
      <div class="unpack-actions">
        <div class="unpack-stat"><strong>{{ status?.preparing_packs || '0' }}</strong><span>{{ t('preparingPacksLabel') }}</span></div>
        <button class="danger" :disabled="busy || !status || status.pack_creation_enabled || status.preparing_packs !== '0' || status.maintenance" @click="unpack(true)">{{ t('unpackAll') }}</button>
      </div>
    </div>
    <div class="single-pack-tools">
      <div class="single-pack-heading"><strong>{{ t('unpackById') }}</strong><span class="field-help">{{ t('unpackByIdHint') }}</span></div>
      <form class="single-pack-form" @submit.prevent="unpack(false)">
        <label><span>{{ t('packId') }}</span><input v-model="pack" inputmode="numeric" pattern="[1-9][0-9]*" required></label>
        <button :disabled="busy || !status || !/^[1-9][0-9]*$/.test(pack) || status.maintenance">{{ t('unpackOne') }}</button>
      </form>
    </div>
  </section>
  <MaintenanceConfirm v-model:open="confirm" operation="unpack" :input="confirmInput" />
</template>

<style scoped>
.unpack-controls { display: grid; grid-template-columns: minmax(0, 1.1fr) minmax(0, .9fr); margin: 24px 0; padding: 5px; border-radius: 18px; background: var(--surface-soft); }
.pack-setting, .unpack-actions { display: flex; align-items: center; min-width: 0; padding: 15px 17px; }
.pack-setting { justify-content: space-between; gap: 16px; }
.unpack-row-copy, .unpack-stat { display: grid; gap: 6px; min-width: 0; }
.unpack-row-title { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; min-width: 0; }
.unpack-row-title strong { overflow-wrap: anywhere; }
.pack-creation-status.is-enabled { background: var(--mint-soft); color: var(--mint); }
.pack-creation-status.is-stopped { background: var(--danger-soft); color: var(--danger); }
.pack-creation-status.is-unconfigured { background: var(--gray-soft); color: var(--gray); }
.pack-setting .ui-switch { flex: 0 0 auto; min-width: 42px; justify-content: flex-end; }
.unpack-actions { justify-content: space-between; gap: 16px; border-left: 1px solid var(--line-soft); }
.unpack-stat { grid-template-columns: auto minmax(0, 1fr); align-items: baseline; column-gap: 8px; }
.unpack-stat strong { font-size: 24px; line-height: 1; font-variant-numeric: tabular-nums; color: var(--ink); }
.unpack-stat span { color: var(--muted); font-size: var(--small-size, 12px); overflow-wrap: anywhere; }
.unpack-actions .danger { flex: 0 0 auto; }
.single-pack-tools { display: grid; gap: 12px; margin-top: 22px; padding-top: 20px; border-top: 1px solid var(--line-soft); }
.single-pack-heading { display: flex; align-items: baseline; justify-content: space-between; gap: 16px; min-width: 0; }
.single-pack-heading strong { min-width: 0; overflow-wrap: anywhere; }
.single-pack-heading .field-help { text-align: right; }
.single-pack-form { max-width: none; display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 12px; align-items: end; }
.single-pack-form label { display: grid; gap: 6px; min-width: 0; }
@media (max-width: 900px) {
  .unpack-controls { grid-template-columns: 1fr; }
  .unpack-actions { border-top: 1px solid var(--line-soft); border-left: 0; }
  .single-pack-heading { align-items: flex-start; flex-direction: column; gap: 5px; }
  .single-pack-heading .field-help { text-align: left; }
}
@media (max-width: 480px) {
  .pack-setting, .unpack-actions { align-items: flex-start; padding: 14px; }
  .pack-setting .ui-switch { margin-top: -2px; }
  .unpack-actions { flex-direction: column; }
  .unpack-actions .danger { width: 100%; }
  .single-pack-form { grid-template-columns: 1fr; }
  .single-pack-form button { width: 100%; }
}
</style>
