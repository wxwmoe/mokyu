<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { FolderOpen, Plus, Trash2 } from 'lucide-vue-next'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiSelect from './ui/UiSelect.vue'
import UiCheckbox from './ui/UiCheckbox.vue'
type Source = components['schemas']['MirroringSource']
const props = defineProps<{ options: { value: string; label: string; bucket: string; prefix: string }[]; admin: boolean }>()
const source = defineModel<Source>({ required: true })
const headers = ref<{ name: string; value: string }[]>([]), replacing = ref(false)
watch(() => source.value, value => {
  replacing.value = value.type === 'http' && value.headers != null
  headers.value = value.type === 'http' ? Object.entries(value.headers || {}).map(([name, value]) => ({ name, value })) : []
}, { immediate: true })
const types = computed(() => [{ value: 'http', label: 'HTTP / HTTPS' }, { value: 's3', label: t('externalBuckets') }])
const location = computed(() => {
  const value = source.value
  if (value.type !== 's3') return ''
  const connection = props.options.find(item => item.value === value.external_id)
  return connection ? `s3://${connection.bucket}/${connection.prefix ? connection.prefix + '/' : ''}${value.prefix || ''}` : ''
})
function change(type: string) {
  source.value = type === 's3' ? { type: 's3', external_id: '', prefix: '' } : { type: 'http', url: '', allow_http: false, allow_private: false, follow_redirects: false, headers: {}, header_names: [] }
}
function flush() {
  if (source.value.type === 's3' && !source.value.external_id) throw new Error(t('mirrorNoExternal'))
  if (source.value.type !== 'http' || !replacing.value) return
  const names = headers.value.map(row => row.name.toLowerCase())
  if (names.some(name => !name) || new Set(names).size !== names.length) throw new Error(t('mirrorHeadersInvalid'))
  source.value.headers = Object.fromEntries(headers.value.map((row, index) => [names[index]!, row.value]))
}
defineExpose({ flush })
</script>
<template>
  <div class="form-stack">
    <label class="field"><span>{{ t('mirrorSourceType') }}</span><UiSelect :model-value="source.type" :options="types" :label="t('mirrorSourceType')" @update:model-value="change" /></label>
    <template v-if="source.type === 's3'">
      <label class="field"><span>{{ t('externalBuckets') }}</span><UiSelect v-model="source.external_id" :options="props.options" :label="t('externalBuckets')" /><small v-if="!props.options.length" class="field-help">{{ t('mirrorNoExternal') }}</small></label>
      <label class="field"><span>{{ t('mirrorSourcePrefix') }}</span><input v-model="source.prefix" :aria-label="t('mirrorSourcePrefix')" maxlength="1024" placeholder="photos/" spellcheck="false"><small class="field-help">{{ t('mirrorSourcePrefixHint') }}</small></label>
      <div v-if="location" class="mirror-source-location"><FolderOpen :size="18" aria-hidden="true" /><div><span>{{ t('mirrorSourceLocation') }}</span><code>{{ location }}</code></div></div>
    </template>
    <template v-else>
      <label class="field"><span>{{ t('mirrorUrl') }}</span><input v-model="source.url" type="url" required maxlength="2048" placeholder="https://origin.example/media/"></label>
      <details><summary>{{ t('mirrorConnectionOptions') }}</summary><div class="form-stack mirror-options">
        <UiCheckbox :model-value="!!source.allow_http" @update:model-value="source.allow_http = $event" :label="t('mirrorAllowHttp')" />
        <UiCheckbox :model-value="!!source.allow_private" @update:model-value="source.allow_private = $event" :label="t('mirrorAllowPrivate')" :disabled="!props.admin" />
        <UiCheckbox :model-value="!!source.follow_redirects" @update:model-value="source.follow_redirects = $event" :label="t('mirrorRedirects')" />
        <div class="section-heading"><h4>{{ t('mirrorHeaders') }}</h4><button v-if="!replacing" type="button" @click="replacing = true">{{ t('mirrorReplaceHeaders') }}</button></div>
        <p v-if="!replacing" class="field-help">{{ t('mirrorHeadersKept') }}<span v-if="source.header_names?.length"> · {{ source.header_names?.join(', ') }}</span></p>
        <template v-else>
          <div v-for="(row, index) in headers" :key="index" class="mirror-header-row">
            <input v-model="row.name" :aria-label="t('mirrorHeaderName')" :placeholder="t('mirrorHeaderName')" required maxlength="128">
            <input v-model="row.value" :aria-label="t('mirrorHeaderValue')" :placeholder="t('mirrorHeaderValue')" type="password" autocomplete="new-password" maxlength="4096">
            <button type="button" class="icon-button" :aria-label="t('mirrorRemove')" @click="headers.splice(index, 1)"><Trash2 :size="16" /></button>
          </div>
          <button type="button" @click="headers.push({ name: '', value: '' })"><Plus :size="16" />{{ t('mirrorAddHeader') }}</button>
        </template>
        <p class="field-help">{{ t('mirrorHeadersHint') }}</p>
      </div></details>
    </template>
  </div>
</template>
<style scoped>
.mirror-options { margin-top: 16px; }
.mirror-source-location { display: flex; align-items: flex-start; gap: 10px; padding: 12px 14px; border-radius: 12px; background: var(--sky-soft); color: var(--sky); }
.mirror-source-location > svg { flex: none; margin-top: 2px; }
.mirror-source-location > div { display: grid; min-width: 0; gap: 4px; }
.mirror-source-location span { font-size: var(--small-size, 12px); }
.mirror-source-location code { color: var(--ink); overflow-wrap: anywhere; white-space: pre-wrap; }
.mirror-header-row { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto; gap: 8px; align-items: center; }
@media (max-width: 480px) { .mirror-header-row { grid-template-columns: minmax(0, 1fr) auto; } .mirror-header-row input:first-child { grid-column: 1 / -1; } }
</style>
