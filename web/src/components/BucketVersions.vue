<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { History } from 'lucide-vue-next'
import { t } from '../app/i18n'
import { api, write, type Bucket } from '../api/client'
import ErrorNotice from './ErrorNotice.vue'
const props = defineProps<{ bucket: Bucket }>()
const base = computed(() => '/api/v1/buckets/' + props.bucket.id + '/versioning')
const mode = useQuery({ queryKey: ['versioning', base], queryFn: ({ signal }) => api<'off'|'enabled'|'suspended'>(base.value, { signal }) })
const busy = ref(false), error = ref<unknown>()
async function set() { busy.value = true; error.value = undefined; try { await write(base.value, mode.data.value === 'enabled' ? 'suspended' : 'enabled', 'PUT'); await mode.refetch() } catch (e) { error.value = e } finally { busy.value = false } }
</script>
<template><section class="settings-section"><h3><History :size="18"/> {{ t('versionTitle') }}</h3><p class="field-help">{{ t('versionHint') }}</p><ErrorNotice v-if="mode.error.value || error" :error="mode.error.value || error"/><div v-if="mode.data.value" class="version-settings"><span class="badge">{{ t('versionMode_' + mode.data.value) }}</span><button :disabled="busy || bucket.status !== 'active'" @click="set">{{ t(mode.data.value === 'enabled' ? 'versionSuspend' : 'versionEnable') }}</button></div></section></template>
<style scoped>.version-settings{display:flex;align-items:center;gap:1rem;margin-top:1rem}h3{display:flex;align-items:center;gap:.6rem}</style>
