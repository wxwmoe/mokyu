<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { api, params } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiSelect from './ui/UiSelect.vue'
import UiListFooter from './ui/UiListFooter.vue'
import ErrorNotice from './ErrorNotice.vue'
const props = defineProps<{ name?: string }>()
const selected = defineModel<string>({ required: true })
const after = ref('')
const query = computed(() => params({ limit: '50', after: after.value || undefined }))
const nodes = useQuery({ queryKey: ['cluster-node-picker', query], queryFn: ({ signal }) => api<components['schemas']['NodePage']>('/api/v1/cluster/nodes?' + query.value, { signal }) })
const options = computed(() => {
  const items = (nodes.data.value?.items || []).map(node => ({ value: node.id, label: `${node.name} · ${t('nodeRole_' + node.role)}` }))
  if (selected.value && !items.some(node => node.value === selected.value)) items.unshift({ value: selected.value, label: props.name || selected.value })
  return items
})
</script>
<template><div class="node-picker"><UiSelect v-model="selected" :options="options" :label="t('clusterSelectNode')" /><UiListFooter :first="!!after" :next="!!nodes.data.value?.next_cursor" @first="after = ''" @next="after = nodes.data.value?.next_cursor || ''" /><ErrorNotice v-if="nodes.error.value" :error="nodes.error.value" /></div></template>
