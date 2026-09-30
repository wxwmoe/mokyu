<script setup lang="ts">
import type { Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiCheckbox from './ui/UiCheckbox.vue'
type Action = components['schemas']['Action']
type Grant = components['schemas']['BucketGrant']
const props = defineProps<{ buckets: Bucket[]; actions: Action[]; disabled?: boolean }>()
const grants = defineModel<Grant[]>({ required: true })
function toggle(bucket: string, action: Action, checked: boolean) {
  const current = grants.value.find(grant => grant.bucket_id === bucket)?.actions || []
  const actions = checked ? [...new Set([...current, action])] : current.filter(value => value !== action)
  grants.value = [...grants.value.filter(grant => grant.bucket_id !== bucket), ...(actions.length ? [{ bucket_id: bucket, actions }] : [])]
}
</script>
<template>
  <fieldset v-for="bucket in props.buckets" :key="bucket.id" class="grant-card"><legend>{{ bucket.name }}</legend><div class="permission-grid"><UiCheckbox v-for="action in actions.filter(action => bucket.actions.includes(action))" :key="action" :model-value="!!grants.find(grant => grant.bucket_id === bucket.id)?.actions.includes(action)" :label="t('action_' + action)" :disabled="disabled" @update:model-value="toggle(bucket.id, action, $event)" /></div></fieldset>
  <p v-if="!buckets.length" class="info-callout">{{ t('noBuckets') }}</p>
</template>
