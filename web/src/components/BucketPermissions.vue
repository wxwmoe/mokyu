<script setup lang="ts">
import type { Bucket as BucketView } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiCheckbox from './ui/UiCheckbox.vue'
type Action = components['schemas']['Action']
type Bucket = Pick<BucketView, 'id' | 'name' | 'actions'>
type Grant = components['schemas']['BucketGrant']
const props = defineProps<{ buckets: Bucket[]; actions: Action[]; disabled?: boolean }>()
const grants = defineModel<Grant[]>({ required: true })
function available(bucket: Bucket) { return props.actions.filter(action => bucket.actions.includes(action)) }
function toggle(bucket: string, action: Action, checked: boolean) {
  const current = grants.value.find(grant => grant.bucket_id === bucket)?.actions || []
  const actions = checked ? [...new Set([...current, action])] : current.filter(value => value !== action)
  grants.value = [...grants.value.filter(grant => grant.bucket_id !== bucket), ...(actions.length ? [{ bucket_id: bucket, actions }] : [])]
}
function grantAll(bucket: Bucket) { const actions = available(bucket); grants.value = [...grants.value.filter(grant => grant.bucket_id !== bucket.id), ...(actions.length ? [{ bucket_id: bucket.id, actions }] : [])] }
function allGranted(bucket: Bucket) { const actions = available(bucket); const current = grants.value.find(grant => grant.bucket_id === bucket.id)?.actions || []; return actions.length > 0 && actions.every(action => current.includes(action)) }
function clearAll(bucket: Bucket) { grants.value = grants.value.filter(grant => grant.bucket_id !== bucket.id) }
</script>
<template>
  <fieldset v-for="bucket in props.buckets" :key="bucket.id" class="grant-card"><legend>{{ bucket.name }}</legend><div class="grant-tools"><span class="field-help">{{ t('permissionSelectionHint') }}</span><div class="grant-actions"><button v-if="!allGranted(bucket)" type="button" class="text-button grant-all" :disabled="disabled || !available(bucket).length" @click="grantAll(bucket)">{{ t('grantAllPermissions') }}</button><button v-else type="button" class="text-button" :disabled="disabled" @click="clearAll(bucket)">{{ t('clearPermissions') }}</button></div></div><div class="permission-grid"><UiCheckbox v-for="action in available(bucket)" :key="action" :model-value="!!grants.find(grant => grant.bucket_id === bucket.id)?.actions.includes(action)" :label="t('action_' + action)" :disabled="disabled" @update:model-value="toggle(bucket.id, action, $event)" /></div></fieldset>
  <p v-if="!buckets.length" class="info-callout">{{ t('noBuckets') }}</p>
</template>
