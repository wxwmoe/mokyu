<script setup lang="ts">
import { useId } from 'vue'
import { CheckboxRoot, CheckboxIndicator } from 'reka-ui'
import { Check } from 'lucide-vue-next'
defineProps<{ label: string; disabled?: boolean; value?: string }>()
const checked = defineModel<boolean>({ default: false })
const id = useId()
</script>
<template>
  <label class="check-field" :for="id">
    <CheckboxRoot :id="id" :value="value" :model-value="checked" :disabled="disabled" :aria-label="label" class="checkbox" @update:model-value="checked = $event === true">
      <CheckboxIndicator><Check :size="14" :stroke-width="3" /></CheckboxIndicator>
    </CheckboxRoot>
    <slot><span>{{ label }}</span></slot>
  </label>
</template>
