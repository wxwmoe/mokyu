<script setup lang="ts">
import { useId } from 'vue'
import { CheckboxRoot, CheckboxIndicator } from 'reka-ui'
import { Check } from 'lucide-vue-next'
defineProps<{ label: string; disabled?: boolean }>()
const value = defineModel<boolean>({ required: true })
const id = useId()
</script>
<template><label class="check-field" :for="id"><CheckboxRoot :id="id" :model-value="value" :disabled="disabled" :aria-label="label" class="checkbox" @update:model-value="value = $event === true"><CheckboxIndicator><Check :size="14" :stroke-width="3" /></CheckboxIndicator></CheckboxRoot><span>{{ label }}</span></label></template>
