<script setup lang="ts">
const props = defineProps<{ label?: string; ariaLabel?: string; disabled?: boolean }>()
const value = defineModel<boolean>({ required: true })
</script>

<template>
  <button class="ui-switch" type="button" role="switch" :aria-checked="value" :aria-label="props.ariaLabel || props.label" :disabled="disabled" @click="value = !value">
    <span v-if="label" class="ui-switch-label">{{ label }}</span>
    <span class="ui-switch-track"><span class="ui-switch-thumb" /></span>
  </button>
</template>
