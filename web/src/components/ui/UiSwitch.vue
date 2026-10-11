<script setup lang="ts">
const props = defineProps<{ label?: string; disabled?: boolean }>()
const value = defineModel<boolean>({ required: true })
</script>

<template>
  <button class="ui-switch" type="button" role="switch" :aria-checked="value" :aria-label="props.label" :disabled="disabled" @click="value = !value">
    <span class="ui-switch-track"><span class="ui-switch-thumb" /></span>
    <span v-if="label" class="ui-switch-label">{{ label }}</span>
  </button>
</template>
