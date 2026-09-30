<script setup lang="ts">
import { SelectRoot, SelectTrigger, SelectValue, SelectPortal, SelectContent, SelectViewport, SelectItem, SelectItemText, SelectItemIndicator, SelectScrollUpButton, SelectScrollDownButton } from 'reka-ui'
import { Check, ChevronDown, ChevronUp } from 'lucide-vue-next'
defineProps<{ options: { value: string; label: string; description?: string; disabled?: boolean }[]; label: string; disabled?: boolean; placeholder?: string }>()
const value = defineModel<string>({ required: true })
</script>
<template>
  <SelectRoot v-model="value" :disabled="disabled">
    <SelectTrigger class="select-trigger" :aria-label="label"><SelectValue :placeholder="placeholder || label" /><ChevronDown :size="16" aria-hidden="true" /></SelectTrigger>
    <SelectPortal><SelectContent class="popover select-content" position="popper" :side-offset="6" :collision-padding="12">
      <SelectScrollUpButton class="select-scroll"><ChevronUp :size="16" /></SelectScrollUpButton>
      <SelectViewport class="menu-viewport"><SelectItem v-for="option in options" :key="option.value" :value="option.value" :disabled="option.disabled" class="menu-item select-item">
        <span><SelectItemText>{{ option.label }}</SelectItemText><small v-if="option.description">{{ option.description }}</small></span>
        <SelectItemIndicator><Check :size="16" aria-hidden="true" /></SelectItemIndicator>
      </SelectItem></SelectViewport>
      <SelectScrollDownButton class="select-scroll"><ChevronDown :size="16" /></SelectScrollDownButton>
    </SelectContent></SelectPortal>
  </SelectRoot>
</template>
