<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { SelectRoot, SelectTrigger, SelectValue, SelectPortal, SelectContent, SelectViewport, SelectItem, SelectItemText, SelectItemIndicator, SelectScrollUpButton, SelectScrollDownButton } from 'reka-ui'
import { Check, ChevronDown, ChevronUp } from 'lucide-vue-next'
import { ComboboxRoot, ComboboxAnchor, ComboboxInput, ComboboxTrigger, ComboboxPortal, ComboboxContent, ComboboxViewport, ComboboxItem, ComboboxItemIndicator, ComboboxEmpty } from 'reka-ui'
import { t } from '../../app/i18n'
const props = defineProps<{ options: { value: string; label: string; description?: string; disabled?: boolean }[]; label: string; disabled?: boolean; placeholder?: string; searchable?: boolean }>()
const value = defineModel<string>({ required: true })
function display(selected: unknown) { return props.options.find(option => option.value === selected)?.label || '' }
const selectedLabel = computed(() => display(value.value))
const open = ref(false), search = ref('')
watch([selectedLabel, open], ([label, expanded]) => { if (!expanded) search.value = label }, { immediate: true })
</script>
<template>
  <ComboboxRoot v-if="searchable" v-model="value" v-model:open="open" :disabled="disabled" class="searchable-select">
    <ComboboxAnchor class="combo-anchor"><ComboboxInput v-model="search" :aria-label="label" :display-value="display" :placeholder="placeholder || label" autocomplete="off" /><ComboboxTrigger class="icon-button" :aria-label="t('openChoices')"><ChevronDown :size="16" /></ComboboxTrigger></ComboboxAnchor>
    <ComboboxPortal><ComboboxContent class="popover combo-content" position="popper" :side-offset="6" :collision-padding="12"><ComboboxViewport class="menu-viewport"><ComboboxEmpty class="combo-empty">{{ t('noChoiceMatches') }}</ComboboxEmpty><ComboboxItem v-for="option in options" :key="option.value" :value="option.value" :text-value="option.label" :disabled="option.disabled" class="menu-item select-item"><span>{{ option.label }}<small v-if="option.description">{{ option.description }}</small></span><ComboboxItemIndicator><Check :size="16" /></ComboboxItemIndicator></ComboboxItem></ComboboxViewport></ComboboxContent></ComboboxPortal>
  </ComboboxRoot>
  <SelectRoot v-else v-model="value" :disabled="disabled">
    <SelectTrigger class="select-trigger" :aria-label="label"><SelectValue :placeholder="placeholder || label">{{ selectedLabel || placeholder || label }}</SelectValue><ChevronDown :size="16" aria-hidden="true" /></SelectTrigger>
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
