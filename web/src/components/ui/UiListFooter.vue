<script setup lang="ts">
import { ChevronRight } from 'lucide-vue-next'
import { t } from '../../app/i18n'
const props = defineProps<{ first?: boolean; next?: boolean; summary?: string }>()
const emit = defineEmits<{ first: []; next: [] }>()
</script>

<template>
  <div v-if="props.first || props.next || props.summary || $slots.after" class="list-footer">
    <span v-if="props.summary" class="list-footer-summary">{{ props.summary }}</span>
    <div v-if="props.first || props.next" class="list-footer-actions">
      <button v-if="props.first" type="button" @click="emit('first')">{{ t('firstPage') }}</button>
      <button v-if="props.next" type="button" @click="emit('next')">{{ t('nextPage') }}<ChevronRight :size="15" /></button>
    </div>
    <slot name="after" />
  </div>
</template>

<style scoped>
.list-footer { display: flex; align-items: center; justify-content: flex-end; flex-wrap: wrap; gap: 12px; margin-top: 12px; padding-top: 4px; }
.list-footer-summary { min-width: 0; margin-right: auto; color: var(--muted); font-size: var(--small-size, 12px); overflow-wrap: anywhere; }
.list-footer-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; }
</style>
