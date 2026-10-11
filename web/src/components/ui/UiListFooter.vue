<script setup lang="ts">
import { ChevronRight } from 'lucide-vue-next'
import { t } from '../../app/i18n'
defineProps<{ first?: boolean; next?: boolean }>()
const emit = defineEmits<{ first: []; next: [] }>()
</script>

<template>
  <div v-if="first || next" class="list-footer"><button v-if="first" type="button" @click="emit('first')">{{ t('firstPage') }}</button><button v-if="next" type="button" @click="emit('next')">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div>
</template>
