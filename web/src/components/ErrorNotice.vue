<script setup lang="ts">
import { computed } from 'vue'
import { CircleAlert, Copy } from 'lucide-vue-next'
import { ApiError } from '../api/client'
import { errorText, copy } from '../app/feedback'
import { t } from '../app/i18n'
const props = defineProps<{ error: unknown }>()
const requestId = computed(() => props.error instanceof ApiError ? props.error.requestId : undefined)
</script>
<template><div v-if="error" class="error inline-error" role="alert"><CircleAlert :size="20" /><div><p>{{ errorText(error) }}</p><button v-if="requestId" class="text-button request-id" :aria-label="t('copyRequestId')" @click="copy(requestId)"><Copy :size="13" /><code>{{ requestId }}</code></button></div></div></template>
