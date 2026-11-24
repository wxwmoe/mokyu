<script setup lang="ts">
import { computed } from 'vue'
import { CircleAlert, Copy, RefreshCw } from 'lucide-vue-next'
import { ApiError } from '../api/client'
import { errorText, copy } from '../app/feedback'
import { t } from '../app/i18n'
const props = defineProps<{ error: unknown; title?: string; retry?: () => unknown; retrying?: boolean }>()
const message = computed(() => errorText(props.error))
const requestId = computed(() => props.error instanceof ApiError ? props.error.requestId : undefined)
</script>
<template><div v-if="message" class="error inline-error" role="alert"><CircleAlert :size="20" /><div><strong v-if="title">{{ title }}</strong><p>{{ message }}</p><button v-if="requestId" class="text-button request-id" :aria-label="t('copyRequestId')" @click="copy(requestId)"><Copy :size="13" /><code>{{ requestId }}</code></button><button v-if="retry" type="button" class="text-button" :disabled="retrying" @click="retry()"><RefreshCw :size="14" />{{ t('tryAgain') }}</button></div></div></template>
