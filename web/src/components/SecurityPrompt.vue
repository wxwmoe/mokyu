<script setup lang="ts">
import { ref, watch } from 'vue'
import { verifyOpen, verified } from '../app/security'
import { write } from '../api/client'
import { errorText } from '../app/feedback'
import { t } from '../app/i18n'
import UiDialog from './ui/UiDialog.vue'
const password = ref(''), busy = ref(false), error = ref('')
watch(verifyOpen, value => { password.value = ''; error.value = ''; if (!value) verified(false) })
async function submit() {
  busy.value = true; error.value = ''
  try { await write('/api/me/reauth', { password: password.value }); verified(true) }
  catch (failure) { error.value = errorText(failure) }
  finally { password.value = ''; busy.value = false }
}
</script>
<template><UiDialog v-model:open="verifyOpen" :title="t('verifyIdentity')" :description="t('verifyHint')" :busy="busy"><form @submit.prevent="submit"><label>{{ t('currentPassword') }}<input v-model="password" type="password" autocomplete="current-password" required maxlength="1024" :disabled="busy"></label><p v-if="error" class="error" role="alert">{{ error }}</p><div class="form-footer"><button type="button" :disabled="busy" @click="verified(false)">{{ t('cancel') }}</button><button class="primary" :disabled="busy">{{ t('continue') }}</button></div></form></UiDialog></template>
