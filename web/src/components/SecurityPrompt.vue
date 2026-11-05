<script setup lang="ts">
import { nextTick, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { passwordPrompt } from '../app/security'
import { ApiError, refreshSession, session, write } from '../api/client'
import { errorText } from '../app/feedback'
import { t } from '../app/i18n'
import UiDialog from './ui/UiDialog.vue'

const route = useRoute()
const password = ref(''), busy = ref(false), error = ref('')
const field = ref<HTMLInputElement | null>(null)
watch([() => route.fullPath, () => session.value?.session_id], () => passwordPrompt.value?.(false), { flush: 'sync' })
watch(passwordPrompt, () => { password.value = ''; error.value = ''; busy.value = false })

async function submit() {
  const finish = passwordPrompt.value
  if (!finish || busy.value) return
  busy.value = true
  error.value = ''
  try {
    await write('/api/v1/account/reauth', { password: password.value })
    if (passwordPrompt.value === finish) finish(true)
  } catch (failure) {
    let message = errorText(failure)
    if (failure instanceof ApiError && ['AccessDenied', 'CsrfTokenMismatch'].includes(failure.code)) {
      try {
        if (await refreshSession() && failure.code === 'AccessDenied') message = t('passwordNotVerified')
      } catch (recoveryError) { message = errorText(recoveryError) }
    }
    if (passwordPrompt.value === finish) {
      error.value = message
      password.value = ''
      busy.value = false
      await nextTick()
      field.value?.focus()
    }
  }
}
</script>

<template>
  <UiDialog :open="!!passwordPrompt" :title="t('verifyIdentity')" :description="t('verifyHint')" :busy="busy"
    @update:open="open => { if (!open) passwordPrompt?.(false) }" @open-auto-focus.prevent="field?.focus()">
    <form @submit.prevent="submit">
      <label>{{ t('currentPassword') }}<input ref="field" v-model="password" type="password" autocomplete="current-password" required maxlength="1024" :disabled="busy" :aria-invalid="!!error"></label>
      <p v-if="error" class="error" role="alert">{{ error }}</p>
      <div class="form-footer">
        <button type="button" :disabled="busy" @click="passwordPrompt?.(false)">{{ t('cancel') }}</button>
        <button class="primary" :disabled="busy">{{ t('continue') }}</button>
      </div>
    </form>
  </UiDialog>
</template>
