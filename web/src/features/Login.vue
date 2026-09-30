<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { api, write, refreshSession } from '../api/client'
import { startupError } from '../app/router'
import { errorText, notify } from '../app/feedback'
import { t } from '../app/i18n'
import LocaleMenu from '../components/LocaleMenu.vue'
const username = ref(''), password = ref(''), busy = ref(false), error = ref('')
const setup = ref(false), ready = ref(false), setupToken = ref(''), confirm = ref('')
const router = useRouter()
async function bootstrap() {
  error.value = ''
  try { setup.value = (await api<{ setup_required: boolean }>('/api/bootstrap')).setup_required; ready.value = true }
  catch (failure) { error.value = errorText(failure) }
}
onMounted(bootstrap)
async function login() {
  busy.value = true; error.value = ''
  try {
    const info = await api<{ contract: string }>('/api/info')
    if (info.contract !== 'mokyu-manage-1') throw new Error(t('incompatible'))
    if (setup.value) {
      if (confirm.value !== password.value) throw new Error(t('passwordMismatch'))
      await write('/api/setup', { token: setupToken.value.trim(), username: username.value, password: password.value })
      setupToken.value = ''; confirm.value = ''; setup.value = false
      notify(t('setupDone'))
    }
    await write('/api/login', { username: username.value, password: password.value })
    password.value = ''
    await refreshSession()
    startupError.value = ''
    await router.replace('/media')
  } catch (failure) {
    error.value = errorText(failure)
  } finally { busy.value = false }
}
</script>

<template>
  <div class="login-page">
    <section class="login-art"><img src="/assets/mokyu-mochi.svg" alt=""><h1>{{ t('loginTaglineFirst') }}<br>{{ t('loginTaglineSecond') }}</h1><p>{{ t('loginStory') }}</p></section>
    <form class="login-card" @submit.prevent="login">
      <div class="login-top"><img src="/assets/mokyu-icon.svg" width="64" height="64" alt="Mokyu"><LocaleMenu /></div>
      <h2>{{ t(setup ? 'setup' : 'welcome') }}</h2><p>{{ t(setup ? 'setupHint' : 'signInHint') }}</p>
      <template v-if="ready"><template v-if="setup"><label>{{ t('setupToken') }}<input v-model="setupToken" name="setup-token" autocomplete="off" required minlength="64" maxlength="64" :disabled="busy" aria-describedby="setup-token-hint"></label><p id="setup-token-hint" class="field-help">{{ t('setupTokenHint') }}</p></template>
      <label>{{ t('username') }}<input v-model="username" name="username" autocomplete="username" required maxlength="64" :disabled="busy"></label>
      <label>{{ t('password') }}<input v-model="password" name="password" type="password" :autocomplete="setup ? 'new-password' : 'current-password'" required :minlength="setup ? 12 : undefined" maxlength="1024" :disabled="busy"></label>
      <label v-if="setup">{{ t('confirmPassword') }}<input v-model="confirm" name="confirm-password" type="password" autocomplete="new-password" required minlength="12" maxlength="1024" :disabled="busy"></label></template>
      <p v-if="error" class="error" role="alert">{{ error }}</p>
      <button v-if="ready" class="primary" :disabled="busy">{{ t(busy ? 'signingIn' : setup ? 'setupCreate' : 'signIn') }}</button>
      <button v-else-if="error" type="button" @click="bootstrap">{{ t('tryAgain') }}</button>
    </form>
  </div>
</template>
