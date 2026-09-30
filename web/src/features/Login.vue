<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { api, write, refreshSession, ApiError } from '../api/client'
const username = ref(''), password = ref(''), busy = ref(false), error = ref('')
const router = useRouter()
async function login() {
  busy.value = true; error.value = ''
  try {
    const info = await api<{ contract: string }>('/api/info')
    if (info.contract !== 'mokyu-manage-1') throw new Error('The frontend and management API need matching versions.')
    await write('/api/login', { username: username.value, password: password.value })
    password.value = ''
    await refreshSession()
    await router.replace('/media')
  } catch (failure) {
    error.value = failure instanceof ApiError ? `${failure.code} · ${failure.requestId || ''}` : String(failure)
  } finally { busy.value = false }
}
</script>

<template>
  <div class="login-page">
    <section class="login-art"><img src="/assets/mokyu-mochi.svg" alt="mochi holding a Mokyu pack"><h1>Little chunks,<br>lots of love.</h1><p>A softer space for everything you create.</p></section>
    <form class="login-card" @submit.prevent="login">
      <img src="/assets/mokyu-icon.svg" width="64" height="64" alt="Mokyu">
      <h2>Welcome home</h2><p>Sign in to your Mokyu workspace.</p>
      <label>Username<input v-model="username" name="username" autocomplete="username" required maxlength="64" :disabled="busy"></label>
      <label>Password<input v-model="password" name="password" type="password" autocomplete="current-password" required maxlength="1024" :disabled="busy"></label>
      <p v-if="error" class="error" role="alert">{{ error }}</p>
      <button class="primary" :disabled="busy">{{ busy ? 'Signing in…' : 'Come on in' }}</button>
    </form>
  </div>
</template>
