<script setup lang="ts">
import ErrorNotice from '../components/ErrorNotice.vue'
import { computed, reactive, ref, watch, onBeforeUnmount } from 'vue'
import { onBeforeRouteLeave } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import { SwitchRoot, SwitchThumb } from 'reka-ui'
import { Heart, ShieldCheck, Laptop, LogOut, Palette, Save } from 'lucide-vue-next'
import { api, write, session, savePreferences, refreshSession } from '../api/client'
import type { components } from '../api/schema'
import { t, date, locales } from '../app/i18n'
import { notify, report, errorText } from '../app/feedback'
import Avatar from '../components/Avatar.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import UiDialog from '../components/ui/UiDialog.vue'

const draft = reactive({ display_name: '', avatar_email: '', avatar_enabled: false })
function reset() { if (session.value) Object.assign(draft, { display_name: session.value.display_name, avatar_email: session.value.avatar_email, avatar_enabled: session.value.avatar_enabled }) }
watch(() => session.value?.id, reset, { immediate: true })
const dirty = computed(() => !!session.value && Object.entries(draft).some(([key, value]) => value !== session.value?.[key as keyof typeof draft]))
const saving = ref(false), passwordBusy = ref(false), revokeBusy = ref('')
const currentPassword = ref(''), newPassword = ref(''), confirmPassword = ref('')
const sessions = useQuery({ queryKey: ['sessions'], queryFn: ({ signal }) => api<components['schemas']['SessionList']>('/api/me/sessions', { signal }) })
const languageOptions = computed(() => [{ value: 'auto', label: t('followBrowser') }, ...locales.map(value => ({ value, label: t(`language_${value}`) }))])
const themeOptions = computed(() => ['auto', 'light', 'dark'].map(value => ({ value, label: t(value) })))
async function save() {
  saving.value = true
  try { await savePreferences({ ...draft }); reset(); notify(t('saved')) } catch (error) { report(error) } finally { saving.value = false }
}
async function preference(key: 'locale' | 'theme', value: string) {
  try { await savePreferences({ [key]: key === 'locale' && value === 'auto' ? null : value }); notify(t('saved')) } catch (error) { report(error) }
}
async function password() {
  if (newPassword.value !== confirmPassword.value) { notify(t('passwordMismatch'), undefined, 'error'); return }
  passwordBusy.value = true
  try {
    await write('/api/me/password', { current_password: currentPassword.value, new_password: newPassword.value })
    currentPassword.value = ''; newPassword.value = ''; confirmPassword.value = ''
    await refreshSession(); await sessions.refetch(); notify(t('passwordChanged'))
  } catch (error) { report(error) } finally { passwordBusy.value = false }
}
async function revoke(id = '') {
  revokeBusy.value = id || 'others'
  try { await api('/api/me/sessions' + (id ? '/' + id : ''), { method: 'DELETE' }); await sessions.refetch(); notify(t(id ? 'sessionRevoked' : 'otherSessionsRevoked')) }
  catch (error) { report(error) } finally { revokeBusy.value = '' }
}
const discardOpen = ref(false)
let decide: ((leave: boolean) => void) | undefined
function discard(leave: boolean) { decide?.(leave); decide = undefined; discardOpen.value = false }
watch(discardOpen, value => { if (!value) discard(false) })
onBeforeRouteLeave(() => !dirty.value || new Promise<boolean>(resolve => { decide?.(false); decide = resolve; discardOpen.value = true }))
function beforeUnload(event: BeforeUnloadEvent) { if (dirty.value) { event.preventDefault(); event.returnValue = '' } }
window.addEventListener('beforeunload', beforeUnload)
onBeforeUnmount(() => { window.removeEventListener('beforeunload', beforeUnload); decide?.(false) })
</script>

<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('preferences') }}</p><h1>{{ t('account') }}</h1><p>{{ t('preferenceHint') }}</p></div><span class="page-sticker"><Heart :size="26" /></span></div>
  <p v-if="session?.must_change_password" class="info-callout" role="status">{{ t('passwordRequired') }}</p>
  <div class="settings-grid">
    <section class="surface settings-card"><h2><Heart :size="20" />{{ t('profile') }}</h2>
      <div class="profile-intro"><Avatar :name="session?.display_name || session?.username || ''" :src="session?.avatar_url" large /><div><strong>{{ session?.display_name || session?.username }}</strong><p class="field-help">{{ t('avatarUnavailable') }}</p></div></div>
      <form @submit.prevent="save"><label>{{ t('username') }}<input :value="session?.username" readonly autocomplete="username"></label>
        <label>{{ t('displayName') }}<input v-model="draft.display_name" maxlength="120" :disabled="saving" aria-describedby="display-name-hint"></label><p id="display-name-hint" class="field-help">{{ t('displayNameHint') }}</p>
        <div class="switch-field"><label id="avatar-label" for="avatar-enabled">{{ t('avatarEnabled') }}</label><SwitchRoot id="avatar-enabled" v-model="draft.avatar_enabled" class="switch" aria-labelledby="avatar-label" :disabled="saving"><SwitchThumb class="switch-thumb" /></SwitchRoot></div>
        <label>{{ t('avatarEmail') }}<input v-model="draft.avatar_email" type="email" autocomplete="email" maxlength="320" :required="draft.avatar_enabled" :disabled="saving"></label>
        <p class="field-help">{{ t('avatarHint') }}</p>
        <div class="form-footer"><button type="button" :disabled="saving || !dirty" @click="reset">{{ t('cancel') }}</button><button class="primary" :disabled="saving || !dirty"><Save :size="16" />{{ t(saving ? 'saving' : 'save') }}</button></div>
      </form>
    </section>
    <section class="surface settings-card"><h2><Palette :size="20" />{{ t('preferences') }}</h2>
      <label>{{ t('language') }}<UiSelect :model-value="session?.locale || 'auto'" :label="t('language')" :options="languageOptions" @update:model-value="preference('locale', $event)" /></label>
      <label>{{ t('theme') }}<UiSelect :model-value="session?.theme || 'auto'" :label="t('theme')" :options="themeOptions" @update:model-value="preference('theme', $event)" /></label>
      <p class="field-help">{{ t('browserHint') }}</p><div class="settings-mochi"><img src="/assets/mokyu-mochi.svg" alt="" width="160" height="150"><p>{{ t('gently') }}</p></div>
    </section>
    <section class="surface settings-card"><h2><ShieldCheck :size="20" />{{ t('security') }}</h2><p class="field-help">{{ t('passwordHint') }}</p>
      <form @submit.prevent="password"><label>{{ t('currentPassword') }}<input v-model="currentPassword" type="password" autocomplete="current-password" required maxlength="1024" :disabled="passwordBusy"></label>
        <label>{{ t('newPassword') }}<input v-model="newPassword" type="password" autocomplete="new-password" required minlength="12" maxlength="1024" :disabled="passwordBusy"></label>
        <label>{{ t('confirmPassword') }}<input v-model="confirmPassword" type="password" autocomplete="new-password" required minlength="12" maxlength="1024" :disabled="passwordBusy"></label>
        <div class="form-footer"><button :disabled="passwordBusy" class="primary">{{ t(passwordBusy ? 'saving' : 'passwordChange') }}</button></div>
      </form>
    </section>
    <section class="surface settings-card"><h2><Laptop :size="20" />{{ t('sessions') }}</h2><p class="field-help">{{ t('sessionHint') }}</p>
      <ErrorNotice v-if="sessions.error.value" :error="sessions.error.value" />
      <p v-if="sessions.data.value?.more" class="field-help">{{ t('sessionMore') }}</p>
      <ul class="session-list"><li v-for="item in sessions.data.value?.sessions" :key="item.id"><div class="session-heading"><span class="device-name" :title="item.user_agent">{{ item.user_agent || t('unknownDevice') }}</span><span v-if="item.current" class="badge public">{{ t('currentSession') }}</span></div>
        <span class="field-help">{{ t('lastSeen') }} · {{ date(item.last_seen_at) }}</span><span class="field-help">{{ t('expires') }} · {{ date(item.expires_at) }}</span>
        <button v-if="!item.current" class="text-button" :disabled="!!revokeBusy" @click="revoke(item.id)"><LogOut :size="14" />{{ t('revoke') }}</button></li></ul>
      <div class="form-footer"><button :disabled="!!revokeBusy || !sessions.data.value?.sessions.some(item => !item.current)" @click="revoke()">{{ t('revokeOthers') }}</button></div>
    </section>
  </div>
  <UiDialog v-model:open="discardOpen" :title="t('discardTitle')" :description="t('discardHint')"><template #footer><button @click="discard(false)">{{ t('keepEditing') }}</button><button class="primary" @click="discard(true)">{{ t('discard') }}</button></template></UiDialog>
</template>
