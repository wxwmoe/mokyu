<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { Users, UserPlus, Search, ShieldCheck, ChevronRight, KeyRound, Trash2 } from 'lucide-vue-next'
import { api, write, params, session, refreshSession, queries } from '../api/client'
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
import { report, notify, errorText } from '../app/feedback'
import { secure } from '../app/security'
import { useRouter } from 'vue-router'
import UiSelect from '../components/ui/UiSelect.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import Avatar from '../components/Avatar.vue'
import EmptyState from '../components/EmptyState.vue'
type User = components['schemas']['User']
const router = useRouter(), search = ref(''), filter = ref(''), after = ref('')
const page = useQuery({ queryKey: ['users', filter, after], queryFn: ({ signal }) => api<components['schemas']['UserPage']>('/api/users?' + params({ q: filter.value, after: after.value }), { signal }) })
const createOpen = ref(false), selected = ref<User | null>(null), busy = ref(false)
const draft = reactive({ username: '', password: '', role: 'member', must_change_password: true })
const role = ref('member'), enabled = ref(true), action = ref<'reset' | 'delete' | ''>(''), password = ref(''), confirm = ref(''), requireChange = ref(true)
const detailOpen = computed({ get: () => !!selected.value, set: value => { if (!value) selected.value = null } })
const actionOpen = computed({ get: () => !!action.value, set: value => { if (!value) action.value = '' } })
const roles = computed(() => ['member', 'admin'].map(value => ({ value, label: t('role_' + value) })))
watch(selected, value => { if (value) { role.value = value.role; enabled.value = value.enabled } })
watch(createOpen, value => { if (!value) draft.password = '' })
watch(action, () => { password.value = ''; confirm.value = ''; requireChange.value = true })
function searchUsers() { filter.value = search.value.trim(); after.value = '' }
async function changed() {
  await queries.invalidateQueries({ queryKey: ['users'] }); await refreshSession()
  if (!session.value) await router.replace('/login')
  notify(t('saved'))
}
async function create() {
  busy.value = true
  try { const user = await secure(() => write<User>('/api/users', { ...draft })); createOpen.value = false; draft.username = ''; await changed(); selected.value = user }
  catch (error) { report(error) } finally { busy.value = false }
}
async function save() {
  if (!selected.value) return
  const id = selected.value.id
  busy.value = true
  try { selected.value = await secure(() => write<User>('/api/users/' + id, { role: role.value, enabled: enabled.value }, 'PATCH')); await changed() }
  catch (error) { report(error) } finally { busy.value = false }
}
async function execute() {
  if (!selected.value) return
  const id = selected.value.id
  busy.value = true
  try {
    if (action.value === 'reset') await secure(() => write('/api/users/' + id + '/reset-password', { password: password.value, must_change_password: requireChange.value }))
    else { await secure(() => api('/api/users/' + id, { method: 'DELETE' })); selected.value = null }
    action.value = ''; await changed()
  } catch (error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('peopleEyebrow') }}</p><h1>{{ t('users') }}</h1><p>{{ t('usersHint') }}</p></div><button class="primary" @click="createOpen = true"><UserPlus :size="17" />{{ t('createUser') }}</button></div>
  <div class="info-callout"><ShieldCheck :size="21" /><p>{{ t('adminHint') }} <RouterLink to="/projects">{{ t('projects') }}<ChevronRight :size="13" /></RouterLink></p></div>
  <section class="surface"><form class="search-bar" @submit.prevent="searchUsers"><Search :size="18" /><input v-model="search" type="search" :aria-label="t('searchUsers')" :placeholder="t('searchUsers')" maxlength="240"><button>{{ t('search') }}</button></form>
    <p v-if="page.error.value" class="error" role="alert">{{ errorText(page.error.value) }} <button @click="page.refetch()">{{ t('tryAgain') }}</button></p>
    <div v-if="page.isPending.value" role="status" :aria-label="t('loading')"><div v-for="i in 4" :key="i" class="skeleton-row" /></div>
    <div v-else class="people-list"><button v-for="user in page.data.value?.users" :key="user.id" class="person-row" @click="selected = user"><Avatar :name="user.display_name || user.username" /><span class="person-name"><strong>{{ user.display_name || user.username }}</strong><small>@{{ user.username }}</small></span><span class="badge" :class="{ public: user.enabled }">{{ t(user.enabled ? 'enabled' : 'disabled') }}</span><span class="role-label">{{ t('role_' + user.role) }}</span><ChevronRight :size="16" /></button></div>
    <EmptyState v-if="page.data.value && !page.data.value.users.length" compact :title="t('noPeople')" :description="t('searchAgain')" />
    <div class="table-footer"><button v-if="after" @click="after = ''">{{ t('firstPage') }}</button><span>{{ t('pageItems', { count: page.data.value?.users.length || 0 }) }}</span><button v-if="page.data.value?.next" @click="after = page.data.value.next">{{ t('nextPage') }}<ChevronRight :size="15" /></button></div>
  </section>
  <UiDialog v-model:open="createOpen" :title="t('createUser')" :description="t('newUserHint')" :busy="busy"><form @submit.prevent="create"><label>{{ t('username') }}<input v-model="draft.username" required maxlength="64" autocomplete="off" :disabled="busy"></label><label>{{ t('role') }}<UiSelect v-model="draft.role" :options="roles" :label="t('role')" :disabled="busy" /></label><p class="field-help">{{ t(draft.role === 'admin' ? 'adminHint' : 'memberHint') }}</p><label>{{ t('initialPassword') }}<input v-model="draft.password" type="password" required minlength="12" maxlength="1024" autocomplete="new-password" :disabled="busy"></label><UiCheckbox v-model="draft.must_change_password" :label="t('requireChange')" :disabled="busy" /><div class="form-footer"><button type="button" :disabled="busy" @click="createOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy"><UserPlus :size="16" />{{ t('createUser') }}</button></div></form></UiDialog>
  <UiDialog v-model:open="detailOpen" :title="selected?.display_name || selected?.username || t('users')" :busy="busy" drawer><template v-if="selected"><div class="person-hero"><Avatar :name="selected.display_name || selected.username" large /><div><span class="eyebrow">{{ t('account') }}</span><h2>@{{ selected.username }}</h2><span class="field-help">{{ t('joined') }} · {{ date(selected.created_at) }}</span></div></div><form @submit.prevent="save"><label>{{ t('role') }}<UiSelect v-model="role" :options="roles" :label="t('role')" :disabled="busy" /></label><UiCheckbox v-model="enabled" :label="t('accountEnabled')" :disabled="busy" /><p class="field-help">{{ t('roleChangeHint') }}</p><button class="primary" :disabled="busy || (role === selected.role && enabled === selected.enabled)">{{ t('save') }}</button></form><div class="detail-section"><h3><Users :size="17" />{{ t('projectMembership') }}</h3><p class="field-help">{{ t(selected.role === 'admin' ? 'adminHint' : 'memberHint') }}</p><RouterLink class="button" :to="{ path: '/projects', query: { user: selected.id } }" @click="selected = null">{{ t('manageMembership') }}<ChevronRight :size="16" /></RouterLink></div><div class="detail-section"><h3><KeyRound :size="17" />{{ t('security') }}</h3><p class="field-help">{{ t('resetHint') }}</p><div class="actions"><button :disabled="busy" @click="action = 'reset'">{{ t('resetPassword') }}</button><button class="danger-button" :disabled="busy" @click="action = 'delete'"><Trash2 :size="15" />{{ t('deleteUser') }}</button></div></div></template></UiDialog>
  <UiDialog v-model:open="actionOpen" :title="t(action === 'reset' ? 'resetPassword' : 'deleteUser')" :description="t(action === 'reset' ? 'resetHint' : 'deleteUserHint')" :busy="busy"><form @submit.prevent="execute"><template v-if="action === 'reset'"><label>{{ t('newPassword') }}<input v-model="password" type="password" required minlength="12" maxlength="1024" autocomplete="new-password" :disabled="busy"></label><UiCheckbox v-model="requireChange" :label="t('requireChange')" :disabled="busy" /></template><label v-else>{{ t('confirmIdentity', { name: selected?.username || '' }) }}<input v-model="confirm" autocomplete="off" :disabled="busy"></label><div class="form-footer"><button type="button" :disabled="busy" @click="action = ''">{{ t('cancel') }}</button><button :class="action === 'delete' ? 'danger-button' : 'primary'" :disabled="busy || (action === 'delete' && confirm !== selected?.username)">{{ t(action === 'reset' ? 'resetPassword' : 'deleteUser') }}</button></div></form></UiDialog>
</template>
