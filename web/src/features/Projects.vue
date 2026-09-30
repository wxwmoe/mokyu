<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import { FolderHeart, Plus, Users, Pencil, Trash2, ArrowUpRight, Search, ShieldCheck } from 'lucide-vue-next'
import { api, write, params, session, refreshSession, queries, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import { notify, report, errorText } from '../app/feedback'
import { secure } from '../app/security'
import UiDialog from '../components/ui/UiDialog.vue'
import UiSelect from '../components/ui/UiSelect.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import Avatar from '../components/Avatar.vue'
import BucketPermissions from '../components/BucketPermissions.vue'
import EmptyState from '../components/EmptyState.vue'
type Project = components['schemas']['Project']
type Member = components['schemas']['Member']
type Grant = components['schemas']['BucketGrant']
type Action = components['schemas']['Action']
const route = useRoute(), router = useRouter(), admin = computed(() => session.value?.role === 'admin')
const projects = useQuery({ queryKey: ['projects'], queryFn: ({ signal }) => api<Project[]>('/api/projects', { signal }) })
const buckets = useQuery({ queryKey: ['buckets'], queryFn: ({ signal }) => api<Bucket[]>('/api/buckets', { signal }) })
const projectId = computed(() => String(route.query.project || ''))
const selected = computed(() => projects.data.value?.find(item => item.id === projectId.value))
const projectBuckets = computed(() => buckets.data.value?.filter(item => item.project_id === projectId.value) || [])
const detailOpen = computed({ get: () => !!selected.value, set: value => { if (!value) router.replace({ query: { ...route.query, project: undefined } }) } })
const members = useQuery({ queryKey: ['members', projectId], enabled: computed(() => admin.value && !!projectId.value), queryFn: ({ signal }) => api<Member[]>(`/api/projects/${projectId.value}/members`, { signal }) })
const editorOpen = ref(false), editing = ref(''), busy = ref(false), deleting = ref(false), confirm = ref('')
const draft = reactive({ name: '', description: '', allow_bucket_create: false })
const membershipOpen = ref(false), memberId = ref(''), memberName = ref(''), memberRole = ref('reader'), scope = ref('selected'), grants = ref<Grant[]>([]), search = ref(''), filter = ref('')
const users = useQuery({ queryKey: ['member-options', filter], enabled: computed(() => admin.value && membershipOpen.value), queryFn: ({ signal }) => api<components['schemas']['UserPage']>('/api/users?' + params({ q: filter.value, limit: '100', role: 'member' }), { signal }) })
const chosenUser = useQuery({ queryKey: ['user', memberId], enabled: computed(() => membershipOpen.value && !!memberId.value && !memberName.value), queryFn: ({ signal }) => api<components['schemas']['User']>('/api/users/' + memberId.value, { signal }) })
const roles = computed(() => ['reader', 'writer', 'maintainer'].map(value => ({ value, label: t('role_' + value) })))
const scopes = computed(() => ['selected', 'all'].map(value => ({ value, label: t('scope_' + value) })))
const actions = computed<Action[]>(() => memberRole.value === 'reader' ? ['bucket.list', 'object.read', 'storage.inspect'] : memberRole.value === 'writer' ? ['bucket.list', 'object.read', 'object.write', 'object.delete', 'object.acl', 'storage.inspect'] : ['bucket.list', 'object.read', 'object.write', 'object.delete', 'object.acl', 'bucket.settings', 'storage.inspect'])
watch(memberRole, () => { grants.value = grants.value.map(grant => ({ ...grant, actions: grant.actions.filter(action => actions.value.includes(action)) })) })
watch(deleting, () => { confirm.value = '' })
function edit(project?: Project) { editing.value = project?.id || ''; Object.assign(draft, { name: project?.name || '', description: project?.description || '', allow_bucket_create: project?.allow_bucket_create || false }); editorOpen.value = true }
function member(row?: Member) { memberRole.value = row?.role || 'reader'; scope.value = row?.scope || 'selected'; grants.value = (row?.grants || []).map(grant => ({ bucket_id: grant.bucket_id, actions: [...grant.actions] })); memberId.value = row?.user_id || String(route.query.user || ''); memberName.value = row?.username || ''; search.value = ''; filter.value = ''; membershipOpen.value = true }
async function changed() { await queries.invalidateQueries({ queryKey: ['projects'] }); await refreshSession(); notify(t('saved')) }
async function save() {
  busy.value = true
  try { await secure(() => write('/api/projects' + (editing.value ? '/' + editing.value : ''), { ...draft }, editing.value ? 'PUT' : 'POST')); editorOpen.value = false; await changed() }
  catch (error) { report(error) } finally { busy.value = false }
}
async function saveMember() {
  busy.value = true
  try { await secure(() => write(`/api/projects/${projectId.value}/members/${memberId.value}`, { role: memberRole.value, scope: scope.value, grants: scope.value === 'all' ? [] : grants.value.filter(grant => grant.actions.length) }, 'PUT')); membershipOpen.value = false; await members.refetch(); notify(t('saved')) }
  catch (error) { report(error) } finally { busy.value = false }
}
const removing = ref<Member | null>(null)
const removeOpen = computed({ get: () => !!removing.value, set: value => { if (!value) removing.value = null } })
async function removeMember() {
  if (!removing.value) return
  const id = removing.value.user_id
  busy.value = true
  try { await secure(() => api(`/api/projects/${projectId.value}/members/${id}`, { method: 'DELETE' })); removing.value = null; await members.refetch(); notify(t('saved')) }
  catch (error) { report(error) } finally { busy.value = false }
}
async function removeProject() {
  busy.value = true
  try { await secure(() => api('/api/projects/' + projectId.value, { method: 'DELETE' })); deleting.value = false; detailOpen.value = false; await changed() }
  catch (error) { report(error) } finally { busy.value = false }
}
async function mode() {
  busy.value = true
  try { await secure(() => write('/api/settings/projects', { enabled: !session.value?.project_management }, 'PUT')); await changed() }
  catch (error) { report(error) } finally { busy.value = false }
}
</script>
<template>
  <div class="page-heading"><div><p class="eyebrow">{{ t('projectsEyebrow') }}</p><h1>{{ t('projects') }}</h1><p>{{ t('projectsHint') }}</p></div><button v-if="admin" class="primary" @click="edit()"><Plus :size="17" />{{ t('createProject') }}</button></div>
  <div v-if="admin" class="info-callout"><FolderHeart :size="22" /><p>{{ t(session?.project_management ? 'projectModeOn' : 'projectModeOff') }}</p><button :disabled="busy" @click="mode">{{ t(session?.project_management ? 'disableProjectMode' : 'enableProjectMode') }}</button></div>
  <p v-if="projects.error.value || buckets.error.value" class="error" role="alert">{{ errorText(projects.error.value || buckets.error.value) }}</p>
  <div class="project-grid"><button v-for="project in projects.data.value" :key="project.id" class="project-card" @click="router.push({ query: { ...route.query, project: project.id } })"><span class="project-symbol"><FolderHeart :size="27" /></span><span v-if="project.builtin" class="badge">{{ t('defaultProject') }}</span><h2>{{ project.name }}</h2><p>{{ project.description || t('projectReady') }}</p><span class="field-help">{{ t('bucketCount', { count: buckets.data.value?.filter(item => item.project_id === project.id).length || 0 }) }}</span><ArrowUpRight :size="17" class="project-arrow" /></button></div>
  <EmptyState v-if="projects.data.value && !projects.data.value.length" :title="t('fresh')" :description="t('askForBucket')" />
  <UiDialog v-model:open="detailOpen" :title="selected?.name || t('projects')" drawer><template v-if="selected"><div class="project-intro"><span class="project-symbol"><FolderHeart :size="28" /></span><p>{{ selected.description || t('projectReady') }}</p></div><div v-if="admin" class="actions"><button @click="edit(selected)"><Pencil :size="15" />{{ t('editProject') }}</button><button v-if="!selected.builtin" class="danger-button" @click="deleting = true"><Trash2 :size="15" />{{ t('deleteProject') }}</button></div><div class="detail-section"><h3>{{ t('media') }}</h3><div class="bucket-links"><RouterLink v-for="bucket in projectBuckets" :key="bucket.id" :to="`/media/${bucket.id}`"><img src="/assets/mokyu-pack.svg" alt="" width="30" height="30">{{ bucket.name }}<ArrowUpRight :size="15" /></RouterLink></div><p v-if="!projectBuckets.length" class="field-help">{{ t('noBuckets') }}</p></div><div v-if="admin" class="detail-section"><div class="section-heading"><h3><Users :size="17" />{{ t('projectMembership') }}</h3><button @click="member()"><Plus :size="14" />{{ t('addMember') }}</button></div><p v-if="members.error.value" class="error" role="alert">{{ errorText(members.error.value) }}</p><div class="member-list"><div v-for="row in members.data.value" :key="row.user_id" class="member-row"><Avatar :name="row.display_name || row.username" /><button class="text-button person-name" @click="member(row)"><strong>{{ row.display_name || row.username }}</strong><small>{{ t('role_' + row.role) }} · {{ t('scope_' + row.scope) }}</small></button><button class="icon-button" :aria-label="t('removeMemberName', { name: row.username })" @click="removing = row"><Trash2 :size="15" /></button></div></div><p v-if="members.data.value && !members.data.value.length" class="field-help">{{ t('noMembers') }}</p></div></template></UiDialog>
  <UiDialog v-model:open="editorOpen" :title="t(editing ? 'editProject' : 'createProject')" :busy="busy"><form @submit.prevent="save"><label>{{ t('name') }}<input v-model="draft.name" required maxlength="128" :disabled="busy"></label><label>{{ t('description') }}<textarea v-model="draft.description" rows="3" maxlength="2000" :disabled="busy" /></label><UiCheckbox v-model="draft.allow_bucket_create" :label="t('allowBucketCreate')" :disabled="busy" /><p class="field-help">{{ t('allowBucketHint') }}</p><div class="form-footer"><button type="button" :disabled="busy" @click="editorOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy">{{ t('save') }}</button></div></form></UiDialog>
  <UiDialog v-model:open="membershipOpen" :title="t('manageMembership')" :description="t('membershipHint')" :busy="busy" wide><form @submit.prevent="saveMember"><div class="membership-layout"><section><h3><Users :size="18" />{{ t('member') }}</h3><p v-if="memberId" class="selected-member">{{ memberName || chosenUser.data.value?.username || t('selectedUser') }}</p><div class="search-bar"><Search :size="16" /><input v-model="search" type="search" :aria-label="t('searchUsers')" :placeholder="t('searchUsers')"><button type="button" @click="filter = search">{{ t('search') }}</button></div><p v-if="users.error.value" class="error" role="alert">{{ errorText(users.error.value) }}</p><div class="user-options"><button v-for="user in users.data.value?.users.filter(user => user.role === 'member')" :key="user.id" type="button" :aria-pressed="memberId === user.id" :class="{ chosen: memberId === user.id }" @click="memberId = user.id; memberName = user.username"><Avatar :name="user.display_name || user.username" /><span>{{ user.display_name || user.username }}</span><ShieldCheck v-if="memberId === user.id" :size="15" /></button></div><p class="field-help">{{ t('memberSearchHint') }}</p><label>{{ t('role') }}<UiSelect v-model="memberRole" :options="roles" :label="t('role')" :disabled="busy" /></label><label>{{ t('bucketScope') }}<UiSelect v-model="scope" :options="scopes" :label="t('bucketScope')" :disabled="busy" /></label><p class="field-help">{{ t(scope === 'all' ? 'allScopeHint' : 'selectedScopeHint') }}</p></section><section class="permission-section"><h3><ShieldCheck :size="18" />{{ t('permissions') }}</h3><p class="field-help">{{ t('role_' + memberRole + 'Hint') }}</p><template v-if="scope === 'selected'"><BucketPermissions v-model="grants" :buckets="projectBuckets" :actions="actions" :disabled="busy" /></template><div v-else class="scope-summary"><img src="/assets/mokyu-pack.svg" alt="" width="90" height="90"><p>{{ t('allScopeHint') }}</p><div class="permission-pills"><span v-for="action in actions" :key="action" class="badge">{{ t('action_' + action) }}</span></div></div></section></div><div class="form-footer"><button type="button" :disabled="busy" @click="membershipOpen = false">{{ t('cancel') }}</button><button class="primary" :disabled="busy || !memberId || (!memberName && !chosenUser.data.value)">{{ t('save') }}</button></div></form></UiDialog>
  <UiDialog v-model:open="deleting" :title="t('deleteProject')" :description="t('deleteProjectHint')" :busy="busy"><form @submit.prevent="removeProject"><label>{{ t('confirmIdentity', { name: selected?.name || '' }) }}<input v-model="confirm" autocomplete="off" :disabled="busy"></label><div class="form-footer"><button type="button" :disabled="busy" @click="deleting = false">{{ t('cancel') }}</button><button class="danger-button" :disabled="busy || confirm !== selected?.name">{{ t('deleteProject') }}</button></div></form></UiDialog>
  <UiDialog v-model:open="removeOpen" :title="t('removeMember')" :description="t('removeMemberHint', { name: removing?.username || '' })" :busy="busy"><template #footer><button :disabled="busy" @click="removing = null">{{ t('cancel') }}</button><button class="danger-button" :disabled="busy" @click="removeMember">{{ t('removeMember') }}</button></template></UiDialog>
</template>
