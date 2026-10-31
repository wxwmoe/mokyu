<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useInfiniteQuery, useQuery } from '@tanstack/vue-query'
import { Plus, RefreshCw } from 'lucide-vue-next'
import { api, params, uuid, write, type Bucket } from '../api/client'
import type { components } from '../api/schema'
import { locale, t } from '../app/i18n'
import ErrorNotice from '../components/ErrorNotice.vue'
import UiCheckbox from '../components/ui/UiCheckbox.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import UiListFooter from '../components/ui/UiListFooter.vue'
import UiSelect from '../components/ui/UiSelect.vue'

type Link = components['schemas']['RedundancyLink']
type Settings = components['schemas']['RedundancySettings']
type Preview = components['schemas']['RedundancyPreview']
type Action = components['schemas']['RedundancyAction']
const props = defineProps<{ bucket: Bucket; buckets: Bucket[] }>()
const base = computed(() => '/api/v1/buckets/' + props.bucket.id + '/redundancy')
const cursor = ref(''), creating = ref(false), target = ref(''), prefix = ref(''), acl = ref(''), deletes = ref(false)
const maxPending = ref(4096)
const busy = ref(false), error = ref<unknown>(), checked = ref<Preview>(), creationId = ref('')
const action = ref<{ link: Link; name: Action }>(), scanPrefix = ref('')
const actionOpen = computed({ get: () => !!action.value, set: value => { if (!value) action.value = undefined } })
const links = useQuery({ queryKey: ['redundancy', base, cursor], queryFn: ({ signal }) =>
  api<components['schemas']['RedundancyPage']>(base.value + '?' + params({after:cursor.value || undefined}), {signal}), refetchInterval: 5000 })
const external = useInfiniteQuery({ queryKey: ['external-buckets','replication'], initialPageParam: '',
  queryFn: ({ signal, pageParam }) => api<components['schemas']['ExternalBucketPage']>('/api/v1/external-buckets?' + params({after:pageParam || undefined,limit:'100'}),{signal}),
  getNextPageParam: page => page.next || undefined })
const resources = computed(() => [
  ...props.buckets.filter(b => b.id !== props.bucket.id && b.storage_id !== props.bucket.storage_id && b.status === 'active' && b.actions.includes('bucket.settings') && b.actions.includes('object.write')).map(b => ({value:b.id,label:b.name,external:false,actions:b.actions})),
  ...(external.data.value?.pages.flatMap(p=>p.items) || []).filter(b => b.enabled && b.actions.includes('object.write')).map(b=>({value:'external:'+b.id,label:b.name,description:t('externalBuckets'),external:true,actions:b.actions})),
])
const selected = computed(() => resources.value.find(b => b.value === target.value))
const canCreate = computed(() => props.bucket.status === 'active' && props.bucket.actions.includes('bucket.list') && props.bucket.actions.includes('object.read'))
const canDelete = computed(() => !!selected.value && !selected.value.external && selected.value.actions.includes('object.delete'))
const permissionOptions = computed(() => [
  {value:'',label:t('redundancyDefaultAcl')},
  ...['inherit','ignore','public_read','private'].filter(v => v==='ignore'||selected.value?.actions.includes('object.acl')).map(v=>({value:v,label:t('replicationPermission_'+v)})),
])
watch(target, () => { acl.value = ''; deletes.value = false })
watch([target,prefix,acl,deletes,maxPending], () => { checked.value = undefined; creationId.value = '' })
const settings = computed<Settings>(() => ({target_bucket:target.value.replace(/^external:/,''),target_external:!!selected.value?.external,
  target_prefix:prefix.value,max_pending_keys:maxPending.value,mode:'asynchronous',permissions:acl.value ? acl.value as NonNullable<Settings['permissions']> : null,propagate_deletes:deletes.value}))
function openCreate() { maxPending.value=4096; error.value=undefined; target.value=''; prefix.value=''; acl.value=''; deletes.value=false; checked.value=undefined; creationId.value=''; creating.value=true }
async function create() {
  if (!selected.value || busy.value) return
  busy.value=true; error.value=undefined
  try {
    if (!checked.value) { checked.value = await write<Preview>(base.value+'/preview',settings.value); return }
    if (!creationId.value) creationId.value=uuid()
    await api(base.value,{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':creationId.value},body:JSON.stringify(checked.value.settings)})
    creating.value=false; cursor.value=''; await links.refetch()
  } catch (e) { error.value=e } finally { busy.value=false }
}
function choose(link: Link, name: Action) { error.value=undefined; scanPrefix.value=''; action.value={link,name} }
async function apply() {
  if (!action.value || busy.value) return
  const {link,name} = action.value
  busy.value=true; error.value=undefined
  try { await write(base.value+'/'+link.id+'/command',{revision:link.revision,action:name,prefix:name==='reconcile'?scanPrefix.value:null}); action.value=undefined; await links.refetch() }
  catch(e) { error.value=e } finally { busy.value=false }
}
function destination(link: Link) {
  if (link.source_bucket !== props.bucket.id) return props.buckets.find(b=>b.id===link.source_bucket)?.name || link.source_bucket
  if (link.settings.target_external) return external.data.value?.pages.flatMap(p=>p.items).find(b=>b.id===link.settings.target_bucket)?.name || link.settings.target_bucket
  return props.buckets.find(b=>b.id===link.settings.target_bucket)?.name || link.settings.target_bucket
}
</script>
<template>
  <div class="form-stack">
    <div class="row-between"><div><h2>{{ t('redundancyTitle') }}</h2><p class="field-help">{{ t('redundancyHint') }}</p></div><div class="actions"><button type="button" class="icon-button" :aria-label="t('refresh')" @click="links.refetch()"><RefreshCw :size="16"/></button><button v-if="canCreate" type="button" class="primary" @click="openCreate"><Plus :size="16"/>{{ t('redundancyAdd') }}</button></div></div>
    <ErrorNotice v-if="links.error.value || external.error.value" :error="links.error.value || external.error.value"/>
    <p v-if="links.data.value && !links.data.value.links.length" class="field-help">{{ t('redundancyEmpty') }}</p>
    <article v-for="link in links.data.value?.links || []" :key="link.id" class="surface form-stack">
      <div class="row-between"><div><span class="eyebrow">{{ t(link.source_bucket===bucket.id?'redundancyOutgoing':'redundancyIncoming') }}</span><h3>{{ destination(link) }}</h3><code>{{ link.settings.target_prefix || '/' }}</code></div><span class="badge">{{ t('redundancyStatus_'+link.status) }}</span></div>
      <p>{{ t('redundancyConfirmed') }}: {{ link.confirmed ?? '—' }} · {{ t('redundancyPending') }}: {{ link.pending }} · {{ t('redundancyLastCheck') }}: {{ link.reconciled_at ? new Date(link.reconciled_at).toLocaleString(locale) : '—' }}</p>
      <p class="field-help">{{ t(link.settings.propagate_deletes?'redundancyDeleteOn':'redundancyDeleteOff') }} <span v-if="link.settings.target_external">{{ t('redundancyExternalHint') }}</span></p>
      <p v-if="link.queued_keys >= (link.settings.max_pending_keys ?? 4096)" role="status">{{ t('redundancyQueueFull') }}</p>
      <p v-if="link.error" role="status">{{ link.error }}</p>
      <div class="actions"><template v-if="link.source_bucket===bucket.id"><button v-if="!['paused','pausing','removing'].includes(link.status)" @click="choose(link,'pause')">{{ t('redundancyAction_pause') }}</button><button v-if="link.status==='paused'" @click="choose(link,'resume')">{{ t('redundancyAction_resume') }}</button><button v-if="['failed','lagging'].includes(link.status)" @click="choose(link,'retry')">{{ t('redundancyAction_retry') }}</button><button v-if="['healthy','lagging'].includes(link.status)" @click="choose(link,'reconcile')">{{ t('redundancyAction_reconcile') }}</button></template><button v-if="link.status!=='removing'" class="danger-button" @click="choose(link,'remove')">{{ t('redundancyAction_remove') }}</button></div>
    </article>
    <UiListFooter :first="!!cursor" :next="!!links.data.value?.next_after" @first="cursor=''" @next="cursor=links.data.value?.next_after || ''"/>
    <UiDialog v-model:open="creating" :busy="busy" :title="t('redundancyAdd')">
      <ErrorNotice v-if="error" :error="error"/>
      <fieldset class="form-stack" :disabled="busy || !!checked">
        <label>{{ t('replicationTarget') }}<UiSelect v-model="target" :options="resources" :label="t('replicationTarget')" searchable/></label>
        <button v-if="external.hasNextPage.value" type="button" :disabled="external.isFetchingNextPage.value" @click="external.fetchNextPage()">{{ t('nextPage') }}</button>
        <label class="field">{{ t('redundancyPrefix') }}<input v-model="prefix" maxlength="1024"></label>
        <label>{{ t('replicationPermissions') }}<UiSelect v-model="acl" :options="permissionOptions" :label="t('replicationPermissions')"/></label>
        <label class="field">{{ t('redundancyQueueLimit') }}<input v-model.number="maxPending" type="number" min="1" max="1000000" step="1" required></label>
        <UiCheckbox v-model="deletes" :disabled="!canDelete" :label="t('redundancyPropagateDeletes')"/>
      </fieldset>
      <p class="field-help">{{ t(selected?.external?'redundancyExternalHint':'redundancyManagedHint') }}</p>
      <p v-if="checked" class="info-callout">{{ t('redundancyReview') }} {{ selected?.label }} / {{ checked.settings.target_prefix }}</p>
      <template #footer><button v-if="checked" :disabled="busy" @click="checked=undefined">{{ t('redundancyEdit') }}</button><button :disabled="busy" @click="creating=false">{{ t('cancel') }}</button><button class="primary" :disabled="busy || !selected" @click="create">{{ t(checked?'redundancyAdd':'redundancyPreview') }}</button></template>
    </UiDialog>
    <UiDialog v-model:open="actionOpen" :busy="busy" :title="t('redundancyAction_'+(action?.name || 'reconcile'))">
      <template v-if="action"><ErrorNotice v-if="error" :error="error"/><p>{{ destination(action.link) }} / {{ action.link.settings.target_prefix }}</p>
        <p>{{ t('redundancyExplain_'+action.name) }}</p><label v-if="action.name==='reconcile'" class="field">{{ t('redundancyScanPrefix') }}<input v-model="scanPrefix" maxlength="1024"></label></template>
      <template #footer><button :disabled="busy" @click="action=undefined">{{ t('cancel') }}</button><button :disabled="busy" :class="action?.name==='remove'?'danger-button':'primary'" @click="apply">{{ t('redundancyAction_'+(action?.name || 'reconcile')) }}</button></template>
    </UiDialog>
  </div>
</template>
