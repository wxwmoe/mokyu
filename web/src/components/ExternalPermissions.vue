<script setup lang="ts">
import { computed } from 'vue'
import { useInfiniteQuery } from '@tanstack/vue-query'
import { api, params } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import BucketPermissions from './BucketPermissions.vue'
import ErrorNotice from './ErrorNotice.vue'

type Grant = components['schemas']['BucketGrant']
type Action = components['schemas']['Action']
const props = defineProps<{ project?: string; actions: Action[]; disabled?: boolean; summary?: boolean }>()
const grants = defineModel<Grant[]>({ required: true })
const allowed: Action[] = ['bucket.list', 'object.read', 'object.write', 'object.delete', 'object.acl']
const actions = computed(() => props.actions.filter(action => allowed.includes(action)))
const query = useInfiniteQuery({
  queryKey: computed(() => ['external-buckets', 'permissions', props.project || '']),
  initialPageParam: '',
  queryFn: ({ signal, pageParam }) => api<components['schemas']['ExternalBucketPage']>('/api/v1/external-buckets?' + params({ project: props.project || undefined, after: pageParam || undefined, limit: '100' }), { signal }),
  getNextPageParam: page => page.next || undefined,
})
const buckets = computed(() => query.data.value?.pages.flatMap(page => page.items) || [])
</script>
<template>
  <section class="detail-section">
    <h3>{{ t('externalBuckets') }}</h3>
    <ErrorNotice :error="query.error.value" />
    <p v-if="query.isPending.value" class="field-help" role="status">{{ t('loading') }}</p>
    <template v-else-if="summary">
      <div v-for="grant in grants" :key="grant.bucket_id" class="grant-summary"><strong>{{ buckets.find(bucket => bucket.id === grant.bucket_id)?.name || grant.bucket_id }}</strong><div class="permission-pills"><span v-for="action in grant.actions" :key="action" class="badge">{{ t('action_' + action) }}</span></div></div>
    </template>
    <BucketPermissions v-else v-model="grants" :buckets="buckets" :actions="actions" :disabled="disabled" />
    <button v-if="query.hasNextPage.value" type="button" :disabled="query.isFetchingNextPage.value" @click="query.fetchNextPage()">{{ t('externalLoadMore') }}</button>
    <button v-if="query.isError.value" type="button" :disabled="query.isFetching.value" @click="query.refetch()">{{ t('externalRetry') }}</button>
  </section>
</template>
