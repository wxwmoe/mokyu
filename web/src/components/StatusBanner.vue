<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { ArrowUpRight, TriangleAlert } from 'lucide-vue-next'
import { session } from '../api/client'
import { useDeployment } from '../app/cluster'
import { t } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'

type StatusNotice = { tone: 'danger' | 'warning'; title: string; description: string; action: string; to: string }
const admin = computed(() => session.value?.role === 'admin')
const route = useRoute()
const { status, cluster, overview } = useDeployment()
const notices = computed<StatusNotice[]>(() => {
  const value = status.data.value
  if (!admin.value || !value) return []
  const rows: StatusNotice[] = []
  const keys = value.nonce_alerts || []
  if (keys.length) rows.push({ tone: keys.some(key => key.state === 'exhausted') ? 'danger' : 'warning', title: t('nonceBannerTitle'), description: t('nonceBannerHint', { names: keys.map(key => key.name).join(', ') }), action: t('managedKeys'), to: '/credentials?tab=managed' + (keys.find(key => key.storage_count > 0) ? '&replace_key=' + keys.find(key => key.storage_count > 0)!.key_id : '') })
  if (value.recovery_required) rows.push({ tone: 'danger', title: t('recoveryBannerTitle'), description: t('recoveryBannerHint'), action: t('openServiceStatus'), to: '/service' })
  if (value.mode_mismatch) rows.push({ tone: 'danger', title: t('modeMismatchBannerTitle'), description: t('modeMismatchBannerHint'), action: t('openServiceStatus'), to: '/service' })
  if (cluster.value) {
    for (const notice of rows) notice.description = t('clusterConnectedControl', { name: value.current_node?.name || '—' }) + ' · ' + notice.description
    const nodes = overview.data.value?.nodes.filter(row => row.contact !== 'released') || []
    const pending = nodes.filter(row => row.configuration === 'restart_required').reduce((n, row) => n + Number(row.count), 0)
    const unknown = nodes.filter(row => row.configuration === 'unknown').reduce((n, row) => n + Number(row.count), 0)
    const unavailable = nodes.filter(row => row.contact !== 'online' || ['recovery', 'restricted'].includes(row.work)).reduce((n, row) => n + Number(row.count), 0)
    if (pending || unknown) rows.push({ tone: 'warning', title: t('clusterConfiguration'), description: [pending ? t('clusterPending', { count: pending }) : '', unknown ? t('clusterUnknown', { count: unknown }) : ''].filter(Boolean).join(' · '), action: t('openServiceStatus'), to: '/service?configuration=' + (pending ? 'restart_required' : 'unknown') })
    if (unavailable) rows.push({ tone: 'warning', title: t('clusterNeedsAttention'), description: t('clusterUnavailableHint', { count: unavailable }), action: t('openServiceStatus'), to: '/service' })
  } else if (value.restart_required) rows.push({ tone: 'warning', title: t('restartBannerTitle'), description: t('restartBannerHint'), action: t('openRuntimeConfig'), to: '/runtime-config' })
  if (value.maintenance && route.path !== '/maintenance') rows.push({ tone: 'warning', title: t('maintenanceBannerTitle'), description: t('maintenanceBannerHint'), action: t('openMaintenance'), to: '/maintenance' })
  return rows
})
</script>

<template>
  <ErrorNotice v-if="status.error.value || overview.error.value" :error="status.error.value || overview.error.value" />
  <div v-if="notices.length" class="overview-status-stack" role="status" aria-live="polite"><section v-for="notice in notices" :key="notice.title" class="overview-status-banner" :class="'is-' + notice.tone"><TriangleAlert :size="21" /><div class="overview-status-copy"><strong>{{ notice.title }}</strong><p>{{ notice.description }}</p></div><RouterLink class="button" :to="notice.to">{{ notice.action }}<ArrowUpRight :size="15" /></RouterLink></section></div>
</template>
