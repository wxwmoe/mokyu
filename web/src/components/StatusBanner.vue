<script setup lang="ts">
import { computed } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { ArrowUpRight, TriangleAlert } from 'lucide-vue-next'
import { api, session } from '../api/client'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import ErrorNotice from './ErrorNotice.vue'

type ServiceStatus = components['schemas']['ServiceStatus']
type StatusNotice = { tone: 'danger' | 'warning'; title: string; description: string; action: string; to: string }
const admin = computed(() => session.value?.role === 'admin')
const status = useQuery({ queryKey: ['service-status'], enabled: admin, queryFn: ({ signal }) => api<ServiceStatus>('/api/v1/system/status', { signal }), refetchInterval: 15_000 })
const notices = computed<StatusNotice[]>(() => {
  const value = status.data.value
  if (!admin.value || !value) return []
  const rows: StatusNotice[] = []
  if (value.recovery_required) rows.push({ tone: 'danger', title: t('recoveryBannerTitle'), description: t('recoveryBannerHint'), action: t('openServiceStatus'), to: '/service' })
  if (value.mode_mismatch) rows.push({ tone: 'danger', title: t('modeMismatchBannerTitle'), description: t('modeMismatchBannerHint'), action: t('openServiceStatus'), to: '/service' })
  if (value.restart_required) rows.push({ tone: 'warning', title: t('restartBannerTitle'), description: t('restartBannerHint'), action: t('openRuntimeConfig'), to: '/runtime-config' })
  if (value.maintenance) rows.push({ tone: 'warning', title: t('maintenanceBannerTitle'), description: t('maintenanceBannerHint'), action: t('openServiceStatus'), to: '/service' })
  return rows
})
</script>

<template>
  <ErrorNotice v-if="status.error.value" :error="status.error.value" />
  <div v-if="notices.length" class="overview-status-stack" role="status" aria-live="polite"><section v-for="notice in notices" :key="notice.title" class="overview-status-banner" :class="'is-' + notice.tone"><TriangleAlert :size="21" /><div class="overview-status-copy"><strong>{{ notice.title }}</strong><p>{{ notice.description }}</p></div><RouterLink class="button" :to="notice.to">{{ notice.action }}<ArrowUpRight :size="15" /></RouterLink></section></div>
</template>
