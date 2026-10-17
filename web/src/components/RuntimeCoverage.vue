<script setup lang="ts">
import type { components } from '../api/schema'
import { t, date } from '../app/i18n'
defineProps<{ runtime?: components['schemas']['ClusterRuntime'] | null }>()
</script>
<template><div class="runtime-coverage" :class="{ 'is-partial': runtime?.stale || (runtime?.current && runtime.current.reported_nodes < runtime.current.expected_nodes) }"><template v-if="runtime?.current"><strong>{{ t('clusterCoverage', { reported: runtime.current.reported_nodes, expected: runtime.current.expected_nodes }) }}</strong><span>{{ t('clusterRateCoverage', { count: runtime.current.rate_nodes }) }}</span><span>{{ date(runtime.current.at) }}</span><span v-if="runtime.stale" class="badge node-state" data-tone="cream">{{ t('clusterReportStale') }}</span></template><span v-else>{{ t('clusterHistoryCollecting') }}</span></div></template>
