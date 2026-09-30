<script setup lang="ts">
import { ArrowRight, Heart, Layers3, Sparkles } from 'lucide-vue-next'
import { bytes } from '../api/client'
import { t } from '../app/i18n'
import type { Usage } from '../app/insights'
defineProps<{ usage: Usage }>()
</script>
<template><section class="space-story"><div class="space-steps"><div class="space-step rose"><small>{{ t('logicalMedia') }}</small><strong>{{ bytes(Number(usage.logical_bytes)) }}</strong><span>{{ t('logicalMediaHint') }}</span></div><ArrowRight class="space-arrow" :size="16" /><div class="space-step lilac"><small>{{ t('uniqueContent') }}</small><strong>{{ bytes(Number(usage.unique_bytes)) }}</strong><span>{{ t('uniqueContentHint') }}</span></div><ArrowRight class="space-arrow" :size="16" /><div class="space-step sky"><small>{{ t('attributedContent') }}</small><strong>{{ bytes(Number(usage.attributed_raw_bytes)) }}</strong><span>{{ t('attributedHint') }}</span></div><ArrowRight class="space-arrow" :size="16" /><div class="space-step mint"><small>{{ t('encodedShare') }}</small><strong>{{ usage.encoded_bytes == null ? t('pendingMeasure') : bytes(Number(usage.encoded_bytes)) }}</strong><span>{{ t('encodedHint') }}</span></div></div>
  <div class="savings-strip"><div><Heart :size="16" /><span>{{ t('localDedup') }}</span><strong>{{ bytes(Number(usage.local_savings)) }}</strong></div><div><Layers3 :size="16" /><span>{{ t('sharedDedup') }}</span><strong>{{ bytes(Number(usage.shared_savings)) }}</strong></div><div><Sparkles :size="16" /><span>{{ t(usage.encoding_savings != null && Number(usage.encoding_savings) < 0 ? 'encodingOverhead' : 'encodingSavings') }}</span><strong>{{ usage.encoding_savings == null ? '—' : bytes(Math.abs(Number(usage.encoding_savings))) }}</strong></div></div>
  <p v-if="Number(usage.pending_bytes) > 0" class="info-callout">{{ t('pendingMeasureHint', { size: bytes(Number(usage.pending_bytes)), known: bytes(Number(usage.encoded_known_bytes)) }) }}</p>
  <details class="metric-explanation"><summary>{{ t('howCounted') }}</summary><p>{{ t('attributionExplanation') }}</p><p>{{ t('packAttributionExplanation') }}</p><code>R − A = (R − U) + (U − D) + (D − A)</code></details>
</section></template>
