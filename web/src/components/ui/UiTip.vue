<script setup lang="ts">
import { Primitive, TooltipRoot, TooltipTrigger, TooltipPortal, TooltipContent, TooltipArrow, useForwardExpose } from 'reka-ui'
defineOptions({ inheritAttrs: false })
defineProps<{ text?: string }>()
const { forwardRef } = useForwardExpose()
</script>
<template><TooltipRoot v-if="text"><TooltipTrigger :ref="forwardRef" as-child v-bind="$attrs"><slot /></TooltipTrigger><TooltipPortal><TooltipContent class="tooltip" :aria-label="text" :side-offset="7">{{ text }}<TooltipArrow /></TooltipContent></TooltipPortal></TooltipRoot><Primitive v-else :ref="forwardRef" as-child v-bind="$attrs"><slot /></Primitive></template>
