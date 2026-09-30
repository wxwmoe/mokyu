<script setup lang="ts">
import { TooltipRoot, TooltipTrigger, TooltipPortal, TooltipContent, TooltipArrow } from 'reka-ui'
defineProps<{ text: string }>()
</script>
<template><TooltipRoot><TooltipTrigger as-child><slot /></TooltipTrigger><TooltipPortal><TooltipContent class="tooltip" :side-offset="7">{{ text }}<TooltipArrow /></TooltipContent></TooltipPortal></TooltipRoot></template>
