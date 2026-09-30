<script setup lang="ts">
import { DropdownMenuRoot, DropdownMenuTrigger, DropdownMenuPortal, DropdownMenuContent } from 'reka-ui'
import UiTip from './UiTip.vue'
withDefaults(defineProps<{ label: string; align?: 'start' | 'center' | 'end'; tooltip?: string }>(), { align: 'end' })
</script>
<template><DropdownMenuRoot><DropdownMenuTrigger as-child><UiTip :text="tooltip"><button class="icon-button menu-trigger" :aria-label="label"><slot name="trigger" /></button></UiTip></DropdownMenuTrigger><DropdownMenuPortal><DropdownMenuContent class="popover menu-content" :align="align" :side-offset="8" :collision-padding="12"><slot /></DropdownMenuContent></DropdownMenuPortal></DropdownMenuRoot></template>
