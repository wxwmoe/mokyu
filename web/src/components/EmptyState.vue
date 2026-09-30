<script setup lang="ts">defineProps<{ title: string; description?: string; compact?: boolean }>()</script>
<template><div class="empty-state" :class="{ compact }"><img src="/assets/mokyu-mochi.svg" alt="" :width="compact ? 96 : 150" :height="compact ? 96 : 150"><h2>{{ title }}</h2><p v-if="description">{{ description }}</p><slot /></div></template>
