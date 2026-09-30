<script setup lang="ts">
import { computed, ref, watch } from 'vue'
const props = defineProps<{ name: string; src?: string | null; large?: boolean }>()
const failed = ref(false)
const initials = computed(() => {
  const words = props.name.trim().split(/\s+/)
  return (words.length > 1 ? words.slice(0, 2).map(word => Array.from(word)[0]).join('') : Array.from(props.name).slice(0, 2).join('')).toUpperCase()
})
watch(() => props.src, () => { failed.value = false })
</script>
<template><span class="avatar-fallback" :class="{ large }"><img v-if="src && !failed" :src="src" alt="" referrerpolicy="no-referrer" @error="failed = true"><template v-else>{{ initials }}</template></span></template>
