<script setup lang="ts">
defineProps<{ title: string; description?: string; compact?: boolean }>()
</script>

<template>
  <div class="empty-state" :class="{ compact }">
    <img src="/assets/mokyu-mochi.svg" alt="" :width="compact ? 96 : 128" :height="compact ? 96 : 128">
    <h2>{{ title }}</h2>
    <p v-if="description">{{ description }}</p>
    <div v-if="$slots.default" class="empty-state-actions"><slot /></div>
  </div>
</template>

<style scoped>
.empty-state {
  display: flex;
  grid-column: 1 / -1;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 238px;
  padding: 34px 28px 30px;
  color: var(--muted);
  text-align: center;
  border-radius: 22px;
}

.empty-state.compact {
  min-height: 174px;
  padding: 24px 20px;
}

.empty-state img {
  width: 128px;
  height: 128px;
  margin-bottom: 2px;
  object-fit: contain;
  filter: drop-shadow(0 8px 12px #ad60821a);
}

.empty-state.compact img {
  width: 96px;
  height: 96px;
}

.empty-state h2 {
  display: block;
  margin: 6px 0 8px;
  color: var(--ink);
  font-size: 20px;
  text-align: center;
}

.empty-state p {
  width: 100%;
  max-width: none;
  margin: 0;
  line-height: 1.7;
  text-wrap: pretty;
}

.empty-state-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  gap: 10px;
  margin-top: 18px;
}
</style>
