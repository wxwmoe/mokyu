<script setup lang="ts">
import { useRouter } from 'vue-router'
import { session, signOut } from '../api/client'
const router = useRouter()
async function logout() { await signOut(); await router.replace('/login') }
</script>

<template>
  <a href="#main" class="skip-link">Skip to content</a>
  <div v-if="session" class="app-shell">
    <aside class="sidebar">
      <RouterLink class="brand" to="/"><img src="/assets/mokyu-icon.svg" alt="" width="46" height="46">Mokyu</RouterLink>
      <p class="nav-label">Workspace</p>
      <nav aria-label="Workspace"><RouterLink to="/media">Media library</RouterLink></nav>
      <div class="sidebar-bottom"><img src="/assets/mokyu-mochi.svg" alt="" width="124" height="124"><p>Little chunks, lots of love.</p></div>
    </aside>
    <div class="workspace">
      <header class="topbar"><span>A cozy home for your media</span><div><a href="/classic/">Classic console</a><span>{{ session.username }}</span><button @click="logout">Sign out</button></div></header>
      <main id="main" tabindex="-1"><RouterView /></main>
      <footer>Mokyu · Little chunks, lots of love.</footer>
    </div>
  </div>
  <main v-else id="main" tabindex="-1"><RouterView /></main>
</template>
