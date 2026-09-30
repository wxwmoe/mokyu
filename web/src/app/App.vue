<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { TooltipProvider, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator } from 'reka-ui'
import { Heart, Sparkles, Images, Sun, Moon, Menu, LogOut, ArrowUpRight, ChevronDown } from 'lucide-vue-next'
import { session, signOut } from '../api/client'
import { appearance, setTheme } from './theme'
import { report } from './feedback'
import { startupError } from './router'
import Notices from '../components/Notices.vue'
import UiMenu from '../components/ui/UiMenu.vue'
import UiTip from '../components/ui/UiTip.vue'
import UiDialog from '../components/ui/UiDialog.vue'
const router = useRouter(), navigation = ref(false)
async function logout() { try { await signOut(); await router.replace('/login') } catch (error) { report(error) } }
function reload() { location.reload() }
</script>

<template><TooltipProvider :delay-duration="350">
  <a href="#main" class="skip-link">Skip to content</a>
  <div v-if="startupError" class="error startup-error" role="alert">{{ startupError }} <button @click="reload">Try again</button></div>
  <div v-if="session" class="app-shell">
    <aside class="sidebar">
      <RouterLink class="brand" to="/"><img src="/assets/mokyu-icon.svg" alt="" width="48" height="48"><span>Mokyu<small>made for your media</small></span></RouterLink>
      <p class="nav-label">Workspace</p>
      <nav aria-label="Workspace"><RouterLink to="/media"><Images :size="19" />Media library<span class="nav-dot" /></RouterLink></nav>
      <div class="sidebar-bottom"><div class="mochi-note"><Sparkles class="note-sparkle" :size="18" /><img src="/assets/mokyu-mochi.svg" alt="" width="124" height="116"><p>Room for a little more love.</p><span>Your media, gently cared for.</span></div></div>
    </aside>
    <div class="workspace">
      <header class="topbar"><div class="topbar-location"><button class="icon-button mobile-nav" aria-label="Open navigation" @click="navigation = true"><Menu :size="21" /></button><span class="location-icon"><Images :size="18" /></span><span>Workspace <span class="crumb-slash">/</span> <strong>Media library</strong></span></div><div class="topbar-actions">
        <UiTip :text="appearance === 'dark' ? 'Switch to light' : 'Switch to dark'"><button class="icon-button" :aria-label="appearance === 'dark' ? 'Switch to light' : 'Switch to dark'" @click="setTheme(appearance === 'dark' ? 'light' : 'dark')"><Sun v-if="appearance === 'dark'" :size="19" /><Moon v-else :size="19" /></button></UiTip>
        <span class="topbar-divider" />
        <UiMenu label="Account menu"><template #trigger><span class="avatar-fallback">{{ session.username.slice(0, 2).toUpperCase() }}</span><span class="account-name">{{ session.username }}</span><ChevronDown :size="14" /></template><DropdownMenuLabel class="menu-label">Your workspace</DropdownMenuLabel><DropdownMenuItem class="menu-item" as-child><a href="/classic/"><ArrowUpRight :size="17" />Classic console</a></DropdownMenuItem><DropdownMenuSeparator class="menu-separator" /><DropdownMenuItem class="menu-item" @select="logout"><LogOut :size="17" />Sign out</DropdownMenuItem></UiMenu>
      </div></header>
      <main id="main" tabindex="-1"><RouterView /></main>
      <footer class="app-footer"><span>Mokyu <span class="footer-dot">·</span> Little chunks, lots of love. <Heart :size="13" class="footer-heart" /></span><span>A cozy home for your media</span></footer>
    </div>
  </div>
  <main v-else id="main" tabindex="-1"><RouterView /></main>
  <UiDialog v-model:open="navigation" title="Your workspace"><nav class="mobile-menu"><RouterLink to="/media" @click="navigation = false"><Images :size="20" />Media library</RouterLink></nav></UiDialog>
  <Notices />
</TooltipProvider></template>
