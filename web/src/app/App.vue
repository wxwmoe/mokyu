<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { TooltipProvider, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator } from 'reka-ui'
import { Heart, Sparkles, Images, Sun, Moon, Menu, LogOut, ChevronDown, Settings2, Users, FolderHeart, KeyRound, BookHeart, UploadCloud, HouseHeart, Package, Flower2, ListChecks, HeartPulse } from 'lucide-vue-next'
import { session, signOut, savePreferences, refreshSession, queries } from '../api/client'
import { appearance, setTheme } from './theme'
import { report, notices } from './feedback'
import { startupError } from './router'
import Notices from '../components/Notices.vue'
import SecurityPrompt from '../components/SecurityPrompt.vue'
import UiMenu from '../components/ui/UiMenu.vue'
import UiTip from '../components/ui/UiTip.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import LocaleMenu from '../components/LocaleMenu.vue'
import Avatar from '../components/Avatar.vue'
import { t, locale } from './i18n'
const router = useRouter(), route = useRoute(), navigation = ref(false)
const destinations = [
  { key: 'overview', icon: HouseHeart, group: 'workspace' }, { key: 'media', icon: Images, group: 'workspace' },
  { key: 'storage', icon: Package, group: 'workspace' }, { key: 'transfers', icon: UploadCloud, group: 'workspace' },
  { key: 'maintenance', icon: Flower2, group: 'workspace', admin: true },
  { key: 'buckets', icon: Settings2, group: 'administration', admin: true },
  { key: 'users', icon: Users, group: 'administration', admin: true },
  { key: 'projects', icon: FolderHeart, group: 'administration', projects: true },
  { key: 'credentials', icon: KeyRound, group: 'administration', admin: true },
  { key: 'tasks', icon: ListChecks, group: 'administration', admin: true },
  { key: 'audit', icon: BookHeart, group: 'administration' },
  { key: 'service', icon: HeartPulse, group: 'administration', admin: true },
]
const groups = computed(() => ['workspace', 'administration'].map(name => ({ name, entries: destinations.filter(item => (session.value?.role === 'admin' ? item.group === name : name === 'workspace' && !item.admin) && (!item.projects || session.value?.project_management)) })).filter(group => group.entries.length))
const current = computed(() => destinations.find(item => item.key === route.name) || { key: route.name === 'login' ? 'signIn' : ['account', 'tokens'].includes(String(route.name)) ? String(route.name) : 'media', icon: route.name === 'account' ? Settings2 : Images, group: 'workspace' })
async function logout() { try { await signOut(); await router.replace('/login') } catch (error) { report(error) } }
function reload() { location.reload() }
async function toggleTheme() { try { const value = appearance.value === 'dark' ? 'light' : 'dark'; if (session.value) await savePreferences({ theme: value }); else setTheme(value) } catch (error) { report(error) } }
watch([current, locale], () => { document.title = `${t(current.value.key)} · Mokyu` }, { immediate: true })
async function synchronize(event: StorageEvent) {
  if (!['mokyu.identity-change', 'mokyu.preferences-change'].includes(event.key || '')) return
  const identity = event.key === 'mokyu.identity-change'
  if (identity) { session.value = null; queries.clear(); notices.value = [] }
  try { await refreshSession(); if (identity) await router.replace(session.value ? '/overview' : '/login') }
  catch (e) { report(e); if (identity) await router.replace('/login') }
}
window.addEventListener('storage', synchronize)
onBeforeUnmount(() => window.removeEventListener('storage', synchronize))
</script>

<template><TooltipProvider :delay-duration="350">
  <a href="#main" class="skip-link">{{ t('skip') }}</a>
  <div v-if="startupError" class="error startup-error" role="alert">{{ t(startupError) }} <button @click="reload">{{ t('tryAgain') }}</button></div>
  <div v-if="session" class="app-shell">
    <aside class="sidebar">
      <RouterLink class="brand" to="/"><img src="/assets/mokyu-icon.svg" alt="" width="48" height="48"><span>Mokyu<small>{{ t('brandNote') }}</small></span></RouterLink>
      <div class="sidebar-links"><template v-for="group in groups" :key="group.name"><p class="nav-label">{{ t(group.name) }}</p><nav :aria-label="t(group.name)"><RouterLink v-for="entry in group.entries" :key="entry.key" :to="'/' + entry.key"><component :is="entry.icon" :size="19" /><span>{{ t(entry.key) }}</span><i class="nav-dot" /></RouterLink></nav></template></div>
      <div class="sidebar-bottom"><div class="mochi-note"><Sparkles class="note-sparkle" :size="18" /><img src="/assets/mokyu-mochi.svg" alt="" width="124" height="116"><p>{{ t('moreLove') }}</p><span>{{ t('gently') }}</span></div></div>
    </aside>
    <div class="workspace">
      <header class="topbar"><div class="topbar-location"><button class="icon-button mobile-nav" :aria-label="t('navigation')" @click="navigation = true"><Menu :size="21" /></button><span class="location-icon"><component :is="current.icon" :size="18" /></span><span>{{ t(session.role === 'admin' ? current.group : 'workspace') }} <span class="crumb-slash">/</span> <strong>{{ t(current.key) }}</strong></span></div><div class="topbar-actions">
        <LocaleMenu />
        <UiTip :text="t(appearance === 'dark' ? 'lightSwitch' : 'darkSwitch')"><button class="icon-button" :aria-label="t(appearance === 'dark' ? 'lightSwitch' : 'darkSwitch')" @click="toggleTheme"><Sun v-if="appearance === 'dark'" :size="19" /><Moon v-else :size="19" /></button></UiTip>
        <span class="topbar-divider" />
        <UiMenu :label="t('accountMenu')"><template #trigger><Avatar :name="session.display_name || session.username" :src="session.avatar_url" /><span class="account-name">{{ session.display_name || session.username }}</span><ChevronDown :size="14" /></template><DropdownMenuLabel class="menu-label">{{ t('yourWorkspace') }}</DropdownMenuLabel><DropdownMenuItem class="menu-item" as-child><RouterLink to="/account"><Settings2 :size="17" />{{ t('account') }}</RouterLink></DropdownMenuItem><DropdownMenuItem class="menu-item" as-child><RouterLink to="/tokens"><KeyRound :size="17" />{{ t('tokens') }}</RouterLink></DropdownMenuItem><DropdownMenuSeparator class="menu-separator" /><DropdownMenuItem class="menu-item" @select="logout"><LogOut :size="17" />{{ t('signOut') }}</DropdownMenuItem></UiMenu>
      </div></header>
      <main id="main" tabindex="-1"><RouterView :key="session.id" /></main>
      <footer class="app-footer"><span>Mokyu <span class="footer-dot">·</span> {{ t('tagline') }} <Heart :size="13" class="footer-heart" /></span><span>{{ t('cozy') }}</span></footer>
    </div>
  </div>
  <main v-else id="main" tabindex="-1"><RouterView v-if="route.name === 'login'" /></main>
  <UiDialog v-model:open="navigation" :title="t('yourWorkspace')"><nav class="mobile-menu"><template v-for="group in groups" :key="group.name"><p class="nav-label">{{ t(group.name) }}</p><RouterLink v-for="entry in group.entries" :key="entry.key" :to="'/' + entry.key" @click="navigation = false"><component :is="entry.icon" :size="20" />{{ t(entry.key) }}</RouterLink></template><RouterLink to="/tokens" @click="navigation = false"><KeyRound :size="20" />{{ t('tokens') }}</RouterLink></nav></UiDialog>
  <Notices />
  <SecurityPrompt />
</TooltipProvider></template>
