<script setup lang="ts">
import { ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { TooltipProvider, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator } from 'reka-ui'
import { Heart, Sparkles, Images, Sun, Moon, Menu, LogOut, ArrowUpRight, ChevronDown, Settings2, Users, FolderHeart, KeyRound, BookHeart } from 'lucide-vue-next'
import { session, signOut, savePreferences } from '../api/client'
import { appearance, setTheme } from './theme'
import { report } from './feedback'
import { startupError } from './router'
import Notices from '../components/Notices.vue'
import SecurityPrompt from '../components/SecurityPrompt.vue'
import UiMenu from '../components/ui/UiMenu.vue'
import UiTip from '../components/ui/UiTip.vue'
import UiDialog from '../components/ui/UiDialog.vue'
import LocaleMenu from '../components/LocaleMenu.vue'
import Avatar from '../components/Avatar.vue'
import { t } from './i18n'
const router = useRouter(), route = useRoute(), navigation = ref(false)
async function logout() { try { await signOut(); await router.replace('/login') } catch (error) { report(error) } }
function reload() { location.reload() }
async function toggleTheme() { try { const value = appearance.value === 'dark' ? 'light' : 'dark'; if (session.value) await savePreferences({ theme: value }); else setTheme(value) } catch (error) { report(error) } }
</script>

<template><TooltipProvider :delay-duration="350">
  <a href="#main" class="skip-link">{{ t('skip') }}</a>
  <div v-if="startupError" class="error startup-error" role="alert">{{ t(startupError) }} <button @click="reload">{{ t('tryAgain') }}</button></div>
  <div v-if="session" class="app-shell">
    <aside class="sidebar">
      <RouterLink class="brand" to="/"><img src="/assets/mokyu-icon.svg" alt="" width="48" height="48"><span>Mokyu<small>{{ t('brandNote') }}</small></span></RouterLink>
      <p class="nav-label">{{ t('workspace') }}</p>
      <nav :aria-label="t('workspace')"><RouterLink to="/media"><Images :size="19" />{{ t('media') }}<span class="nav-dot" /></RouterLink><RouterLink v-if="session.role !== 'admin'" to="/audit"><BookHeart :size="19" />{{ t('audit') }}</RouterLink><RouterLink v-if="session.role !== 'admin' && session.project_management" to="/projects"><FolderHeart :size="19" />{{ t('projects') }}</RouterLink></nav>
      <template v-if="session.role === 'admin'"><p class="nav-label">{{ t('administration') }}</p><nav :aria-label="t('administration')"><RouterLink to="/audit"><BookHeart :size="19" />{{ t('audit') }}</RouterLink><RouterLink to="/credentials"><KeyRound :size="19" />{{ t('credentials') }}</RouterLink><RouterLink to="/users"><Users :size="19" />{{ t('users') }}</RouterLink><RouterLink v-if="session.project_management" to="/projects"><FolderHeart :size="19" />{{ t('projects') }}</RouterLink></nav></template>
      <div class="sidebar-bottom"><div class="mochi-note"><Sparkles class="note-sparkle" :size="18" /><img src="/assets/mokyu-mochi.svg" alt="" width="124" height="116"><p>{{ t('moreLove') }}</p><span>{{ t('gently') }}</span></div></div>
    </aside>
    <div class="workspace">
      <header class="topbar"><div class="topbar-location"><button class="icon-button mobile-nav" :aria-label="t('navigation')" @click="navigation = true"><Menu :size="21" /></button><span class="location-icon"><Images :size="18" /></span><span>{{ t('workspace') }} <span class="crumb-slash">/</span> <strong>{{ t(['account', 'users', 'projects', 'tokens', 'credentials', 'audit'].includes(String(route.name)) ? String(route.name) : 'media') }}</strong></span></div><div class="topbar-actions">
        <LocaleMenu />
        <UiTip :text="t(appearance === 'dark' ? 'lightSwitch' : 'darkSwitch')"><button class="icon-button" :aria-label="t(appearance === 'dark' ? 'lightSwitch' : 'darkSwitch')" @click="toggleTheme"><Sun v-if="appearance === 'dark'" :size="19" /><Moon v-else :size="19" /></button></UiTip>
        <span class="topbar-divider" />
        <UiMenu :label="t('accountMenu')"><template #trigger><Avatar :name="session.display_name || session.username" :src="session.avatar_url" /><span class="account-name">{{ session.display_name || session.username }}</span><ChevronDown :size="14" /></template><DropdownMenuLabel class="menu-label">{{ t('yourWorkspace') }}</DropdownMenuLabel><DropdownMenuItem class="menu-item" as-child><RouterLink to="/account"><Settings2 :size="17" />{{ t('account') }}</RouterLink></DropdownMenuItem><DropdownMenuItem class="menu-item" as-child><RouterLink to="/tokens"><KeyRound :size="17" />{{ t('tokens') }}</RouterLink></DropdownMenuItem><DropdownMenuItem v-if="session.role === 'admin'" class="menu-item" as-child><a href="/classic/"><ArrowUpRight :size="17" />{{ t('classic') }}</a></DropdownMenuItem><DropdownMenuSeparator class="menu-separator" /><DropdownMenuItem class="menu-item" @select="logout"><LogOut :size="17" />{{ t('signOut') }}</DropdownMenuItem></UiMenu>
      </div></header>
      <main id="main" tabindex="-1"><RouterView /></main>
      <footer class="app-footer"><span>Mokyu <span class="footer-dot">·</span> {{ t('tagline') }} <Heart :size="13" class="footer-heart" /></span><span>{{ t('cozy') }}</span></footer>
    </div>
  </div>
  <main v-else id="main" tabindex="-1"><RouterView /></main>
  <UiDialog v-model:open="navigation" :title="t('yourWorkspace')"><nav class="mobile-menu"><RouterLink to="/media" @click="navigation = false"><Images :size="20" />{{ t('media') }}</RouterLink><RouterLink to="/audit" @click="navigation = false"><BookHeart :size="20" />{{ t('audit') }}</RouterLink><RouterLink to="/tokens" @click="navigation = false"><KeyRound :size="20" />{{ t('tokens') }}</RouterLink><RouterLink v-if="session?.role === 'admin'" to="/credentials" @click="navigation = false"><KeyRound :size="20" />{{ t('credentials') }}</RouterLink><RouterLink v-if="session?.role === 'admin'" to="/users" @click="navigation = false"><Users :size="20" />{{ t('users') }}</RouterLink><RouterLink v-if="session?.project_management" to="/projects" @click="navigation = false"><FolderHeart :size="20" />{{ t('projects') }}</RouterLink></nav></UiDialog>
  <Notices />
  <SecurityPrompt />
</TooltipProvider></template>
