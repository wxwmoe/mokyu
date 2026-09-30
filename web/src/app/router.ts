import { createRouter, createWebHistory } from 'vue-router'
import { ref } from 'vue'
import { refreshSession, session } from '../api/client'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', name: 'login', component: () => import('../features/Login.vue') },
    { path: '/', redirect: '/media' },
    { path: '/media', name: 'media', component: () => import('../features/Media.vue') },
    { path: '/users', name: 'users', meta: { admin: true }, component: () => import('../features/Users.vue') },
    { path: '/projects', name: 'projects', component: () => import('../features/Projects.vue') },
    { path: '/account', name: 'account', component: () => import('../features/Account.vue') },
    { path: '/media/:bucket', name: 'objects', component: () => import('../features/Media.vue') },
    { path: '/:pathMatch(.*)*', redirect: '/media' },
  ],
  scrollBehavior(to, from, saved) { return saved || (to.path === from.path ? undefined : { top: 0 }) },
})

let initialized = false
export const startupError = ref('')
router.beforeEach(async to => {
  if (!initialized) {
    try { await refreshSession() }
    catch { startupError.value = 'unreachable' }
    initialized = true
  }
  if (!session.value && to.name !== 'login') return { name: 'login' }
  if (session.value?.must_change_password && to.name !== 'account') return { name: 'account' }
  if (to.meta.admin && session.value?.role !== 'admin') return { name: 'media' }
  if (session.value && to.name === 'login') return { name: 'media' }
})
