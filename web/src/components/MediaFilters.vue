<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Search, SlidersHorizontal, Images, Film, Music2, FileText, Archive, Sparkles, ArrowDownUp, X } from 'lucide-vue-next'
import { t } from '../app/i18n'
import { parseBytes } from '../api/client'
import UiSelect from './ui/UiSelect.vue'
import UiDialog from './ui/UiDialog.vue'
import UiDate from './ui/UiDate.vue'
import UiCheckbox from './ui/UiCheckbox.vue'

defineProps<{ ready: boolean }>()
const route = useRoute(), router = useRouter(), search = ref(''), match = ref('name'), advanced = ref(false), invalid = ref(false)
const categories = [{ value: '', icon: Sparkles }, { value: 'image', icon: Images }, { value: 'video', icon: Film }, { value: 'audio', icon: Music2 }, { value: 'document', icon: FileText }, { value: 'archive', icon: Archive }]
const sort = computed({ get: () => String(route.query.sort || 'name') + ':' + String(route.query.order || 'asc'), set: v => { const [sort, order] = v.split(':'); update({ sort, order }) } })
function update(changes: Record<string, string | undefined>) { router.push({ query: { ...route.query, ...changes, cursor: undefined } }) }
watch(() => route.query, q => { search.value = String(q.q || ''); match.value = q.mode && q.mode !== 'contains' ? String(q.mode) : String(q.search_in || 'name') }, { immediate: true })
function find() { update({ q: search.value || undefined, mode: ['name', 'path'].includes(match.value) ? 'contains' : match.value, search_in: match.value === 'path' ? 'path' : 'name' }) }
const filtered = computed(() => ['q', 'category', 'public', 'min_size', 'max_size', 'from', 'to', 'recursive', 'sort'].some(k => route.query[k]))
function reset() { router.push({ query: { prefix: route.query.prefix, limit: route.query.limit } }) }
const draft = reactive({ public: 'all', min: '', max: '', unit: '1048576', from: '', to: '', recursive: false })
watch(advanced, open => { if (open) { invalid.value = false; Object.assign(draft, { public: String(route.query.public || 'all'), min: String(route.query.min_size || ''), max: String(route.query.max_size || ''), unit: '1', from: String(route.query.from || ''), to: String(route.query.to || ''), recursive: route.query.recursive === 'true' }) } })
function apply() {
  try {
    const min = parseBytes(draft.min, draft.unit) || undefined, max = parseBytes(draft.max, draft.unit) || undefined
    if (min && max && BigInt(min) > BigInt(max) || draft.from && draft.to && draft.from > draft.to) throw new Error('Invalid range')
    update({ public: draft.public === 'all' ? undefined : draft.public, min_size: min, max_size: max, from: draft.from || undefined, to: draft.to || undefined, recursive: draft.recursive ? 'true' : undefined }); advanced.value = false
  } catch { invalid.value = true }
}
</script>
<template>
  <div class="catalog-controls"><form class="catalog-search" @submit.prevent="find"><Search :size="19" /><input v-model="search" type="search" maxlength="256" :aria-label="t('mediaSearch')" :placeholder="t('mediaSearchHint')"><UiSelect v-model="match" :label="t('searchMatch')" :options="[{ value: 'name', label: t('matchName'), disabled: !ready }, { value: 'path', label: t('matchPath'), disabled: !ready }, { value: 'prefix', label: t('matchPrefix') }, { value: 'exact', label: t('matchExact') }]" /><button type="submit" class="primary" :disabled="!ready && ['name', 'path'].includes(match)">{{ t('search') }}</button></form>
    <div class="catalog-filterbar"><div class="type-chips" :aria-label="t('fileType')"><button v-for="category in categories" :key="category.value" :data-category="category.value" :aria-pressed="String(route.query.category || '') === category.value" :disabled="!ready" @click="update({ category: category.value || undefined })"><component :is="category.icon" :size="15" />{{ t('category_' + (category.value || 'all')) }}</button></div><button :disabled="!ready" @click="advanced = true"><SlidersHorizontal :size="15" />{{ t('moreFilters') }}</button></div>
    <div class="catalog-sort"><span><ArrowDownUp :size="13" />{{ t('mediaSort') }}</span><UiSelect v-model="sort" :label="t('mediaSort')" :disabled="!ready" :options="[{ value: 'name:asc', label: t('sortNameAsc') }, { value: 'name:desc', label: t('sortNameDesc') }, { value: 'modified:desc', label: t('sortNewest') }, { value: 'modified:asc', label: t('sortOldest') }, { value: 'size:desc', label: t('sortLargest') }, { value: 'size:asc', label: t('sortSmallest') }]" /><button v-if="filtered" class="text-button" @click="reset"><X :size="13" />{{ t('clearFilters') }}</button></div>
  </div>
  <UiDialog v-model:open="advanced" :title="t('moreFilters')" :description="t('mediaFilterHint')"><div class="media-filter-grid"><label class="field"><span>{{ t('access') }}</span><UiSelect v-model="draft.public" :label="t('access')" :options="[{ value: 'all', label: t('accessAll') }, { value: 'true', label: t('public') }, { value: 'false', label: t('private') }]" /></label><label class="field"><span>{{ t('quotaUnit') }}</span><UiSelect v-model="draft.unit" :label="t('quotaUnit')" :options="['B', 'KiB', 'MiB', 'GiB', 'TiB'].map((label, n) => ({ label, value: String(1024 ** n) }))" /></label><label class="field"><span>{{ t('minimumSize') }}</span><input v-model="draft.min" inputmode="decimal" placeholder="0"></label><label class="field"><span>{{ t('maximumSize') }}</span><input v-model="draft.max" inputmode="decimal" :placeholder="t('quotaUnlimited')"></label><UiDate v-model="draft.from" :label="t('modifiedFrom')" /><UiDate v-model="draft.to" :label="t('modifiedUntil')" /></div><p class="field-help">{{ t('mediaDatesHint') }}</p><UiCheckbox v-model="draft.recursive" :label="t('includeSubfolders')" /><p v-if="invalid" class="error" role="alert">{{ t('invalidFilterRange') }}</p><template #footer><button @click="advanced = false">{{ t('cancel') }}</button><button class="primary" @click="apply">{{ t('applyFilters') }}</button></template></UiDialog>
</template>
