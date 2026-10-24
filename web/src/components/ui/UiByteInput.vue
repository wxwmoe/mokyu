<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { byteSize, parseByteSize } from '../../api/client'
import { t, locale } from '../../app/i18n'
defineOptions({ inheritAttrs: false })
const model = defineModel<number | null>({ required: true })
const input = ref<HTMLInputElement>()
const text = ref(model.value == null ? '' : byteSize(model.value))
const parsed = computed(() => {
  try { return text.value.trim() ? parseByteSize(text.value) : null }
  catch { return null }
})
watch(model, value => { if (value !== parsed.value) text.value = value == null ? '' : byteSize(value) })
function validate() {
  const invalid = text.value.trim() !== '' && parsed.value === null
  input.value?.setCustomValidity(invalid ? t('invalidByteSize') : '')
}
watch([text, locale], validate, { flush: 'post' })
function update() { model.value = parsed.value; validate() }
function normalize() { if (parsed.value !== null) text.value = byteSize(parsed.value) }
</script>
<template>
  <input ref="input" v-bind="$attrs" v-model="text" type="text" inputmode="text" spellcheck="false" autocomplete="off" @input="update" @blur="normalize">
</template>
