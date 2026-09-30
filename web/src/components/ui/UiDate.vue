<script setup lang="ts">
import { computed, useId } from 'vue'
import { parseDate } from '@internationalized/date'
import { DatePickerRoot, DatePickerField, DatePickerInput, DatePickerTrigger, DatePickerContent, DatePickerCalendar, DatePickerHeader, DatePickerPrev, DatePickerHeading, DatePickerNext, DatePickerGrid, DatePickerGridHead, DatePickerGridRow, DatePickerHeadCell, DatePickerGridBody, DatePickerCell, DatePickerCellTrigger } from 'reka-ui'
import { CalendarDays, ChevronLeft, ChevronRight, X } from 'lucide-vue-next'
import { t, locale } from '../../app/i18n'
defineProps<{ label: string }>()
const value = defineModel<string>({ required: true }), id = useId()
const date = computed({ get: () => { try { return value.value ? parseDate(value.value) : undefined } catch { return undefined } }, set: v => { value.value = v?.toString() || '' } })
</script>
<template>
  <div class="date-control"><span :id="id" class="field-label">{{ label }}</span><DatePickerRoot v-model="date" :locale="locale" :fixed-weeks="true" :close-on-select="true" :aria-labelledby="id" class="date-root">
    <DatePickerField v-slot="{ segments }" class="date-field"><template v-for="segment in segments" :key="segment.part"><span v-if="segment.part === 'literal'" class="date-literal">{{ segment.value }}</span><DatePickerInput v-else :part="segment.part" class="date-segment">{{ segment.value }}</DatePickerInput></template><DatePickerTrigger class="icon-button" :aria-label="t('chooseDate', { label })"><CalendarDays :size="16" /></DatePickerTrigger><button v-if="value" type="button" class="icon-button" :aria-label="t('clearDate', { label })" @click="value = ''"><X :size="13" /></button></DatePickerField>
    <DatePickerContent class="popover calendar-popover" :side-offset="6" :collision-padding="12"><DatePickerCalendar v-slot="{ grid, weekDays }" class="calendar"><DatePickerHeader class="calendar-header"><DatePickerPrev class="icon-button" :aria-label="t('previousMonth')"><ChevronLeft :size="16" /></DatePickerPrev><DatePickerHeading /><DatePickerNext class="icon-button" :aria-label="t('nextMonth')"><ChevronRight :size="16" /></DatePickerNext></DatePickerHeader><DatePickerGrid v-for="month in grid" :key="month.value.toString()"><DatePickerGridHead><DatePickerGridRow><DatePickerHeadCell v-for="day in weekDays" :key="day">{{ day }}</DatePickerHeadCell></DatePickerGridRow></DatePickerGridHead><DatePickerGridBody><DatePickerGridRow v-for="(week, index) in month.rows" :key="index"><DatePickerCell v-for="day in week" :key="day.toString()" :date="day"><DatePickerCellTrigger :day="day" :month="month.value" /></DatePickerCell></DatePickerGridRow></DatePickerGridBody></DatePickerGrid></DatePickerCalendar></DatePickerContent>
  </DatePickerRoot></div>
</template>
