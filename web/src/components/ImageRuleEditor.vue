<script setup lang="ts">
import { computed } from 'vue'
import { ArrowDown, ArrowUp, Crop, Film, Layers, Maximize2, PaintBucket, Plus, RotateCw, Trash2 } from 'lucide-vue-next'
import type { components } from '../api/schema'
import { t } from '../app/i18n'
import UiSelect from './ui/UiSelect.vue'
import UiCheckbox from './ui/UiCheckbox.vue'
type Pipeline = components['schemas']['Pipeline']
type Stage = Pipeline['stages'][number]
type Operation = Stage['operations'][number]
const rule = defineModel<Pipeline>({ required: true })
const props = defineProps<{ disabled?: boolean }>()
const total = computed(() => rule.value.stages.reduce((n,s) => n+s.operations.length+1,0))
const choices = (values: readonly string[], prefix: string) => values.map(value=>({value,label:t(prefix+value)}))
const formats = computed(()=>choices(['source','auto','jpeg','png','webp','jxl','avif'],'imageFormat_'))
const qualities = computed(()=>choices(['source','exact','cap','relative'],'imageQuality_'))
const modes = computed(()=>choices(['contain','cover'],'imageFit_'))
const anchors = computed(()=>choices(['center','north','south','east','west','northwest','northeast','southwest','southeast'],'imageAnchor_'))
const operations = ['orient','resize','crop','background','frame'] as const
const icons = { orient: RotateCw, resize: Maximize2, crop: Crop, background: PaintBucket, frame: Film }
function operation(stage: Stage, name: (typeof operations)[number]) {
  if(total.value>=24)return
  const defaults: Record<(typeof operations)[number],Operation> = {
    orient:{operation:'orient'},frame:{operation:'frame',index:0},
    resize:{operation:'resize',mode:'contain',width:300,height:300,upscale:false},
    crop:{operation:'crop',width:400,height:400,anchor:'center'},
    background:{operation:'background',color:'ffffff'},
  }
  stage.operations.push(defaults[name])
}
function move<T>(rows:T[], index:number, delta:number) {
  const other=index+delta
  if(other>=0 && other<rows.length)[rows[index],rows[other]]=[rows[other],rows[index]]
}
function moveStage(index:number, delta:number) {
  const rows=rule.value.stages, other=index+delta
  if(other<0||other>=rows.length||rows[index]?.format==='auto'||rows[other]?.format==='auto')return
  move(rows,index,delta)
}
function addStage() {
  if(rule.value.stages.length>=8||total.value>=24)return
  const last=rule.value.stages.at(-1)
  if(last?.format==='auto')last.format='source'
  rule.value.stages.push({operations:[],format:'source',quality:null,quality_mode:'source',progressive:false})
}
function quality(stage:Stage, mode:string) {
  stage.quality_mode=mode as Stage['quality_mode']
  stage.quality=mode==='source'?null:stage.quality||90
}
function format(stage:Stage, value:string) {
  stage.format=value as Stage['format']
  if(!['source','auto','jpeg'].includes(value))stage.progressive=false
}
function dimension(event:Event) {const value=(event.target as HTMLInputElement).value;return value?Number(value):null}
</script>
<template>
  <fieldset :disabled="props.disabled" class="form-stack image-rule-editor">
    <section v-for="(stage,index) in rule.stages" :key="index" class="form-stack image-stage">
      <header class="stage-heading">
        <h3><span class="chunk-bead rose"><Layers :size="17" aria-hidden="true" /></span>{{ t('imageStage',{count:index+1}) }}</h3>
        <div class="stage-actions">
          <button type="button" class="icon-button" :disabled="!index || stage.format==='auto'" :aria-label="t('imageMoveUp')" :title="t('imageMoveUp')" @click="moveStage(index,-1)"><ArrowUp :size="16" /></button>
          <button type="button" class="icon-button" :disabled="index===rule.stages.length-1 || rule.stages[index+1]?.format==='auto'" :aria-label="t('imageMoveDown')" :title="t('imageMoveDown')" @click="moveStage(index,1)"><ArrowDown :size="16" /></button>
          <button type="button" class="icon-button" :disabled="rule.stages.length===1" :aria-label="t('imageRemoveStage')" :title="t('imageRemoveStage')" @click="rule.stages.splice(index,1)"><Trash2 :size="16" /></button>
        </div>
      </header>
      <section v-for="(op,position) in stage.operations" :key="position" class="form-stack image-operation">
        <header class="stage-heading">
          <h4><component :is="icons[op.operation]" :size="16" aria-hidden="true" />{{ t('imageOperation_'+op.operation) }}</h4>
          <div class="stage-actions">
            <button type="button" class="icon-button" :disabled="!position" :aria-label="t('imageMoveUp')" :title="t('imageMoveUp')" @click="move(stage.operations,position,-1)"><ArrowUp :size="15" /></button>
            <button type="button" class="icon-button" :disabled="position===stage.operations.length-1" :aria-label="t('imageMoveDown')" :title="t('imageMoveDown')" @click="move(stage.operations,position,1)"><ArrowDown :size="15" /></button>
            <button type="button" class="icon-button" :aria-label="t('imageRemoveOperation')" :title="t('imageRemoveOperation')" @click="stage.operations.splice(position,1)"><Trash2 :size="15" /></button>
          </div>
        </header>
        <template v-if="op.operation==='resize'">
          <label class="field"><span>{{ t('imageFit') }}</span><UiSelect v-model="op.mode" :label="t('imageFit')" :options="modes" :disabled="props.disabled"/></label>
          <div class="form-pair"><label class="field"><span>{{ t('imageWidth') }}</span><input :value="op.width" type="number" min="1" max="32768" step="1" :placeholder="t('imageUnconstrained')" @input="op.width=dimension($event)"></label><label class="field"><span>{{ t('imageHeight') }}</span><input :value="op.height" type="number" min="1" max="32768" step="1" :placeholder="t('imageUnconstrained')" @input="op.height=dimension($event)"></label></div>
          <UiCheckbox v-model="op.upscale" :disabled="props.disabled" :label="t('imageUpscale')"/>
        </template>
        <template v-else-if="op.operation==='crop'">
          <div class="form-pair"><label class="field"><span>{{ t('imageWidth') }}</span><input v-model.number="op.width" required type="number" min="1" max="32768" step="1"></label><label class="field"><span>{{ t('imageHeight') }}</span><input v-model.number="op.height" required type="number" min="1" max="32768" step="1"></label></div>
          <label class="field"><span>{{ t('imageAnchor') }}</span><UiSelect v-model="op.anchor" :label="t('imageAnchor')" :options="anchors" :disabled="props.disabled"/></label>
        </template>
        <label v-else-if="op.operation==='background'" class="field"><span>{{ t('imageBackground') }}</span>
          <span class="color-picker">
            <span class="color-chip" :style="{ backgroundColor: '#'+op.color }" aria-hidden="true"></span>
            <code aria-hidden="true">#{{ op.color.toUpperCase() }}</code>
            <input type="color" :aria-label="t('imageBackground')" :value="'#'+op.color" @input="op.color=($event.target as HTMLInputElement).value.slice(1)">
          </span>
        </label>
        <label v-else-if="op.operation==='frame'" class="field"><span>{{ t('imageFrameIndex') }}</span><input v-model.number="op.index" type="number" min="0" max="255" step="1" required></label>
      </section>
      <div class="operation-picker">
        <strong>{{ t('imageAddOperation') }}</strong>
        <div class="actions"><button v-for="name in operations" :key="name" type="button" :disabled="total>=24" @click="operation(stage,name)"><component :is="icons[name]" :size="15" aria-hidden="true" />{{ t('imageOperation_'+name) }}</button></div>
      </div>
      <div class="stage-output form-stack">
        <h4>{{ t('imageEncoding') }}</h4>
        <div class="form-pair"><label class="field"><span>{{ t('imageOutput') }}</span><UiSelect :model-value="stage.format" :label="t('imageOutput')" :options="formats.map(v=>({...v,disabled:v.value==='auto' && index!==rule.stages.length-1}))" :disabled="props.disabled" @update:model-value="format(stage,$event)"/></label><label class="field"><span>{{ t('imageQualityMode') }}</span><UiSelect :model-value="stage.quality_mode" :label="t('imageQualityMode')" :options="qualities" :disabled="props.disabled" @update:model-value="quality(stage,$event)"/></label></div>
        <label v-if="stage.quality_mode!=='source'" class="field"><span>{{ t('imageQualityValue') }}</span><input v-model.number="stage.quality" type="number" min="1" max="100" step="1" required></label>
        <UiCheckbox v-if="['source','auto','jpeg'].includes(stage.format)" v-model="stage.progressive" :disabled="props.disabled" :label="t('imageProgressive')"/>
      </div>
    </section>
    <div class="stage-footer">
      <button type="button" :disabled="rule.stages.length>=8||total>=24" @click="addStage"><Plus :size="16"/>{{ t('imageAddStage') }}</button>
      <p class="field-help">{{ t('imageRuleHint') }}</p>
    </div>
  </fieldset>
</template>
<style scoped>
.image-stage { padding: 16px; gap: 12px; border: 1px solid var(--line); border-radius: 18px; background: var(--surface-soft); }
.stage-heading, .stage-heading h3, .stage-heading h4, .stage-actions { display: flex; align-items: center; }
.stage-heading { justify-content: space-between; flex-wrap: wrap; gap: 4px 12px; }
.stage-heading h3, .stage-heading h4 { gap: 8px; }
.stage-heading h4 svg { color: var(--rose); }
.stage-actions { gap: 4px; margin-inline-start: auto; }
.stage-actions button:last-child:not(:disabled):hover { color: var(--danger); background: var(--danger-soft); }
.image-operation { padding: 10px 14px; gap: 12px; border-radius: 12px; background: var(--surface); }
.operation-picker { display: grid; gap: 8px; padding: 4px 0; }
.operation-picker > strong, .stage-output h4 { font-size: var(--small-size, 12px); }
.operation-picker button { padding: 7px 10px; min-height: 36px; }
.operation-picker button svg { color: var(--rose); }
.stage-output { padding: 14px; gap: 12px; border-radius: 12px; background: var(--rose-soft); }
.stage-footer { display: flex; align-items: center; flex-wrap: wrap; gap: 8px 14px; }
.stage-footer .field-help { flex: 1 1 200px; }
.color-picker { position: relative; display: inline-flex; justify-self: start; align-items: center; gap: 10px; min-height: 42px; padding: 6px 12px 6px 7px; border: 1px solid var(--line); border-radius: 11px; background: var(--surface); }
.color-picker:focus-within { outline: 2px solid var(--rose); outline-offset: 2px; }
.color-chip { width: 26px; height: 26px; border-radius: 7px; border: 1px solid color-mix(in srgb, var(--ink) 30%, transparent); }
.color-picker code { font-variant-numeric: tabular-nums; }
.color-picker input[type='color'] { position: absolute; inset: 0; width: 100%; height: 100%; padding: 0; opacity: 0; cursor: pointer; }
@media (max-width: 480px) { .image-stage { padding: 12px; } .image-operation, .stage-output { padding: 10px; } }
</style>
