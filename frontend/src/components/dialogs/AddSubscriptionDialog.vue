<template>
  <n-modal
    v-model:show="open"
    preset="card"
    title="Add Subscription"
    :style="{ width: '520px' }"
  >
    <n-form label-placement="top" :show-require-mark="false">
      <n-form-item label="Asset">
        <n-select
          v-model:value="assetKey"
          :options="assetOptionsComputed"
          placeholder="Asset auswählen"
          filterable
        />
      </n-form-item>

      <n-form-item label="Time Window (lookup period in minutes)">
        <n-select
          v-model:value="windowMins"
          :options="windowOptions"
          style="width: 160px"
        />
      </n-form-item>

      <n-alert
        v-if="isDuplicate"
        type="warning"
        title="Aliready Subscribed"
        :bordered="false"
      >
        This subscription already exists.
      </n-alert>
    </n-form>

    <template #footer>
      <div class="footer">
        <n-button tertiary @click="open = false">Cancel</n-button>
        <n-button
          type="primary"
          :disabled="!assetKey || isDuplicate"
          @click="onCreate"
        >
          Add
        </n-button>
      </div>
    </template>
  </n-modal>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import type { Subscription } from '../../types/trading'
import { NModal, NSelect, NButton, NAlert, NForm, NFormItem, NTag } from 'naive-ui'
import { nanoid } from 'nanoid/non-secure'
import type { Asset } from '../../lib/assets'

const props = defineProps<{
  open: boolean
  assets: Array<Asset>
  existing?: Subscription[]
  defaults?: {
    windowMins?: number
  }
}>()
const emit = defineEmits<{
  (e: 'update:open', v: boolean): void
  (e: 'create', sub: Subscription): void
}>()

const open = computed({
  get: () => props.open,
  set: (v: boolean) => emit('update:open', v)
})

const windowOptions = [
  { label: '30 Minuten', value: 30 },
  { label: '60 Minuten', value: 60 },
  { label: '240 Minuten', value: 240 },
]

const assetKey = ref<string>('')
const windowMins = ref<number>(props.defaults?.windowMins ?? 60)

const existingHashes = computed(() => {
  const set = new Set<string>()
  for (const s of (props.existing ?? [])) {
    set.add(`${s.key.venueId}:${s.key.symbolId}:${s.key.channel ?? 'last'}`)
  }
  return set
})

const assetOptionsComputed = computed(() =>
  props.assets.map(a => {
    const value = `${a.venueId}:${a.symbolId}:${a.channel ?? 'last'}`
    return {
      label: a.label,
      value,
      disabled: existingHashes.value.has(value)
    }
  })
)

const selectedChannel = computed(() => {
  if (!assetKey.value) return ''
  const parts = assetKey.value.split(':')
  return parts[2] ?? 'last'
})

const isDuplicate = computed(() => !!assetKey.value && existingHashes.value.has(assetKey.value))

function onCreate() {
  if (!assetKey.value) return
  const [venueId, symbolId, channel] = assetKey.value.split(':')
  if (!venueId || !symbolId) return
  const sub: Subscription = {
    id: nanoid(6),
    key: { venueId, symbolId, channel: channel || 'last' },
    windowSecs: windowMins.value * 60,
  }
  emit('create', sub)
  open.value = false
}
</script>

<style scoped>
.footer {
  display: flex;
  gap: .5rem;
  justify-content: flex-end;
}
.hint {
  margin: -.5rem 0 .25rem 0;
  color: #cbd5e1;
  font-size: .875rem;
  display: flex;
  align-items: center;
  gap: .5rem;
}
</style>
