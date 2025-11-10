<template>
  <aside class="side">
    <div class="side-header">
      <div class="title">Watchlist</div>
      <n-button size="small" type="primary" secondary @click="open = true">
        + Add
      </n-button>
    </div>

    <div class="list">
      <div
        v-for="s in subs"
        :key="s.id"
        class="item"
        :class="{ active: s.id === activeId }"
        @click="$emit('select', s.id)"
      >
        <div class="meta">
          <div class="symbol">{{ labelForSub(s) }}</div>
          <div class="subline">
            {{ s.key.channel ?? 'last' }} • {{ Math.round(s.windowSecs/60) }}m
          </div>
        </div>
        <div class="actions" @click.stop>
          <n-button text quaternary size="tiny" @click="$emit('remove', s.id)">✕</n-button>
        </div>
      </div>

      <div v-if="!subs.length" class="empty">
        Noch keine Subscriptions<br />
        <n-button size="small" tertiary @click="open = true">Jetzt hinzufügen</n-button>
      </div>
    </div>

    <AddSubscriptionDialog
      v-model:open="open"
      :assets="assetOptions"
      :existing="subs"
      :defaults="defaults"
      @create="onCreate"
    />
  </aside>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import type { Subscription } from '../types/trading'
import { NButton, useMessage } from 'naive-ui'
import AddSubscriptionDialog from './dialogs/AddSubscriptionDialog.vue'
import { getAllAssets, labelForKey, type Asset } from '../lib/assets'

const props = defineProps<{
  subs: Subscription[]
  activeId: string
  assets?: Array<Asset>
  defaults?: {
    windowMins?: number
  }
}>()

const emit = defineEmits<{
  (e: 'add', sub: Subscription): void
  (e: 'select', id: string): void
  (e: 'remove', id: string): void
}>()

const message = useMessage()
const open = ref(false)

const assetOptions = computed<Asset[]>(() => props.assets?.length ? props.assets : getAllAssets())

function labelForSub(s: Subscription) {
  return labelForKey(s.key)
}

const defaults = {
  windowMins: props.defaults?.windowMins ?? 60
}

const keyHash = (k: { venueId:string; symbolId:string; channel?:string }) =>
  `${k.venueId}:${k.symbolId}:${k.channel ?? 'last'}`

function onCreate(sub: Subscription) {
  const targetHash = keyHash(sub.key)
  const existing = props.subs.find(x => keyHash(x.key) === targetHash)
  if (existing) {
    emit('select', existing.id)
    message.info('Bereits abonniert – habe die bestehende Subscription aktiviert.')
    return
  }
  emit('add', sub)
  message.success('Subscription hinzugefügt.')
}
</script>

<style scoped>
.side {
  display: grid;
  grid-template-rows: auto 1fr;
  background: #0b0f14;
  border-right: 1px solid #1f2937;
  height: 100%;
}
.side-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: .75rem .75rem;
}
.title { font-weight: 700; color: #e5e7eb; }
.list { overflow: auto; padding: .25rem; }
.item {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: .25rem .5rem;
  padding: .5rem .5rem;
  border-radius: .5rem;
  cursor: pointer;
  color: #cbd5e1;
}
.item:hover { background: #111827; }
.item.active { background: #0f172a; border: 1px solid #1f2937; }
.symbol { color: #e5e7eb; font-weight: 600; }
.subline { opacity: .75; font-size: .8rem; }
.actions { display: flex; align-items: center; }
.empty { margin: 1rem; text-align: center; color: #94a3b8; }
</style>
