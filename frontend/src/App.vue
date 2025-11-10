<template>
   <n-message-provider>
    <div class="layout">

      <header class="topbar">
        <div class="brand">Market Viewer</div>
        <n-space style="margin-left:auto;" align="center">
          <n-select v-model:value="mode" :options="modeOpts" size="small" style="width: 140px;" />
          <n-select v-model:value="resSecs" :options="resolutionOpts" size="small" style="width: 140px;" />
          <span class="status">{{ status }}</span>
        </n-space>
      </header>


      <SidebarSubscriptions
        class="side"
        :subs="store.subs"
        :active-id="store.activeId"
        @add="store.addSub"
        @select="store.setActive"
        @remove="store.removeSub"
      />


      <div class="ticker">
        <QuoteTicker :items="tickerItems" :speed-px-per-sec="100" />
      </div>


      <main class="main">
        <div class="chart-host">
          <Chart :mode="mode" :candles="active.candlesAgg" :quotes="active.quotesAgg" />
        </div>
      </main>
    </div>
    </n-message-provider>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useMarketStore } from './store/market'
import Chart from './components/Chart.vue'
import SidebarSubscriptions from './components/SidebarSubscriptions.vue'
import QuoteTicker from './components/QuoteTicker.vue'
import { NSpace, NSelect, NMessageProvider } from 'naive-ui'
import { labelForKey } from './lib/assets'

const store = useMarketStore()
const active = computed(()=>store.active)
const status = computed(()=>store.status)


const tickerItems = computed(() => {
  return store.subs.map(s => {
    const q = store.quotes[s.id]
    return {
      id: s.id,
      label: labelForKey(s.key),
      price: q?.price ?? null,
      abs: q?.abs ?? null,
      rel: q?.rel ?? null,
      tick: q?.tick ?? null,
      tsUnixSec: q?.tsUnixSec ?? null,
      precision: q?.precision ?? null
    }
  })
})

const mode = computed({
  get: () => store.activeMode,
  set: (m: 'candles'|'quote') => store.setActiveMode(m)
})
const modeOpts = [
  { label: 'Candlesticks', value: 'candles' },
  { label: 'Quote (Line)', value: 'quote' }
]

const resSecs = computed({
  get: () => store.activeResolutionSecs,
  set: (v: number) => store.setResolution(v)
})
const resolutionOpts = [
  { label: '1m',  value: 60 },
  { label: '5m',  value: 5*60 },
  { label: '15m', value: 15*60 },
  { label: '30m', value: 30*60 },
  { label: '1h',  value: 60*60 },
  { label: '4h',  value: 4*60*60 },
  { label: '1d',  value: 24*60*60 },
]

onMounted(()=>{
  if (!store.subs.length) {
    store.addSub({ id: 'default', key: { venueId:'98', symbolId:'133979', channel:'bid' }, windowSecs: 3600 })
  }
})
</script>

<style scoped>
.layout {
  display: grid;
  grid-template-columns: 260px 1fr;
  grid-template-rows: 56px auto 1fr;
  grid-template-areas:
    "top  top"
    "side ticker"
    "side main";
  height: 100vh;
  background: #0b0f14;
  overflow-x: hidden;
}
.topbar {
  grid-area: top;
  display: flex;
  align-items: center;
  gap: .75rem;
  padding: 0 .75rem;
  border-bottom: 1px solid #1f2937;
}
.brand { font-weight: 800; color: #e5e7eb; letter-spacing: .3px; }
.status { opacity: .7; color: #cbd5e1; }
.side { grid-area: side; min-width: 0; overflow: hidden; }
.ticker { grid-area: ticker; min-width: 0; overflow: hidden; }
.main {
  grid-area: main;
  padding: .5rem;
  min-height: 0;
  min-width: 0;
  overflow: hidden;
}
.chart-host { position: relative; width: 100%; height: 100%; }
</style>
