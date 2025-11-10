<template>
  <div
    class="ticker-wrap"
    ref="wrap"
    @mouseenter="paused = true"
    @mouseleave="paused = false"
  >
    <div class="rail" :style="railStyle">

      <div
        v-for="n in repeatCount"
        :key="n"
        class="block"
        ref="blocks"
      >
        <template v-for="it in items" :key="it.id + '-' + n">
          <span class="pill">{{ itLabel(it) }}</span>

          <span class="lbl">Price</span><span class="colon">:</span>
          <span class="val big" :class="priceClass(it.id)">
            {{ fmt(it.price, it.precision) }}
          </span>

          <span class="lbl">Rel:</span><span class="colon">:</span>
          <span class="val" :class="chgClass(it.rel)">
            {{ signed(percent(it.rel)) }}
          </span>

          <span class="lbl">Last Update</span><span class="colon">:</span>
          <span class="val mono">{{ timeStr(it.tsUnixSec) }}</span>

          <span class="gap" aria-hidden="true"></span>
        </template>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount, watch, nextTick, reactive } from 'vue'

type TickerItem = {
  id: string
  label?: string
  price: number | null
  abs: number | null
  rel: number | null
  tick?: number | null
  tsUnixSec?: number | null
  precision?: number | null
}

const props = defineProps<{

  items: TickerItem[]

  speedPxPerSec?: number

  gap?: number
}>()


function fmt(v?: number | null, precision?: number | null) {
  if (v === undefined || v === null) return '–'
  const p = typeof precision === 'number' && precision >= 0 ? Math.floor(precision) : 2
  return v.toFixed(p)
}
function percent(rel?: number | null) {
  if (rel === undefined || rel === null) return '–'
  const val = rel * 100
  return `${val >= 0 ? '+' : ''}${val.toFixed(2)}%`
}
function signed(s: string) {
  if (s === '–') return s
  return s.startsWith('-') || s.startsWith('+') ? s : `+${s}`
}
function chgClass(rel?: number | null) {
  if (rel === undefined || rel === null) return ''
  return rel > 0 ? 'up' : rel < 0 ? 'down' : ''
}
function timeStr(tsSec?: number | null) {
  if (!tsSec) return '–'
  const d = new Date(tsSec * 1000)
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}
function itLabel(it: TickerItem) {
  return it.label ?? it.id
}


const lastPrice = reactive(new Map<string, number>())
const flash = reactive(new Map<string, 'flash-up' | 'flash-down' | ''>())
const timers = new Map<string, number>()

function priceClass(id: string) {
  return flash.get(id) || ''
}

function handlePriceChange() {
  for (const it of props.items) {
    if (it.price == null) continue
    const prev = lastPrice.get(it.id)
    if (prev == null) {
      lastPrice.set(it.id, it.price)
      continue
    }
    if (it.price !== prev) {
      const dir: 'flash-up' | 'flash-down' = it.price > prev ? 'flash-up' : 'flash-down'
      flash.set(it.id, dir)
      lastPrice.set(it.id, it.price)
      if (timers.has(it.id)) window.clearTimeout(timers.get(it.id)!)
      const t = window.setTimeout(() => { flash.set(it.id, '') }, 600)
      timers.set(it.id, t)
    }
  }
}


const wrap = ref<HTMLDivElement | null>(null)
const blocks = ref<HTMLDivElement[]>([])

const paused = ref(false)
const offset = ref(0)
let speed = 160
let blockWidth = 0
let wrapWidth = 0
let lastTs = 0
let rafId = 0
let ro: ResizeObserver | null = null

const GAP = computed(() => props.gap ?? 64)


const railStyle = computed(() => ({
  transform: `translateX(${-Math.round(offset.value)}px)`,
  '--gap': `${GAP.value}px`
} as any))


const repeatCount = ref(3)

async function measure() {
  await nextTick()
  const first = blocks.value?.[0]
  blockWidth = first?.getBoundingClientRect().width ?? 0
  wrapWidth  = wrap.value?.getBoundingClientRect().width ?? 0
  if (!blockWidth || !wrapWidth) return

  const minRail = wrapWidth * 2 + blockWidth
  const count = Math.max(2, Math.ceil(minRail / blockWidth))
  repeatCount.value = count

  offset.value = blockWidth ? (offset.value % blockWidth) : 0
}

function step(ts: number) {
  if (lastTs === 0) lastTs = ts
  const dt = Math.max(0, (ts - lastTs) / 1000)
  lastTs = ts
  if (!paused.value && blockWidth > 0) {
    offset.value += speed * dt
    if (offset.value >= blockWidth) {
      offset.value = offset.value % blockWidth
    }
  }
  rafId = requestAnimationFrame(step)
}

onMounted(async () => {
  speed = typeof props.speedPxPerSec === 'number' ? props.speedPxPerSec! : 60
  await measure()
  ro = new ResizeObserver(() => measure())
  if (wrap.value) ro.observe(wrap.value)
  watch(blocks, () => measure(), { deep: true })
  lastTs = 0
  rafId = requestAnimationFrame(step)
  handlePriceChange()
})

onBeforeUnmount(() => {
  if (rafId) cancelAnimationFrame(rafId)
  ro?.disconnect()
  for (const t of timers.values()) window.clearTimeout(t)
})

watch(
  () => [props.items.map(i => [i.id, i.price, i.abs, i.rel, i.tick, i.precision]), GAP.value],
  () => { measure(); handlePriceChange() },
  { deep: true }
)
</script>

<style scoped>
.ticker-wrap {
  position: relative;
  overflow: hidden;
  background: linear-gradient(90deg, #0b0f14, #0b0f14), #0b0f14;
  border-top: 1px solid #1f2937;
  border-bottom: 1px solid #1f2937;
  min-height: 38px;
}


.rail {
  display: inline-flex;
  will-change: transform;
}


.block {
  display: inline-flex;
  align-items: baseline;
  white-space: nowrap;
}


.gap {
  display: inline-block;
  width: var(--gap, 64px);
}


.lbl { opacity: .75; margin-left: .4rem; }
.colon { opacity: .55; padding: 0 .25rem 0 .2rem; }
.val { font-weight: 600; }
.big { font-size: 1.1rem; }
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
    "Liberation Mono", "Courier New", monospace;
}
.pill {
  background: #0f172a;
  border: 1px solid #1f2937;
  padding: .1rem .5rem;
  border-radius: 999px;
  font-weight: 700;
  color: #cbd5e1;
}


.up   { color: #22c55e; }
.down { color: #ef4444; }

.flash-up   { color: #22c55e; animation: flashUp .6s ease-out; }
.flash-down { color: #ef4444; animation: flashDown .6s ease-out; }
@keyframes flashUp   { 0% { color: #22c55e; } 100% { color: #e5e7eb; } }
@keyframes flashDown { 0% { color: #ef4444; } 100% { color: #e5e7eb; } }
</style>
