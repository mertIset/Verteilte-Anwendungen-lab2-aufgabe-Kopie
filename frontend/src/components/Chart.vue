<template>
  <div ref="wrap" class="chart-root"></div>
</template>

<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref, watch, computed } from 'vue'
import { createChart, CrosshairMode, type IChartApi, type UTCTimestamp } from 'lightweight-charts'

type Candle = {
  bucketStartSec: number
  open: number
  high: number
  low: number
  close: number
  ticks: number
  precision?: number | null
}
type QuotePoint = { tsSec: number; price: number; tickSize?: number; precision?: number | null }

const props = defineProps<{
  mode: 'candles' | 'quote'
  candles: Candle[]
  quotes: QuotePoint[]
}>()

const wrap = ref<HTMLDivElement | null>(null)
let chart: IChartApi | null = null
let firstSet = true

type CandleSeries = ReturnType<IChartApi['addCandlestickSeries']>
type LineSeries   = ReturnType<IChartApi['addLineSeries']>
let candleSeries: CandleSeries | null = null
let lineSeries:   LineSeries   | null = null
let ro: ResizeObserver | null = null

const toSec = (ts: number) => (ts > 1e12 ? Math.floor(ts / 1000) : ts)

function timeToLocal(timestampSec: number): number {
    const d = new Date(timestampSec * 1000);
    return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds(), d.getMilliseconds()) / 1000;
}

const derivedPrecision = computed<number>(() => {

  const lastQ = props.quotes?.[props.quotes.length - 1]
  if (lastQ && typeof lastQ.precision === 'number') return Math.max(0, Math.floor(lastQ.precision))

  const firstC = props.candles?.find(c => typeof c.precision === 'number')
  if (firstC && typeof firstC.precision === 'number') return Math.max(0, Math.floor(firstC.precision!))

  return 2
})

const derivedMinMove = computed<number>(() => {
  const lastQ = props.quotes?.[props.quotes.length - 1]
  if (lastQ && typeof lastQ.tickSize === 'number' && lastQ.tickSize > 0) return lastQ.tickSize

  const p = derivedPrecision.value
  return Number((1 / Math.pow(10, p)).toFixed(p))
})

function applyPriceFormat() {
  const p = derivedPrecision.value
  const mm = derivedMinMove.value
  const fmt = { type: 'price' as const, precision: p, minMove: mm }
  if (props.mode === 'candles' && candleSeries) {
    candleSeries.applyOptions({ priceFormat: fmt })
  } else if (props.mode === 'quote' && lineSeries) {
    lineSeries.applyOptions({ priceFormat: fmt })
  }
}


function setCandleData() {
  if (!candleSeries) return
  const data = (props.candles ?? []).map(c => ({
    time: timeToLocal(toSec(c.bucketStartSec) ) as UTCTimestamp,
    open: c.open, high: c.high, low: c.low, close: c.close
  }))
  candleSeries.setData(data)
  applyPriceFormat()
  if (firstSet) { chart?.timeScale().fitContent(); firstSet = false }
}

function setLineData() {
  if (!lineSeries) return
  const data = (props.quotes ?? []).map(p => ({
    time: p.tsSec as UTCTimestamp,
    value: p.price
  }))
  lineSeries.setData(data)
  applyPriceFormat()
  if (firstSet) { chart?.timeScale().fitContent(); firstSet = false }
}


function ensureSeries() {
  if (!chart) return
  if (candleSeries) { /* @ts-ignore */ chart.removeSeries(candleSeries); candleSeries = null }
  if (lineSeries)   { /* @ts-ignore */ chart.removeSeries(lineSeries);   lineSeries   = null }

  const fmt = { type: 'price' as const, precision: derivedPrecision.value, minMove: derivedMinMove.value }

  if (props.mode === 'candles') {
    candleSeries = chart.addCandlestickSeries({
      upColor: '#16a34a',
      downColor: '#ef4444',
      wickUpColor: '#16a34a',
      wickDownColor: '#ef4444',
      borderVisible: false,
      priceFormat: fmt
    })
    firstSet = true
    setCandleData()
  } else {
    lineSeries = chart.addLineSeries({
      priceLineVisible: true,
      priceFormat: fmt
    })
    firstSet = true
    setLineData()
  }
}

onMounted(() => {
  if (!wrap.value) return
  chart = createChart(wrap.value, {
    layout: { attributionLogo: false, background: { color: '#0b0f14' }, textColor: '#e5e7eb' },
    grid:   { vertLines: { color: '#1f2937' }, horzLines: { color: '#1f2937' } },
    rightPriceScale: { borderVisible: false },
    timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
    crosshair: {
        mode: CrosshairMode.Normal,
      },
  })
  ensureSeries()


  ro = new ResizeObserver(() => {
    if (!wrap.value || !chart) return
    const rect = wrap.value.getBoundingClientRect()
    const w = Math.max(50, Math.floor(rect.width))
    const h = Math.max(50, Math.floor(rect.height))
    chart.applyOptions({ width: w, height: h })
  })
  ro.observe(wrap.value)
})

onBeforeUnmount(() => {
  ro?.disconnect(); ro = null
  chart?.remove(); chart = null; candleSeries = null; lineSeries = null
})


watch(() => props.candles, () => { if (props.mode === 'candles') setCandleData() }, { deep: true })
watch(() => props.quotes,  () => { if (props.mode === 'quote')   setLineData()   }, { deep: true })
watch(() => props.mode, () => ensureSeries())


watch([derivedPrecision, derivedMinMove], () => applyPriceFormat())
</script>

<style scoped>
.chart-root {
  width: 100%;
  height: 100%;
}
</style>
