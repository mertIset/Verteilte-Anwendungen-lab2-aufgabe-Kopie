import { defineStore } from "pinia";
import type { Candle, Subscription, Quote } from "../types/trading";
import { WSClient } from "../services/ws";

const toSec = (ts: number) => (ts > 1e12 ? Math.floor(ts / 1000) : ts);

const MAX_CANDLE_BUCKETS = 10000;
const MAX_QUOTE_POINTS = 20000;

function aggregateCandles(candles: Candle[], bucketSecs: number): Candle[] {
  if (!candles?.length || bucketSecs <= 60) return candles;
  const out: Candle[] = [];
  let curBucketStart = -1;
  let agg: Candle | null = null;

  for (const c of candles) {
    const t = toSec(c.bucketStartSec);
    const b = Math.floor(t / bucketSecs) * bucketSecs;
    if (b !== curBucketStart) {
      if (agg) out.push(agg);
      curBucketStart = b;
      agg = {
        bucketStartSec: b,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
        ticks: (c as any).ticks ?? 0,
      };
    } else {
      if (!agg) continue;
      if (c.high > agg.high) agg.high = c.high;
      if (c.low < agg.low) agg.low = c.low;
      agg.close = c.close;
      (agg as any).ticks = ((agg as any).ticks ?? 0) + ((c as any).ticks ?? 0);
    }
  }
  if (agg) out.push(agg);
  return out;
}

type QuotePt = { tsSec: number; price: number };
function aggregateQuotes(points: QuotePt[], bucketSecs: number): QuotePt[] {
  if (!points?.length || bucketSecs <= 1) return points;
  const out: QuotePt[] = [];
  let curB = -1;
  let lastInBucket: QuotePt | null = null;
  for (const p of points) {
    const b = Math.floor(p.tsSec / bucketSecs) * bucketSecs;
    if (b !== curB) {
      if (lastInBucket) out.push({ tsSec: curB, price: lastInBucket.price });
      curB = b;
      lastInBucket = { tsSec: p.tsSec, price: p.price };
    } else {
      lastInBucket = p;
    }
  }
  if (lastInBucket) out.push({ tsSec: curB, price: lastInBucket.price });
  return out;
}

export const useMarketStore = defineStore("market", {
  state: () => ({
    subs: [] as Subscription[],
    activeId: "" as string,

    data: {} as Record<string, Candle[]>,
    quotes: {} as Record<string, Quote | undefined>,
    quoteHist: {} as Record<string, Array<{ tsSec: number; price: number }>>,

    status: "" as string | "",
    ws: null as null | WSClient,

    activeMode: "candles" as "candles" | "quote",
    activeResolutionSecs: 60,
  }),
  getters: {
    active(state) {
      const sub = state.subs.find((s) => s.id === state.activeId) || null;
      const rawCandles = sub ? state.data[sub.id] ?? [] : [];
      const rawQuote = sub ? state.quotes[sub.id] : undefined;
      const rawQuotes = sub ? state.quoteHist[sub.id] ?? [] : [];

      const res = state.activeResolutionSecs;

      const candlesAgg = aggregateCandles(rawCandles, res);
      const quotesAgg = aggregateQuotes(rawQuotes, res);

      return {
        sub,
        quote: rawQuote,
        candlesAgg,
        quotesAgg,
        mode: state.activeMode,
      };
    },
  },
  actions: {
    ensureWS() {
      if (!this.ws) {
        const WS_BASE =
          import.meta.env.VITE_WS_URL ?? "ws://localhost:8080/quotes";
        this.ws = new WSClient(
          WS_BASE,
          (id, payload) => this.upsertCandles(id, payload),
          (id, payload) => this.upsertQuote(id, payload),
          (s) => (this.status = s)
        );
      }
      this.ws.connect();
    },

    upsertCandles(id: string, payload: Candle[] | Candle) {
      const arr = (Array.isArray(payload) ? payload : [payload]).map((c) => ({
        ...c,
        bucketStartSec: toSec(c.bucketStartSec),
      }));

      const cur = this.data[id] ?? (this.data[id] = []);
      const idx = new Map<number, number>();
      for (let i = 0; i < cur.length; i++) idx.set(cur[i]!.bucketStartSec, i);

      for (const c of arr) {
        const pos = idx.get(c.bucketStartSec);
        if (pos !== undefined) cur[pos] = c;
        else {
          cur.push(c);
          idx.set(c.bucketStartSec, cur.length - 1);
        }
      }

      cur.sort((a, b) => a.bucketStartSec - b.bucketStartSec);

      const sub = this.subs.find((s) => s.id === id);
      if (sub && cur.length) {
        const lastSec = cur[cur.length - 1]!.bucketStartSec;
        const minSec = lastSec - sub.windowSecs;
        let firstKeep = 0;
        while (
          firstKeep < cur.length &&
          cur[firstKeep]!.bucketStartSec < minSec
        )
          firstKeep++;
        if (firstKeep > 0) cur.splice(0, firstKeep);
      }

      if (cur.length > MAX_CANDLE_BUCKETS)
        cur.splice(0, cur.length - MAX_CANDLE_BUCKETS);
      this.data[id] = cur;
    },

    upsertQuote(id: string, qOrList: Quote | Quote[]) {
      const list = Array.isArray(qOrList) ? qOrList : [qOrList];

      const last = list[list.length - 1];
      if (last) this.quotes[id] = { ...(this.quotes[id] ?? {}), ...last };

      const hist = this.quoteHist[id] ?? (this.quoteHist[id] = []);
      for (const q of list) {
        const pt = { tsSec: q.tsUnixSec, price: q.price };
        const prev = hist[hist.length - 1];
        if (!prev || prev.tsSec !== pt.tsSec || prev.price !== pt.price) {
          hist.push(pt);
        }
      }

      const sub = this.subs.find((s) => s.id === id);
      if (sub && hist.length) {
        const lastSec = hist[hist.length - 1]!.tsSec;
        const minSec = lastSec - sub.windowSecs;
        let firstKeep = 0;
        while (firstKeep < hist.length && hist[firstKeep]!.tsSec < minSec)
          firstKeep++;
        if (firstKeep > 0) hist.splice(0, firstKeep);
      }

      if (hist.length > MAX_QUOTE_POINTS)
        hist.splice(0, hist.length - MAX_QUOTE_POINTS);

      this.quoteHist[id] = hist;
    },

    setActiveMode(mode: "candles" | "quote") {
      this.activeMode = mode;
    },
    setResolution(bucketSecs: number) {
      this.activeResolutionSecs = bucketSecs;
    },

    addSub(sub: Subscription) {
      this.subs.push(sub);
      this.activeId = sub.id;
      this.ensureWS();
      this.ws!.add(sub);
    },
    updateSub(id: string, next: Subscription) {
      const cur = this.subs.find((s) => s.id === id);
      if (!cur) return;
      Object.assign(cur, next);
      this.ws?.update(cur);
    },
    removeSub(id: string) {
      const cur = this.subs.find((s) => s.id === id);
      if (!cur) return;
      this.ws?.remove(cur);
      this.subs = this.subs.filter((s) => s.id !== id);
      delete this.data[id];
      delete this.quotes[id];
      delete this.quoteHist[id];
      if (this.activeId === id) this.activeId = this.subs[0]?.id ?? "";
    },
    setActive(id: string) {
      this.activeId = id;
    },
  },
});
