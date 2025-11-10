import type { Candle, SymbolKey, Subscription, Quote } from "../types/trading";

export type WSMessage =
  | { type: "candles"; key: SymbolKey; data: Candle[] }
  | { type: "candle"; key: SymbolKey; data: Candle }
  | { type: "quotes"; key: SymbolKey; data: Quote | Quote[] }
  | { type: "quote"; key: SymbolKey; data: Quote }
  | { type: "pong" }
  | { type: "error"; message: string };

const keyHash = (k: SymbolKey) =>
  `${k.venueId}:${k.symbolId}:${k.channel ?? "last"}`;

export class WSClient {
  private url: string;
  private ws: WebSocket | null = null;
  private heartbeat?: number;
  private reconnectTimer?: number;

  private subs = new Map<string, Subscription>();

  private onCandles: (subId: string, payload: Candle[] | Candle) => void;
  private onQuote: (subId: string, payload: Quote | Quote[]) => void;
  private onStatus: (msg: string) => void;

  constructor(
    url: string,
    onCandles: (subId: string, payload: Candle[] | Candle) => void,
    onQuote: (subId: string, payload: Quote | Quote[]) => void,
    onStatus: (s: string) => void
  ) {
    this.url = url;
    this.onCandles = onCandles;
    this.onQuote = onQuote;
    this.onStatus = onStatus;
  }

  connect() {
    if (
      this.ws &&
      (this.ws.readyState === WebSocket.OPEN ||
        this.ws.readyState === WebSocket.CONNECTING)
    )
      return;
    this.onStatus("Verbinde…");
    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      this.onStatus("Verbunden");
      for (const sub of this.subs.values())
        this.send({ action: "subscribe", ...sub.key, window: sub.windowSecs });
      this.heartbeat && clearInterval(this.heartbeat);
      this.heartbeat = window.setInterval(
        () => this.send({ type: "ping" }),
        15_000
      );
    };

    this.ws.onmessage = (e) => {
      try {
        const msg: WSMessage | Candle[] = JSON.parse(e.data);

        if (Array.isArray(msg)) {
          if (this.subs.size === 1) {
            const subId = [...this.subs.values()][0]?.id;
            if (subId) this.onCandles(subId, msg);
          }
          return;
        }

        if (msg.type === "error") {
          this.onStatus(`Fehler: ${msg.message}`);
          return;
        }
        if (msg.type === "pong") return;

        let subId: string | undefined;
        if ((msg as any).key) {
          const hash = keyHash((msg as any).key as SymbolKey);
          const sub = [...this.subs.values()].find(
            (s) => keyHash(s.key) === hash
          );
          if (sub) subId = sub.id;
        } else if (this.subs.size === 1) {
          subId = [...this.subs.values()][0]?.id;
        }
        if (!subId) return;

        if (msg.type === "candles" || msg.type === "candle") {
          this.onCandles(subId, msg.data);
        } else if (msg.type === "quotes" || msg.type === "quote") {
          const data = msg.data as Quote | Quote[];
          this.onQuote(subId, data);
        }
      } catch {}
    };

    const scheduleReconnect = () => {
      this.onStatus("Reconnect…");
      this.reconnectTimer && clearTimeout(this.reconnectTimer);
      this.reconnectTimer = window.setTimeout(() => this.connect(), 1500);
    };
    this.ws.onclose = scheduleReconnect;
    this.ws.onerror = scheduleReconnect;
  }

  add(sub: Subscription) {
    const hash = keyHash(sub.key);
    this.subs.set(hash, sub);
    this.connect();
    this.send({ action: "subscribe", ...sub.key, window: sub.windowSecs });
  }

  update(sub: Subscription) {
    const oldHash = [...this.subs.keys()].find(
      (h) => this.subs.get(h)!.id === sub.id
    );
    if (oldHash) {
      const old = this.subs.get(oldHash)!;
      const newHash = keyHash(sub.key);
      if (newHash !== oldHash) {
        this.send({ action: "unsubscribe", ...old.key });
        this.subs.delete(oldHash);
      }
    }
    this.subs.set(keyHash(sub.key), sub);
    this.connect();
    this.send({ action: "subscribe", ...sub.key, window: sub.windowSecs });
  }

  remove(sub: Subscription) {
    const hash = keyHash(sub.key);
    this.subs.delete(hash);
    this.send({ action: "unsubscribe", ...sub.key });
    if (!this.subs.size) this.close();
  }

  close() {
    this.heartbeat && clearInterval(this.heartbeat);
    this.reconnectTimer && clearTimeout(this.reconnectTimer);
    this.ws?.close();
    this.ws = null;
  }

  private send(obj: any) {
    const data = JSON.stringify(obj);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(data);
  }
}
