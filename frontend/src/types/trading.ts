export type Candle = {
  bucketStartSec: number;
  open: number;
  high: number;
  low: number;
  close: number;
  ticks: number;
  precision?: number | null;
};
export type Quote = {
  s: SymbolKey;
  tsUnixSec: number;
  price: number;
  high?: number | null;
  low?: number | null;
  open?: number | null;
  prevClose?: number | null;
  abs: number;
  rel: number;
  tickSize: number;
  active: boolean;
  tick: number;
  subId?: number | null;
  precision?: number | null;
};

export type SymbolKey = { venueId: string; symbolId: string; channel?: string };
export type Subscription = {
  id: string;
  key: SymbolKey;
  windowSecs: number;
};
