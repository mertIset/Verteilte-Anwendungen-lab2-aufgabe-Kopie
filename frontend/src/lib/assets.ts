import type { SymbolKey } from "../types/trading";

export type Asset = {
  label: string;
  symbolId: string;
  venueId: string;
  channel?: string;
};

export const ASSETS: Asset[] = [
  { label: "DAX", symbolId: "133962", venueId: "22", channel: "last" },
  { label: "DOW JONES", symbolId: "133965", venueId: "119", channel: "last" },
  { label: "S&P 500", symbolId: "133954", venueId: "119", channel: "last" },
  { label: "NASDAQ 100", symbolId: "133955", venueId: "119", channel: "last" },
  { label: "NIKKEI225", symbolId: "133958", venueId: "119", channel: "last" },
  { label: "GOLD", symbolId: "133979", venueId: "98", channel: "bid" },
  { label: "EUR/USD", symbolId: "134000", venueId: "27", channel: "bid" },
  { label: "BTC/USD", symbolId: "23087055", venueId: "117", channel: "last" },
  { label: "ETH/USD", symbolId: "23087058", venueId: "117", channel: "last" },
  {
    label: "BRENT CRUDE ÖL",
    symbolId: "133978",
    venueId: "98",
    channel: "bid",
  },
  {
    label: "EURO BOND FUTURES",
    symbolId: "134018",
    venueId: "119",
    channel: "last",
  },
];

const keyHash = (k: Pick<SymbolKey, "venueId" | "symbolId" | "channel">) =>
  `${k.venueId}:${k.symbolId}:${k.channel ?? "last"}`;

let _assetMap: Map<string, Asset> = new Map();
function rebuildMap() {
  _assetMap = new Map(ASSETS.map((a) => [keyHash(a), a]));
}
rebuildMap();

export function getAllAssets(): Asset[] {
  return ASSETS.slice();
}

export function addAssets(list: Asset[]): void {
  if (!Array.isArray(list) || !list.length) return;
  for (const a of list) ASSETS.push(a);
  rebuildMap();
}

export function replaceAssets(list: Asset[]): void {
  (ASSETS as Asset[]).length = 0;
  for (const a of list) ASSETS.push(a);
  rebuildMap();
}

export function findAssetByKey(key: SymbolKey): Asset | undefined {
  return _assetMap.get(keyHash(key));
}

export function labelForKey(key: SymbolKey): string {
  const a = findAssetByKey(key);
  if (a) return a.label;
  return `${key.symbolId} • ${key.venueId} • ${key.channel ?? "last"}`;
}

export function findAssetByLabel(label: string): Asset | undefined {
  return ASSETS.find((a) => a.label === label);
}
