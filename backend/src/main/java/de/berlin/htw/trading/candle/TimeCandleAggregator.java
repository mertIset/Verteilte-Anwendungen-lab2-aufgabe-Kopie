package de.berlin.htw.trading.candle;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import de.berlin.htw.trading.candle.dto.Candle;
import de.berlin.htw.trading.quote.dto.Quote;

public final class TimeCandleAggregator {

    private final long frameSec;

    private Long lastTsSec = null;
    private Long lastAbsTick = null;

    private final NavigableMap<Long, Candle> candles = new ConcurrentSkipListMap<>();

    private Candle lastKnownCandle = null;

    public TimeCandleAggregator(long frameSec) {
        this.frameSec = frameSec;
    }

    public NavigableMap<Long, Candle> view() {
        return candles;
    }

    public Candle getLastKnownCandle() {
        return lastKnownCandle;
    }

    public void acceptQuote(Quote q) {
        if (lastTsSec == null || lastAbsTick == null) {
            long start = CandleClock.timeBucketStartSec(q.tsUnixSec(), frameSec);
            ensureGaps(start);
            upsertOhlcWithTicks(start, q.price(), 1, q.precision());
        } else {
            long ts0 = lastTsSec;
            long ts1 = q.tsUnixSec();
            long dSec = Math.max(1, ts1 - ts0);
            long dTick = Math.max(0, q.tick() - lastAbsTick);

            long cursor = ts0;
            long remaining = dTick;

            while (cursor < ts1) {
                long bucketStart = floor(cursor, frameSec);
                long bucketEnd = bucketStart + frameSec;
                long segEnd = Math.min(bucketEnd, ts1);
                long segDur = Math.max(1, segEnd - cursor);

                long alloc = (segEnd == ts1) ? remaining
                        : Math.round((double) dTick * ((double) segDur / (double) dSec));
                if (alloc > remaining)
                    alloc = remaining;

                ensureGaps(bucketStart);
                upsertOhlcWithTicks(bucketStart, q.price(), alloc, q.precision());

                remaining -= alloc;
                cursor = segEnd;
            }
        }

        lastTsSec = q.tsUnixSec();
        lastAbsTick = q.tick();
    }

    public void evictOlderThan(long minStartSec) {
        while (!candles.isEmpty()) {
            var first = candles.firstEntry();
            if (first.getKey() < minStartSec) {
                candles.pollFirstEntry();
            } else
                break;
        }
    }

    private void ensureGaps(long start) {
        if (candles.isEmpty())
            return;
        Long prevStart = candles.lastKey();
        while (prevStart != null && prevStart + frameSec < start) {
            Candle prev = candles.get(prevStart);
            long gapStart = prevStart + frameSec;
            double px = prev.close();
            candles.putIfAbsent(gapStart, new Candle(gapStart, px, px, px, px, 0, prev.precision()));
            prevStart = gapStart;
        }
    }

    private void upsertOhlcWithTicks(long start, double px, long addTicks, Double precision) {
        Candle c = candles.get(start);
        if (c == null) {
            long ticks = Math.max(0, addTicks);
            candles.put(start, new Candle(start, px, px, px, px, ticks, precision));
        } else {
            long ticks = c.ticks() + Math.max(0, addTicks);
            candles.put(start, new Candle(
                    c.bucketStartSec(),
                    c.open(),
                    Math.max(c.high(), px),
                    Math.min(c.low(), px),
                    px,
                    ticks, precision));

        }
        lastKnownCandle = candles.get(start);
    }

    private static long floor(long x, long frame) {
        return (x / frame) * frame;
    }
}
