package de.berlin.htw.trading.candle;

public final class CandleClock {
    private CandleClock() {
    }

    public static long timeBucketStartSec(long tsSec, long frameSec) {
        return (tsSec / frameSec) * frameSec;
    }
}
