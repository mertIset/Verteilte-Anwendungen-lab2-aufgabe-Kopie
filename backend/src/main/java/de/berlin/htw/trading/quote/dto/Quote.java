package de.berlin.htw.trading.quote.dto;

public record Quote(
        SymbolKey s,
        long tsUnixSec,
        double price,
        Double high,
        Double low,
        Double open,
        Double prevClose,
        double abs,
        double rel,
        double tickSize,
        boolean active,
        long tick,
        Integer subId,
        Double precision) {

    public static Quote applyDelta(SymbolKey key, Quote prev, DeltaQuote dq, String quoteType) {
        if (prev == null && dq.value() == null)
            return null;

        double price = (dq.value() != null) ? dq.value() : (prev != null ? prev.price() : 0.0);

        long baseTs = (prev != null) ? prev.tsUnixSec() : (System.currentTimeMillis() / 1000);
        long ts = (dq.secSinceLastMessage() != null) ? baseTs + dq.secSinceLastMessage() : baseTs;

        long baseTick = (prev != null) ? prev.tick() : 0;
        long tick;
        if (dq.tickDelta() != null) {
            tick = baseTick + dq.tickDelta();
        } else {
            tick = baseTick;
        }

        Double high = prev != null ? prev.high() : null;
        Double low = prev != null ? prev.low() : null;

        if (dq.newHigh() != null)
            high = dq.newHigh();
        if (dq.newLow() != null)
            low = dq.newLow();
        if (dq.value() != null) {
            if (high == null || dq.value() > high)
                high = dq.value();
            if (low == null || dq.value() < low)
                low = dq.value();
        }

        double abs = (prev != null) ? prev.abs() : 0.0;
        double rel = (prev != null) ? prev.rel() : 0.0;

        if (dq.vDelta() != null)
            rel = dq.vDelta();
        if ("last".equals(quoteType) && dq.tvAbs() != null)
            abs = dq.tvAbs();

        if (prev != null && dq.value() != null) {
            abs = price - prev.prevClose();
            if (prev.prevClose() != 0.0)
                rel = (price / prev.prevClose()) - 1.0;
        }

        return new Quote(
                key, ts, price, high, low,
                prev != null ? prev.open() : null,
                prev != null ? prev.prevClose() : null,
                abs, rel,
                prev != null ? prev.tickSize() : 0.0,
                prev != null && prev.active(),
                tick, dq.subId(), prev != null ? prev.precision() : null);
    }
}
