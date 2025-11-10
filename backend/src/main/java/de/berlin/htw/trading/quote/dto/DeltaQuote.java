package de.berlin.htw.trading.quote.dto;

import static de.berlin.htw.util.ParsingHelper.tryD;
import static de.berlin.htw.util.ParsingHelper.tryInt;
import static de.berlin.htw.util.ParsingHelper.tryL;

public record DeltaQuote(
        int subId,
        Double value,
        Long secSinceLastMessage,
        Long tickDelta,
        Double newHigh,
        Double newLow,
        Double vDelta,
        Double tvAbs) {
    public static DeltaQuote parse(String frame) {
        String[] p = frame.split(":", -1);
        return new DeltaQuote(
                tryInt(p[0]),
                tryD(p, 1),
                tryL(p, 2),
                tryL(p, 3),
                tryD(p, 4),
                tryD(p, 5),
                tryD(p, 6),
                tryD(p, 7));
    }

}
