package de.berlin.htw.trading.marketdata;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import de.berlin.htw.trading.quote.dto.DeltaQuote;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.SymbolKey;

public interface IMarketDataBuffer {
    long currentSeq();

    long appendFull(Quote q);

    long appendDelta(DeltaQuote dq);

    Snapshot snapshot(Duration window);

    List<ChangeRecord> pollSince(long afterSeq, int maxRecords);

    SymbolKey symbolForSubId(int subId);

    String quoteTypeForSubId(int subId);

    record Snapshot(
            long seq,
            Map<Integer, SymbolKey> idToKey,
            Map<Integer, String> idToType,
            Map<SymbolKey, Quote> lastPerSymbol,
            Map<SymbolKey, java.util.List<Quote>> windowPerSymbol) {
    }

    sealed interface ChangeRecord permits QuoteChange {
        long seq();

        long tsSec();

        int subId();

        SymbolKey key();
    }

    record QuoteChange(long seq, long tsSec, int subId, SymbolKey key, Quote quote) implements ChangeRecord {
    }
}
