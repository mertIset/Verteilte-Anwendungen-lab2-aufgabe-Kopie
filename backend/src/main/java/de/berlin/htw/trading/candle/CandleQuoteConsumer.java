package de.berlin.htw.trading.candle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;

import de.berlin.htw.trading.candle.dto.Candle;
import de.berlin.htw.trading.consumer.AbstractReplayingConsumer;
import de.berlin.htw.trading.events.CandleEvent;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer.ChangeRecord;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer.Snapshot;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.SymbolKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

@ApplicationScoped
public class CandleQuoteConsumer extends AbstractReplayingConsumer {

    private static final long FRAME_SEC = 60;
    private final Duration retention = Duration.ofHours(1);

    private final Map<SymbolKey, TimeCandleAggregator> aggs = new ConcurrentHashMap<>();

    @Inject
    Logger logger;

    @Inject
    private Event<CandleEvent> candleEvent;

    @Override
    protected Duration initialSnapshotWindow() {
        return Duration.ofMinutes(30);
    }

    @Override
    protected void rebuildFromSnapshot(Snapshot snap) {
        aggs.clear();
        for (var e : snap.windowPerSymbol().entrySet()) {
            var key = e.getKey();
            var agg = aggs.computeIfAbsent(key, k -> new TimeCandleAggregator(FRAME_SEC));
            for (Quote q : e.getValue()) {
                agg.acceptQuote(q);
            }
            candleEvent.fireAsync(new CandleEvent(key));
        }
    }

    @Override
    protected void applyChanges(List<ChangeRecord> changes) {
        Set<SymbolKey> updatedKeys = new HashSet<>();
        long minStart = (System.currentTimeMillis() / 1000) - retention.getSeconds();
        for (var cr : changes) {
            var qc = (IMarketDataBuffer.QuoteChange) cr;
            var agg = aggs.computeIfAbsent(qc.key(), k -> new TimeCandleAggregator(FRAME_SEC));
            agg.acceptQuote(qc.quote());
            agg.evictOlderThan(minStart);
            updatedKeys.add(qc.key());
        }

        for (var key : updatedKeys) {
            candleEvent.fireAsync(new CandleEvent(key));
        }

    }

    public List<Candle> getCandles(SymbolKey key, Duration window) {
        var agg = aggs.get(key);
        if (agg == null)
            return List.of();
        NavigableMap<Long, Candle> m = agg.view();
        long min = (System.currentTimeMillis() / 1000) - window.getSeconds();
        var out = new ArrayList<Candle>();
        for (Candle c : m.values())
            if (c.bucketStartSec() >= min)
                out.add(c);
        return out;
    }

    public NavigableMap<Long, Candle> view(SymbolKey key) {
        var agg = aggs.get(key);
        return agg == null ? new TreeMap<>() : new TreeMap<>(agg.view());
    }

    public Candle getLatestCandle(SymbolKey key) {
        var agg = aggs.get(key);
        if (agg == null)
            return null;
        var m = agg.view();
        return m.isEmpty() ? null : m.lastEntry().getValue();
    }

    public Candle getLastKnownCandle(SymbolKey key) {
        var agg = aggs.get(key);
        if (agg == null)
            return null;
        return agg.getLastKnownCandle();
    }
}