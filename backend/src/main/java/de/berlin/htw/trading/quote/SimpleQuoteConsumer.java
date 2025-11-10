package de.berlin.htw.trading.quote;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.jboss.logging.Logger;

import de.berlin.htw.trading.consumer.AbstractReplayingConsumer;
import de.berlin.htw.trading.events.QuoteEvent;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer.ChangeRecord;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer.Snapshot;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.SymbolKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

@ApplicationScoped
public class SimpleQuoteConsumer extends AbstractReplayingConsumer {

    private final Duration retention = Duration.ofHours(1);

    private final Map<SymbolKey, Deque<Quote>> series = new ConcurrentHashMap<>();

    private final Map<SymbolKey, Quote> last = new ConcurrentHashMap<>();

    @Inject
    private Logger logger;

    @Inject
    private Event<QuoteEvent> quoteEvent;

    @Override
    protected Duration initialSnapshotWindow() {
        return Duration.ofMinutes(30);
    }

    @Override
    protected void rebuildFromSnapshot(Snapshot snap) {
        series.clear();
        for (var e : snap.windowPerSymbol().entrySet()) {
            series.put(e.getKey(), new ConcurrentLinkedDeque<>(e.getValue()));
            quoteEvent.fireAsync(new QuoteEvent(e.getKey()));
        }

    }

    @Override
    protected void applyChanges(List<ChangeRecord> changes) {

        Set<SymbolKey> updatedKeys = ConcurrentHashMap.newKeySet();

        // TODO: Hier fehlt die Verarbeitung der eingehenden Kursänderungen

        for (var key : updatedKeys) {
            quoteEvent.fireAsync(new QuoteEvent(key));
        }
    }

    public List<Quote> getQuotes(SymbolKey key, Duration window) {
        // TODO: Implementieren Sie die Methode, um die Kursserie für das gegebene
        // SymbolKey und Zeitfenster zurückzugeben
        return new ArrayList<>();
    }

    public Quote getLast(SymbolKey key) {
        Deque<Quote> dq = series.get(key);
        return (dq == null || dq.isEmpty()) ? null : dq.peekLast();
    }

    public Quote getLastKnown(SymbolKey key) {
        return last.get(key);
    }
}
