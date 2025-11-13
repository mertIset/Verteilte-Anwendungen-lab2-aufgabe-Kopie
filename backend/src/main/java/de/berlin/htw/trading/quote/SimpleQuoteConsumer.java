package de.berlin.htw.trading.quote;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
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

    // Map: SymbolKey -> Deque von Quotes (Zeitreihe)
    // Deque (Double-ended queue) ermöglicht effizientes Hinzufügen/Entfernen an beiden Enden
    private final Map<SymbolKey, Deque<Quote>> series = new ConcurrentHashMap<>();

    // Map: SymbolKey -> letztes bekanntes Quote
    // Wird verwendet, wenn wir ein Quote brauchen, aber die Serie leer ist
    private final Map<SymbolKey, Quote> last = new ConcurrentHashMap<>();

    @Inject
    private Logger logger;

    @Inject
    private Event<QuoteEvent> quoteEvent;

    @Override
    protected Duration initialSnapshotWindow() {
        // Beim Start laden wir die letzten 30 Minuten
        return Duration.ofMinutes(30);
    }

    @Override
    protected void rebuildFromSnapshot(Snapshot snap) {
        // Diese Methode wird beim Start aufgerufen
        // Sie baut den internen Zustand aus dem Snapshot auf

        logger.info("Rebuilding SimpleQuoteConsumer from snapshot...");

        // Lösche alle bestehenden Daten
        series.clear();
        last.clear();

        // Verarbeite alle Quotes aus dem Snapshot
        for (var entry : snap.windowPerSymbol().entrySet()) {
            SymbolKey key = entry.getKey();
            List<Quote> quotes = entry.getValue();

            logger.infov("Lade {0} Quotes für {1}", quotes.size(), key);

            // Erstelle eine neue Deque und füge alle Quotes hinzu
            Deque<Quote> deque = new ConcurrentLinkedDeque<>(quotes);
            series.put(key, deque);

            // Speichere das letzte Quote
            if (!quotes.isEmpty()) {
                last.put(key, quotes.get(quotes.size() - 1));
            }

            // Feuere ein Event, damit andere Komponenten wissen, dass Daten verfügbar sind
            quoteEvent.fireAsync(new QuoteEvent(key));
        }

        logger.infov("SimpleQuoteConsumer rebuilt with {0} symbols", series.size());
    }

    @Override
    protected void applyChanges(List<ChangeRecord> changes) {
        // Diese Methode wird aufgerufen, wenn neue Quotes ankommen
        // Sie verarbeitet eine Liste von Änderungen

        logger.debugv("Applying {0} changes to SimpleQuoteConsumer", changes.size());

        // Set zum Sammeln aller geänderten SymbolKeys
        Set<SymbolKey> updatedKeys = ConcurrentHashMap.newKeySet();

        // Berechne die minimale Zeitstempel-Grenze für die Retention-Policy
        long minTs = (System.currentTimeMillis() / 1000) - retention.getSeconds();

        // Verarbeite jede Änderung
        for (ChangeRecord cr : changes) {
            // Caste den ChangeRecord zu QuoteChange (das ist der einzige Typ, den wir haben)
            IMarketDataBuffer.QuoteChange qc = (IMarketDataBuffer.QuoteChange) cr;

            SymbolKey key = qc.key();
            Quote quote = qc.quote();

            logger.debugv("Processing quote change for {0}: price={1}, ts={2}",
                    key, quote.price(), quote.tsUnixSec());

            // Hole oder erstelle die Deque für diesen SymbolKey
            Deque<Quote> deque = series.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>());

            // Füge das neue Quote am Ende hinzu
            deque.addLast(quote);

            // Speichere als letztes bekanntes Quote
            last.put(key, quote);

            // Entferne alte Quotes, die außerhalb der Retention-Period liegen
            evictOld(key, minTs);

            // Merke, dass dieser SymbolKey aktualisiert wurde
            updatedKeys.add(key);
        }

        // Feuere Events für alle aktualisierten SymbolKeys
        for (SymbolKey key : updatedKeys) {
            logger.debugv("Firing QuoteEvent for {0}", key);
            quoteEvent.fireAsync(new QuoteEvent(key));
        }

        logger.debugv("Applied changes for {0} symbols", updatedKeys.size());
    }

    /**
     * Entfernt alte Quotes, die älter als minTs sind
     */
    private void evictOld(SymbolKey key, long minTs) {
        Deque<Quote> deque = series.get(key);
        if (deque == null) return;

        // Entferne Quotes vom Anfang der Deque, solange sie zu alt sind
        while (!deque.isEmpty()) {
            Quote first = deque.peekFirst();
            if (first != null && first.tsUnixSec() < minTs) {
                deque.pollFirst();
                logger.debugv("Evicted old quote for {0}: ts={1}", key, first.tsUnixSec());
            } else {
                break;
            }
        }

        // Wenn die Deque leer ist, entferne sie aus der Map
        if (deque.isEmpty()) {
            series.remove(key);
            logger.debugv("Removed empty series for {0}", key);
        }
    }

    /**
     * Gibt alle Quotes für einen SymbolKey innerhalb eines Zeitfensters zurück
     */
    public List<Quote> getQuotes(SymbolKey key, Duration window) {
        logger.debugv("Getting quotes for {0} with window {1}", key, window);

        Deque<Quote> deque = series.get(key);
        if (deque == null || deque.isEmpty()) {
            logger.debugv("No quotes found for {0}", key);
            return new ArrayList<>();
        }

        // Berechne die minimale Zeitstempel-Grenze
        long nowSec = System.currentTimeMillis() / 1000;
        long minTs = nowSec - window.getSeconds();

        // Erstelle eine neue Liste mit allen Quotes innerhalb des Zeitfensters
        List<Quote> result = new ArrayList<>();
        for (Quote q : deque) {
            if (q.tsUnixSec() >= minTs) {
                result.add(q);
            }
        }

        logger.debugv("Returning {0} quotes for {1}", result.size(), key);
        return result;
    }

    /**
     * Gibt das aktuellste Quote aus der Serie zurück (oder null)
     */
    public Quote getLast(SymbolKey key) {
        Deque<Quote> dq = series.get(key);
        if (dq == null || dq.isEmpty()) {
            return null;
        }
        return dq.peekLast();
    }

    /**
     * Gibt das letzte bekannte Quote zurück (auch wenn die Serie leer ist)
     */
    public Quote getLastKnown(SymbolKey key) {
        return last.get(key);
    }
}