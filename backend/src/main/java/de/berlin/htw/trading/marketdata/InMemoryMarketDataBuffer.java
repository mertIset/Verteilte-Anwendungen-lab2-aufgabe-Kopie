package de.berlin.htw.trading.marketdata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import de.berlin.htw.trading.quote.dto.DeltaQuote;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.SymbolKey;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InMemoryMarketDataBuffer extends AbstractMarketDataBuffer {

    private final Duration retention = Duration.ofHours(1);
    private final int logMaxRecords = 200_000;

    private final Map<Integer, SymbolKey> idToKey = new ConcurrentHashMap<>();
    private final Map<Integer, String> idToType = new ConcurrentHashMap<>();
    private final Map<SymbolKey, Deque<Quote>> series = new ConcurrentHashMap<>();
    private final Map<SymbolKey, Quote> lastPerSymbol = new ConcurrentHashMap<>();

    private final Deque<ChangeRecord> log = new ConcurrentLinkedDeque<>();
    private final AtomicLong seq = new AtomicLong(0);

    @Override
    public long currentSeq() {
        return seq.get();
    }

    @Override
    public Snapshot snapshot(Duration window) {
        long nowSec = System.currentTimeMillis() / 1000;
        long minTs = nowSec - window.getSeconds();
        Map<SymbolKey, List<Quote>> byKey = new HashMap<>();
        for (var e : series.entrySet()) {
            var out = new ArrayList<Quote>();
            for (Quote q : e.getValue())
                if (q.tsUnixSec() >= minTs)
                    out.add(q);
            if (!out.isEmpty())
                byKey.put(e.getKey(), out);
        }
        return new Snapshot(
                seq.get(),
                Map.copyOf(idToKey),
                Map.copyOf(idToType),
                Map.copyOf(lastPerSymbol),
                Map.copyOf(byKey));
    }

    @Override
    public List<ChangeRecord> pollSince(long afterSeq, int maxRecords) {
        var out = new ArrayList<ChangeRecord>(Math.min(maxRecords, 4096));
        for (ChangeRecord cr : log) {
            if (cr.seq() > afterSeq) {
                out.add(cr);
                if (out.size() >= maxRecords)
                    break;
            }
        }
        return out;
    }

    @Override
    public SymbolKey symbolForSubId(int subId) {
        return idToKey.get(subId);
    }

    @Override
    public String quoteTypeForSubId(int subId) {
        return idToType.get(subId);
    }

    @Override
    protected synchronized long doAppendFull(Quote q) {
        if (q == null || q.s() == null)
            return 0L;

        idToKey.putIfAbsent(q.subId(), q.s());
        if (q.s().channel != null)
            idToType.putIfAbsent(q.subId(), q.s().channel);

        lastPerSymbol.put(q.s(), q);
        series.computeIfAbsent(q.s(), k -> new ConcurrentLinkedDeque<>()).addLast(q);
        evictOld(q.s(), q.tsUnixSec());

        long s = seq.incrementAndGet();
        logAdd(new QuoteChange(s, q.tsUnixSec(), q.subId(), q.s(), q));
        return s;
    }

    @Override
    protected synchronized long doAppendDelta(DeltaQuote dq) {
        if (dq == null)
            return 0L;
        SymbolKey key = idToKey.get(dq.subId());
        if (key == null)
            return 0L;

        Quote prev = lastPerSymbol.get(key);
        Quote updated = Quote.applyDelta(key, prev, dq, idToType.get(dq.subId()));
        if (updated == null)
            return 0L;

        lastPerSymbol.put(key, updated);
        series.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>()).addLast(updated);
        evictOld(key, updated.tsUnixSec());

        long s = seq.incrementAndGet();
        logAdd(new QuoteChange(s, updated.tsUnixSec(), dq.subId(), key, updated));
        return s;
    }

    private void evictOld(SymbolKey key, long newestTsSec) {
        long minTs = newestTsSec - retention.getSeconds();
        Deque<Quote> dq = series.get(key);
        if (dq != null) {
            while (!dq.isEmpty() && dq.peekFirst().tsUnixSec() < minTs)
                dq.pollFirst();
            if (dq.isEmpty())
                series.remove(key);
        }
        while (log.size() > logMaxRecords)
            log.pollFirst();
    }

    private void logAdd(ChangeRecord cr) {
        log.addLast(cr);
        while (log.size() > logMaxRecords)
            log.pollFirst();
    }
}