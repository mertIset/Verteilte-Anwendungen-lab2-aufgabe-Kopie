package de.berlin.htw.trading.marketdata;

import de.berlin.htw.trading.events.InitialQuoteEvent;
import de.berlin.htw.trading.events.QuoteDeltaEvent;
import de.berlin.htw.trading.quote.dto.DeltaQuote;
import de.berlin.htw.trading.quote.dto.Quote;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

public abstract class AbstractMarketDataBuffer implements IMarketDataBuffer {

    @Inject
    protected Event<BufferAdvancedEvent> out;

    void onQuote(@Observes InitialQuoteEvent ev) {
        if (ev != null && ev.quote() != null)
            appendFull(ev.quote());
    }

    void onQuoteAsync(@ObservesAsync InitialQuoteEvent ev) {
        if (ev != null && ev.quote() != null)
            appendFull(ev.quote());
    }

    void onDelta(@Observes QuoteDeltaEvent ev) {
        if (ev != null && ev.deltaQuote() != null)
            appendDelta(ev.deltaQuote());
    }

    void onDeltaAsync(@ObservesAsync QuoteDeltaEvent ev) {
        if (ev != null && ev.deltaQuote() != null)
            appendDelta(ev.deltaQuote());
    }

    @Override
    public final long appendFull(Quote q) {
        long seq = doAppendFull(q);
        if (seq > 0)
            fireSignal(seq);
        return seq;
    }

    @Override
    public final long appendDelta(DeltaQuote dq) {
        long seq = doAppendDelta(dq);
        if (seq > 0)
            fireSignal(seq);
        return seq;
    }

    protected void fireSignal(long seqHi) {
        if (out != null)
            out.fire(new BufferAdvancedEvent(seqHi));
    }

    protected abstract long doAppendFull(Quote q);

    protected abstract long doAppendDelta(DeltaQuote dq);
}
