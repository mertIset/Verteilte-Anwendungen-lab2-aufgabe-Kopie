package de.berlin.htw.trading.consumer;

import java.time.Duration;
import java.util.List;

import de.berlin.htw.trading.marketdata.BufferAdvancedEvent;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer.ChangeRecord;
import de.berlin.htw.trading.marketdata.IMarketDataBuffer.Snapshot;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

public abstract class AbstractReplayingConsumer {

    protected abstract Duration initialSnapshotWindow();

    protected int maxBatch() {
        return 10_000;
    }

    @Inject
    protected IMarketDataBuffer buffer;

    protected volatile long lastAppliedSeq = 0L;

    @PostConstruct
    protected void postConstructInit() {
        initialize();
    }

    public final void initialize() {
        var snap = buffer.snapshot(initialSnapshotWindow());
        rebuildFromSnapshot(snap);
        this.lastAppliedSeq = snap.seq();
    }

    protected final void onAdvance(@Observes BufferAdvancedEvent ev) {
        if (ev == null)
            return;
        pullAndApplyUntil(ev.seq());
    }

    protected final void onAdvanceAsync(@ObservesAsync BufferAdvancedEvent ev) {
        if (ev == null)
            return;
        pullAndApplyUntil(ev.seq());
    }

    protected void pullAndApplyUntil(long targetSeq) {
        long cursor = this.lastAppliedSeq;
        while (cursor < targetSeq) {
            List<ChangeRecord> batch = buffer.pollSince(cursor, maxBatch());
            if (batch.isEmpty())
                break;
            applyChanges(batch);
            cursor = batch.get(batch.size() - 1).seq();
            this.lastAppliedSeq = cursor;
        }
    }

    protected abstract void rebuildFromSnapshot(Snapshot snap);

    protected abstract void applyChanges(List<ChangeRecord> changes);
}