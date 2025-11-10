package de.berlin.htw.trading.quote.dto;

import java.util.Objects;

public final class SymbolKey {
    public final String symbolId;
    public final String venueId;
    public final String channel;

    public SymbolKey(String symbolId, String venueId, String channel) {
        this.symbolId = symbolId;
        this.venueId = venueId;
        this.channel = channel;
    }

    public static SymbolKey fromSub(String sub) {
        var parts = sub.split(":");
        return new SymbolKey(parts[0], parts[1], parts[2]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof SymbolKey k))
            return false;
        return Objects.equals(symbolId, k.symbolId)
                && Objects.equals(venueId, k.venueId)
                && Objects.equals(channel, k.channel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbolId, venueId, channel);
    }

    @Override
    public String toString() {
        return symbolId + ":" + venueId + ":" + channel;
    }
}