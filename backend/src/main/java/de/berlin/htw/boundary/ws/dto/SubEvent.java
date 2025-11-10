package de.berlin.htw.boundary.ws.dto;

import de.berlin.htw.trading.quote.dto.SymbolKey;

public record SubEvent(SymbolKey key) {
    public String toMessage() {
        return "a" + key().symbolId + ":" + key().venueId + ":" + key().channel;
    }
}
