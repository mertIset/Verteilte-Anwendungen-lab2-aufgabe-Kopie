package de.berlin.htw.boundary.ws.dto;

import de.berlin.htw.trading.quote.dto.SymbolKey;

public record UnsubEvent(SymbolKey key) {
    public String toMessage() {
        return "r" + key().symbolId + ":" + key().venueId + ":" + key().channel;
    }
}
