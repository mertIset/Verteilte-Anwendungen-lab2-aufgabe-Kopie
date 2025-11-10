package de.berlin.htw.trading.events;

import de.berlin.htw.trading.quote.dto.SymbolKey;

public record QuoteEvent(SymbolKey key) {
}
