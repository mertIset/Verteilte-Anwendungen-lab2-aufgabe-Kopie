package de.berlin.htw.trading.events;

import de.berlin.htw.trading.quote.dto.SymbolKey;

public record CandleEvent(SymbolKey symbolKey) {
}
