package de.berlin.htw.trading.events;

import de.berlin.htw.trading.quote.dto.Quote;

public record InitialQuoteEvent(Quote quote) {
}
