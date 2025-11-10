package de.berlin.htw.trading.events;

import de.berlin.htw.trading.quote.dto.DeltaQuote;

public record QuoteDeltaEvent(DeltaQuote deltaQuote) {
}
