package de.berlin.htw.trading.quote.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record QuoteMessage(
        double q,
        Double h,
        Double l,
        Double o,
        Double pc,
        long ts,
        long t,
        double abs,
        double rel,
        double tickSize,
        boolean active,
        Integer i,
        String s,
        Double precision) {
}
