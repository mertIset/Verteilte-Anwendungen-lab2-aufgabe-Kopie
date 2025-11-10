package de.berlin.htw.boundary.ws.dto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import de.berlin.htw.trading.quote.dto.SymbolKey;

public class Subscription {
    public final Map<SymbolKey, Integer> windows = new ConcurrentHashMap<>();
}
