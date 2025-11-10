package de.berlin.htw.boundary.ws.client;

import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.websocket.ClientEndpoint;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.berlin.htw.trading.events.InitialQuoteEvent;
import de.berlin.htw.trading.events.QuoteDeltaEvent;
import de.berlin.htw.trading.quote.dto.DeltaQuote;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.SymbolKey;

import io.quarkus.scheduler.Scheduled;

@ClientEndpoint
public class QuoteClient {

    @Inject
    Logger logger;

    @Inject
    private Event<InitialQuoteEvent> quoteEvent;

    @Inject
    private Event<QuoteDeltaEvent> quoteDeltaEvent;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Map<Integer, SymbolKey> subMap = new ConcurrentHashMap<>();

    private Double lastValue;

    // TODO: IMPLEMENTIEREN SIE DEN WEBSOCKET-CLIENTEN FÜR KURSE

    // Schauen Sie sich in dieser Methode an, wie Sie die empfangenen Kurse nachher
    // weitergeben können
    @Scheduled(every = "5s")
    public void generateRandomData() {
        if (lastValue == null) {

            long currentTimeUnixInSeconds = System.currentTimeMillis() / 1000;
            Quote q = new Quote(new SymbolKey("133979", "98", "bid"), currentTimeUnixInSeconds, 12000.00, 12200.00,
                    11800.00, 11950.00,
                    11800.00,
                    200.00, 0.016949153, 0.01,
                    true, 560L, 1, 2.0);
            quoteEvent.fireAsync(new InitialQuoteEvent(q));
            lastValue = 12000.00;
        }

        lastValue += (Math.random() - 0.5) * 100;
        DeltaQuote dq = new DeltaQuote(1, lastValue, 5L, 15L, null, null, null, null);
        quoteDeltaEvent.fireAsync(new QuoteDeltaEvent(dq));
    }

}
