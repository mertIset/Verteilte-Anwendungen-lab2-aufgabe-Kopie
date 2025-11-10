package de.berlin.htw.boundary.ws;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;

import de.berlin.htw.boundary.ws.dto.SubEvent;
import de.berlin.htw.boundary.ws.dto.Subscription;
import de.berlin.htw.boundary.ws.dto.UnsubEvent;
import de.berlin.htw.boundary.ws.dto.WsMsgs;
import de.berlin.htw.trading.candle.CandleQuoteConsumer;
import de.berlin.htw.trading.candle.dto.Candle;
import de.berlin.htw.trading.events.CandleEvent;
import de.berlin.htw.trading.events.QuoteEvent;
import de.berlin.htw.trading.quote.SimpleQuoteConsumer;
import de.berlin.htw.trading.quote.dto.SymbolKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.websocket.Session;

@ApplicationScoped
public class WebsocketServer {

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    private final Map<String, Subscription> subs = new ConcurrentHashMap<>();

    private final Jsonb jsonb = JsonbBuilder.create();

    @Inject
    private CandleQuoteConsumer candleQuoteConsumer;

    @Inject
    private SimpleQuoteConsumer simpleQuoteConsumer;

    @Inject
    private Event<SubEvent> subEvent;

    @Inject
    private Event<UnsubEvent> unsubEvent;

    @Inject
    Logger logger;

    // TODO: IMPLEMENTIEREN SIE DIE METHODEN FÜR DEN WEBSOCKET-SERVER

    void unsubscribe(WsMsgs.Sub sub, Session session) {
        SymbolKey key = toKey(sub);
        if (key == null)
            return;
        subs.get(session.getId()).windows.remove(key);
        subs.forEach((sid, s) -> {
            if (s.windows.keySet().stream().anyMatch(key::equals))
                return;
        });
        unsubEvent.fire(new UnsubEvent(key));
    }

    void subscribe(WsMsgs.Sub sub, Session session) {
        SymbolKey key = toKey(sub);
        if (key == null)
            return;
        subEvent.fire(new SubEvent(key));

        int windowSecs = (sub.window != null && sub.window > 0) ? sub.window : 3600;

        subs.get(session.getId()).windows.put(key, windowSecs);

        var list = candleQuoteConsumer.getCandles(key, Duration.ofSeconds(windowSecs));
        if (list == null || list.isEmpty()) {
            var lastKnownCandle = candleQuoteConsumer.getLastKnownCandle(key);
            if (lastKnownCandle != null) {
                list = List.of(lastKnownCandle);
            } else {
                list = List.of();
            }
        }
        var payload = Map.of("type", "candles", "key", key, "data", list);
        sendJson(session, payload);
        var quoteList = simpleQuoteConsumer.getQuotes(key, Duration.ofSeconds(windowSecs));
        if (quoteList == null || quoteList.isEmpty()) {
            var lastKnownQuote = simpleQuoteConsumer.getLastKnown(key);
            if (lastKnownQuote != null) {
                quoteList = List.of(lastKnownQuote);
            } else {
                quoteList = List.of();
            }
        }
        var quotePayload = Map.of("type", "quotes", "key", key,
                "data", quoteList);
        sendJson(session, quotePayload);
    }

    void onCandleEvent(@ObservesAsync CandleEvent ev) {
        SymbolKey key = ev.symbolKey();

        Candle c = candleQuoteConsumer.getLatestCandle(key);
        if (c == null)
            c = candleQuoteConsumer.getLastKnownCandle(key);
        if (c == null)
            return;

        var msg = Map.of("type", "candle", "key", key, "data", c);

        for (var entry : sessions.entrySet()) {
            var sessionId = entry.getKey();
            var session = entry.getValue();
            var st = subs.get(sessionId);
            if (st == null)
                continue;
            if (st.windows.keySet().stream().anyMatch(key::equals)) {
                sendJson(session, msg);
            }
        }
    }

    void onQuoteEvent(@ObservesAsync QuoteEvent ev) {
        SymbolKey key = ev.key();

        var q = simpleQuoteConsumer.getLast(key);
        if (q == null)
            q = simpleQuoteConsumer.getLastKnown(key);
        if (q == null)
            return;

        var msg = Map.of("type", "quote", "key", key, "data", q);

        for (var entry : sessions.entrySet()) {
            var sessionId = entry.getKey();
            var session = entry.getValue();
            var st = subs.get(sessionId);
            if (st == null)
                continue;
            if (st.windows.keySet().stream().anyMatch(key::equals)) {
                sendJson(session, msg);
            }
        }
    }

    private static SymbolKey toKey(WsMsgs.Sub sub) {
        if (sub.venueId == null || sub.symbolId == null)
            return null;
        return new SymbolKey(sub.symbolId, sub.venueId, sub.channel == null ? "last" : sub.channel);
    }

    private void sendJson(Session s, Object obj) {
        Session session = sessions.get(s.getId());
        if (session == null || !session.isOpen())
            return;
        try {
            String json = jsonb.toJson(obj);
            logger.infov("Sending JSON over WebSocket: {0}", json);
            // TODO: Senden Sie die JSON-Nachricht an den Client über die WebSocket-Session
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
