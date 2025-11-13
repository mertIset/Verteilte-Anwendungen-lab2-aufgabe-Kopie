package de.berlin.htw.boundary.ws.client;

import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.berlin.htw.trading.events.InitialQuoteEvent;
import de.berlin.htw.trading.events.QuoteDeltaEvent;
import de.berlin.htw.trading.quote.dto.DeltaQuote;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.QuoteMessage;
import de.berlin.htw.trading.quote.dto.SymbolKey;

import io.quarkus.scheduler.Scheduled;

@ClientEndpoint  // <-- Diese Annotation macht die Klasse zu einem WebSocket-Client
public class QuoteClient {

    @Inject
    Logger logger;

    @Inject
    private Event<InitialQuoteEvent> quoteEvent;

    @Inject
    private Event<QuoteDeltaEvent> quoteDeltaEvent;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Map: Subscription-ID -> SymbolKey
    // Die Subscription-ID (i) kommt vom Stock3-Server
    private final Map<Integer, SymbolKey> subMap = new ConcurrentHashMap<>();

    private Double lastValue;

    // METHODE 1: Wird aufgerufen, wenn die Verbindung zum Stock3-Server hergestellt wird
    @OnOpen
    public void onOpen(Session session) {
        logger.info("Verbindung zum Stock3-Server hergestellt!");
        logger.infov("Session-ID: {0}", session.getId());
    }

    // METHODE 2: Empfängt Nachrichten vom Stock3-Server
    @OnMessage
    public void onMessage(String message, Session session) {
        logger.debugv("Nachricht vom Stock3-Server: {0}", message);

        try {
            // Filtere Willkommensnachrichten heraus
            if (message.contains("[stock3-")) {
                logger.debug("Willkommensnachricht gefiltert");
                return;
            }

            // Prüfe, ob es eine Delta-Nachricht ist (Format: "22:49032.7196395:3:::::")
            if (message.contains(":") && !message.contains("{")) {
                handleDeltaMessage(message);
            } else {
                // Es ist eine vollständige JSON-Nachricht (initial quote)
                handleInitialQuote(message);
            }

        } catch (Exception e) {
            logger.errorv(e, "Fehler beim Verarbeiten der Stock3-Nachricht: {0}", message);
        }
    }

    // METHODE 3: Verarbeitet initiale Kursnachrichten (JSON-Format)
    private void handleInitialQuote(String message) {
        try {
            // Parse die JSON-Nachricht
            QuoteMessage qm = MAPPER.readValue(message, QuoteMessage.class);

            logger.infov("Initial Quote empfangen - SubID: {0}, Symbol: {1}, Preis: {2}",
                    qm.i(), qm.s(), qm.q());

            // Erstelle SymbolKey aus dem Symbol-String (Format: "symbolId:venueId:channel")
            SymbolKey key = SymbolKey.fromSub(qm.s());

            // Speichere die Zuordnung Subscription-ID -> SymbolKey
            if (qm.i() != null) {
                subMap.put(qm.i(), key);
            }

            // Erstelle ein Quote-Objekt
            Quote quote = new Quote(
                    key,                    // s: SymbolKey
                    qm.ts(),               // tsUnixSec: Zeitstempel
                    qm.q(),                // price: aktueller Kurs
                    qm.h(),                // high: Tageshoch
                    qm.l(),                // low: Tagestief
                    qm.o(),                // open: Eröffnungskurs
                    qm.pc(),               // prevClose: Vortageskurs
                    qm.abs(),              // abs: absoluter Unterschied
                    qm.rel(),              // rel: relativer Unterschied
                    qm.tickSize(),         // tickSize: Tick-Größe
                    qm.active(),           // active: ist Börse geöffnet
                    qm.t(),                // tick: Ticknummer
                    qm.i(),                // subId: Subscription-ID
                    qm.precision()         // precision: Nachkommastellen
            );

            // Feuere das Event, damit andere Komponenten das Quote verarbeiten können
            quoteEvent.fireAsync(new InitialQuoteEvent(quote));

        } catch (Exception e) {
            logger.errorv(e, "Fehler beim Parsen der initialen Quote-Nachricht");
        }
    }

    // METHODE 4: Verarbeitet Delta-Nachrichten (Format: "22:49032.7196395:3:::::")
    private void handleDeltaMessage(String message) {
        try {
            // Parse die Delta-Nachricht
            DeltaQuote deltaQuote = DeltaQuote.parse(message);

            // Hole den SymbolKey für diese Subscription-ID
            SymbolKey key = subMap.get(deltaQuote.subId());

            if (key == null) {
                logger.warnv("Keine SymbolKey für Subscription-ID {0} gefunden",
                        deltaQuote.subId());
                return;
            }

            logger.debugv("Delta empfangen - SubID: {0}, Neuer Preis: {1}",
                    deltaQuote.subId(), deltaQuote.value());

            // Feuere das Delta-Event
            quoteDeltaEvent.fireAsync(new QuoteDeltaEvent(deltaQuote));

        } catch (Exception e) {
            logger.errorv(e, "Fehler beim Verarbeiten der Delta-Nachricht: {0}", message);
        }
    }

    // METHODE 5: Wird aufgerufen, wenn die Verbindung geschlossen wird
    @OnClose
    public void onClose(Session session) {
        logger.warn("Verbindung zum Stock3-Server wurde geschlossen!");
        subMap.clear();
    }

    // Diese Methode generiert zufällige Test-Daten (kann später entfernt werden)
    @Scheduled(every = "5s")
    public void generateRandomData() {
        if (lastValue == null) {
            long currentTimeUnixInSeconds = System.currentTimeMillis() / 1000;
            Quote q = new Quote(
                    new SymbolKey("133979", "98", "bid"),
                    currentTimeUnixInSeconds,
                    12000.00,
                    12200.00,
                    11800.00,
                    11950.00,
                    11800.00,
                    200.00,
                    0.016949153,
                    0.01,
                    true,
                    560L,
                    1,
                    2.0
            );
            quoteEvent.fireAsync(new InitialQuoteEvent(q));
            lastValue = 12000.00;
        }

        lastValue += (Math.random() - 0.5) * 100;
        DeltaQuote dq = new DeltaQuote(1, lastValue, 5L, 15L, null, null, null, null);
        quoteDeltaEvent.fireAsync(new QuoteDeltaEvent(dq));
    }
}