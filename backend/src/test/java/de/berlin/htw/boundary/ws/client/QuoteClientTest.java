package de.berlin.htw.boundary.ws.client;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

import de.berlin.htw.trading.quote.dto.DeltaQuote;
import de.berlin.htw.trading.quote.dto.Quote;
import de.berlin.htw.trading.quote.dto.SymbolKey;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

@QuarkusTest
public class QuoteClientTest {

    @Inject
    QuoteController quoteController;

    /**
     * TEST 1: QuoteController startet erfolgreich
     */
    @Test
    public void testQuoteControllerInitialization() {
        // Assert: QuoteController wurde injiziert
        assertNotNull(quoteController, "QuoteController sollte injiziert sein");
    }

    /**
     * TEST 2: DeltaQuote-Parsing funktioniert
     */
    @Test
    public void testDeltaQuoteParsing() {
        // Beispiel einer Delta-Nachricht vom Stock3-Server
        String deltaMessage = "22:49032.7196395:3:5::::";

        // Parse die Nachricht
        DeltaQuote delta = DeltaQuote.parse(deltaMessage);

        // Assert: Alle Felder wurden korrekt geparst
        assertNotNull(delta, "DeltaQuote sollte nicht null sein");
        assertEquals(22, delta.subId(), "SubId sollte 22 sein");
        assertEquals(49032.7196395, delta.value(), 0.0001, "Value sollte korrekt sein");
        assertEquals(3L, delta.secSinceLastMessage(), "Sekunden sollten 3 sein");
        assertEquals(5L, delta.tickDelta(), "TickDelta sollte 5 sein");
    }

    /**
     * TEST 3: SymbolKey wird aus Subscription-String erstellt
     */
    @Test
    public void testSymbolKeyFromSubscription() {
        // Beispiel: "133962:22:last"
        String subscriptionString = "133962:22:last";

        // Erstelle SymbolKey
        SymbolKey key = SymbolKey.fromSub(subscriptionString);

        // Assert: Alle Felder wurden korrekt extrahiert
        assertNotNull(key, "SymbolKey sollte nicht null sein");
        assertEquals("133962", key.symbolId, "SymbolId sollte korrekt sein");
        assertEquals("22", key.venueId, "VenueId sollte korrekt sein");
        assertEquals("last", key.channel, "Channel sollte korrekt sein");
    }

    /**
     * TEST 4: Quote.applyDelta funktioniert korrekt
     */
    @Test
    public void testQuoteApplyDelta() {
        // Erstelle ein initiales Quote
        SymbolKey key = new SymbolKey("133979", "98", "bid");
        Quote initial = new Quote(
                key,
                System.currentTimeMillis() / 1000,
                12000.0,  // price
                12200.0,  // high
                11800.0,  // low
                11950.0,  // open
                11800.0,  // prevClose
                200.0,    // abs
                0.016949, // rel
                0.01,     // tickSize
                true,     // active
                100L,     // tick
                1,        // subId
                2.0       // precision
        );

        // Erstelle ein Delta (neuer Preis: 12050.0, +2 Ticks)
        DeltaQuote delta = new DeltaQuote(
                1,        // subId
                12050.0,  // neuer Preis
                5L,       // 5 Sekunden später
                2L,       // +2 Ticks
                null,     // kein neues High
                null,     // kein neues Low
                null,
                null
        );

        // Wende das Delta an
        Quote updated = Quote.applyDelta(key, initial, delta, "bid");

        // Assert: Das neue Quote wurde korrekt berechnet
        assertNotNull(updated, "Updated Quote sollte nicht null sein");
        assertEquals(12050.0, updated.price(), 0.01, "Preis sollte aktualisiert sein");
        assertEquals(102L, updated.tick(), "Tick sollte um 2 erhöht sein");
        assertEquals(initial.tsUnixSec() + 5, updated.tsUnixSec(),
                "Zeitstempel sollte um 5 Sekunden erhöht sein");
    }

    /**
     * TEST 5: SubEvent wird korrekt in Subscribe-Message konvertiert
     */
    @Test
    public void testSubEventToMessage() {
        SymbolKey key = new SymbolKey("133962", "22", "last");
        de.berlin.htw.boundary.ws.dto.SubEvent event =
                new de.berlin.htw.boundary.ws.dto.SubEvent(key);

        String message = event.toMessage();

        // Assert: Die Nachricht hat das richtige Format
        assertEquals("a133962:22:last", message,
                "Subscribe-Message sollte korrekt formatiert sein");
    }

    /**
     * TEST 6: UnsubEvent wird korrekt in Unsubscribe-Message konvertiert
     */
    @Test
    public void testUnsubEventToMessage() {
        SymbolKey key = new SymbolKey("133962", "22", "last");
        de.berlin.htw.boundary.ws.dto.UnsubEvent event =
                new de.berlin.htw.boundary.ws.dto.UnsubEvent(key);

        String message = event.toMessage();

        // Assert: Die Nachricht hat das richtige Format
        assertEquals("r133962:22:last", message,
                "Unsubscribe-Message sollte korrekt formatiert sein");
    }
}