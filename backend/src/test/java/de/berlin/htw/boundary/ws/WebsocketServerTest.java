package de.berlin.htw.boundary.ws;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;

import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class WebsocketServerTest {

    private Session clientSession;
    private TestWebSocketClient testClient;

    /**
     * Einfacher WebSocket-Client für Tests
     */
    @ClientEndpoint
    public static class TestWebSocketClient {
        // Queue zum Sammeln empfangener Nachrichten
        public LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();
        public boolean connected = false;

        @OnOpen
        public void onOpen(Session session) {
            connected = true;
            System.out.println("Test-Client verbunden: " + session.getId());
        }

        @OnMessage
        public void onMessage(String message) {
            System.out.println("Test-Client empfing: " + message);
            messages.add(message);
        }

        @OnClose
        public void onClose() {
            connected = false;
            System.out.println("Test-Client getrennt");
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        // Erstelle einen Test-Client
        testClient = new TestWebSocketClient();

        // Verbinde mit dem WebSocket-Server
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        URI uri = URI.create("ws://localhost:8080/quotes");
        clientSession = container.connectToServer(testClient, uri);

        // Warte, bis die Verbindung hergestellt ist
        Thread.sleep(500);
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (clientSession != null && clientSession.isOpen()) {
            clientSession.close();
        }
    }

    /**
     * TEST 1: Verbindung kann hergestellt werden
     */
    @Test
    public void testWebSocketConnection() throws Exception {
        // Assert: Der Client ist verbunden
        assertTrue(testClient.connected, "Client sollte verbunden sein");
        assertTrue(clientSession.isOpen(), "Session sollte offen sein");
    }

    /**
     * TEST 2: Ping-Pong funktioniert
     */
    @Test
    public void testPingPong() throws Exception {
        // Sende eine Ping-Nachricht
        String pingMessage = "{\"type\":\"ping\"}";
        clientSession.getAsyncRemote().sendText(pingMessage);

        // Warte auf die Pong-Antwort
        String response = testClient.messages.poll(5, TimeUnit.SECONDS);

        // Assert: Wir haben eine Antwort bekommen
        assertNotNull(response, "Sollte Pong-Antwort erhalten");
        assertTrue(response.contains("pong"), "Antwort sollte 'pong' enthalten");
    }

    /**
     * TEST 3: Subscribe-Nachricht wird verarbeitet
     */
    @Test
    public void testSubscribe() throws Exception {
        // Sende eine Subscribe-Nachricht für Gold
        String subscribeMessage =
                "{\"action\":\"subscribe\",\"symbolId\":\"133979\",\"venueId\":\"98\",\"channel\":\"bid\",\"window\":3600}";
        clientSession.getAsyncRemote().sendText(subscribeMessage);

        // Warte auf Antworten
        Thread.sleep(2000);

        // Assert: Wir sollten mindestens eine Nachricht bekommen haben
        assertFalse(testClient.messages.isEmpty(),
                "Sollte mindestens eine Nachricht nach Subscribe erhalten");

        // Die erste Nachricht sollte Candles oder Quotes sein
        String firstMessage = testClient.messages.poll();
        assertTrue(firstMessage.contains("candles") || firstMessage.contains("quotes"),
                "Erste Nachricht sollte candles oder quotes sein");
    }

    /**
     * TEST 4: Unsubscribe-Nachricht wird verarbeitet
     */
    @Test
    public void testUnsubscribe() throws Exception {
        // Erst abonnieren
        String subscribeMessage =
                "{\"action\":\"subscribe\",\"symbolId\":\"133979\",\"venueId\":\"98\",\"channel\":\"bid\",\"window\":3600}";
        clientSession.getAsyncRemote().sendText(subscribeMessage);
        Thread.sleep(1000);

        // Leere die Message-Queue
        testClient.messages.clear();

        // Dann wieder abbestellen
        String unsubscribeMessage =
                "{\"action\":\"unsubscribe\",\"symbolId\":\"133979\",\"venueId\":\"98\",\"channel\":\"bid\"}";
        clientSession.getAsyncRemote().sendText(unsubscribeMessage);

        // Warte kurz
        Thread.sleep(1000);

        // Assert: Nach Unsubscribe sollten keine neuen Nachrichten mehr kommen
        // (außer die Bestätigung)
        assertTrue(testClient.messages.size() <= 1,
                "Nach Unsubscribe sollten keine weiteren Nachrichten kommen");
    }

    /**
     * TEST 5: Mehrere Clients können gleichzeitig verbunden sein
     */
    @Test
    public void testMultipleClients() throws Exception {
        // Erstelle einen zweiten Client
        TestWebSocketClient testClient2 = new TestWebSocketClient();
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        URI uri = URI.create("ws://localhost:8080/quotes");
        Session clientSession2 = container.connectToServer(testClient2, uri);

        Thread.sleep(500);

        try {
            // Assert: Beide Clients sind verbunden
            assertTrue(testClient.connected, "Client 1 sollte verbunden sein");
            assertTrue(testClient2.connected, "Client 2 sollte verbunden sein");

            // Sende Ping an beide
            clientSession.getAsyncRemote().sendText("{\"type\":\"ping\"}");
            clientSession2.getAsyncRemote().sendText("{\"type\":\"ping\"}");

            Thread.sleep(500);

            // Assert: Beide sollten Pong erhalten
            assertFalse(testClient.messages.isEmpty(), "Client 1 sollte Antwort erhalten");
            assertFalse(testClient2.messages.isEmpty(), "Client 2 sollte Antwort erhalten");

        } finally {
            if (clientSession2.isOpen()) {
                clientSession2.close();
            }
        }
    }

    /**
     * TEST 6: Ungültige Nachrichten werden behandelt
     */
    @Test
    public void testInvalidMessage() throws Exception {
        // Sende ungültige JSON-Nachricht
        String invalidMessage = "{invalid json}";
        clientSession.getAsyncRemote().sendText(invalidMessage);

        // Warte auf Antwort
        String response = testClient.messages.poll(5, TimeUnit.SECONDS);

        // Assert: Wir sollten eine Fehlermeldung erhalten
        assertNotNull(response, "Sollte Fehlerantwort erhalten");
        assertTrue(response.contains("error"), "Antwort sollte Fehler enthalten");
    }
}