package de.berlin.htw.boundary.ws.client;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;

import de.berlin.htw.boundary.ws.dto.SubEvent;
import de.berlin.htw.boundary.ws.dto.UnsubEvent;
import de.berlin.htw.trading.quote.dto.SymbolKey;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;

@Startup  // Diese Annotation sorgt dafür, dass die Bean beim Start initialisiert wird
@ApplicationScoped
public class QuoteController {

    private Session session;
    private Set<SymbolKey> subscriptions = ConcurrentHashMap.newKeySet();

    @Inject
    Logger logger;

    @Inject
    QuoteClient quoteClient;  // Injiziere den QuoteClient

    @PostConstruct
    public void start() {
        logger.info("QuoteController wird gestartet...");

        try {
            // WICHTIG: Erstelle den WebSocketContainer
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();

            // Verbinde dich mit dem Stock3-Server
            URI uri = URI.create("wss://quotepush.stock3.com/delta");
            logger.infov("Verbinde mit Stock3-Server: {0}", uri);

            // Erstelle die Session mit dem QuoteClient
            this.session = container.connectToServer(quoteClient, uri);

            logger.info("Erfolgreich mit Stock3-Server verbunden!");

        } catch (Exception e) {
            logger.error("Fehler beim Verbinden mit Stock3-Server", e);
            // Beende die Anwendung, wenn die Verbindung fehlschlägt
            System.exit(1);
        }

        logger.info("QuoteController erfolgreich gestartet.");
    }

    // Diese Methode wird aufgerufen, wenn ein Frontend-Client eine Aktie abonniert
    protected void subscribe(@Observes SubEvent ev) {
        // Prüfe, ob wir diese Aktie schon abonniert haben
        if (this.subscriptions.add(ev.key())) {
            logger.infov("Abonniere Kurse für {0}", ev.key());

            // Sende die Subscribe-Nachricht an Stock3
            // Format: "a" + symbolId + ":" + venueId + ":" + channel
            // Beispiel: a133962:22:last
            String message = ev.toMessage();
            logger.infov("Sende an Stock3: {0}", message);

            this.session.getAsyncRemote().sendText(message);
        } else {
            logger.infov("Bereits abonniert: {0}", ev.key());
        }
    }

    // Diese Methode wird aufgerufen, wenn ein Frontend-Client ein Abo beendet
    protected void unsubscribe(@Observes UnsubEvent ev) {
        // Prüfe, ob wir diese Aktie abonniert haben
        if (this.subscriptions.remove(ev.key())) {
            logger.infov("Beende Abo für {0}", ev.key());

            // Sende die Unsubscribe-Nachricht an Stock3
            // Format: "r" + symbolId + ":" + venueId + ":" + channel
            // Beispiel: r133962:22:last
            String message = ev.toMessage();
            logger.infov("Sende an Stock3: {0}", message);

            this.session.getAsyncRemote().sendText(message);
        } else {
            logger.infov("Nicht abonniert: {0}", ev.key());
        }
    }
}