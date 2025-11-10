package de.berlin.htw.boundary.ws.client;

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
import jakarta.websocket.Session;

@Startup
@ApplicationScoped
public class QuoteController {

    private Session session;
    private Set<SymbolKey> subscriptions = ConcurrentHashMap.newKeySet();

    @Inject
    Logger logger;

    @PostConstruct
    public void start() {
        logger.info("Starting QuoteController...");
        try {
            // TODO: ERZEUGEN SIE EINE WEBSOCKET-SESSION MIT DEM QuoteClient UND VERBINDEN
            // SIE SICH
        } catch (Exception e) {
            logger.error("Failed to start QuoteController.", e);
            System.exit(1);
        }

        logger.info("QuoteController started.");
    }

    protected void subscribe(@Observes SubEvent ev) {
        if (this.subscriptions.add(ev.key())) {
            logger.infof("Subscribing to quotes for %s", ev.key());
            this.session.getAsyncRemote().sendText(ev.toMessage());
        } else {
            logger.infof("Already subscribed to quotes for %s", ev.key());
        }

    }

    protected void unsubscribe(@Observes UnsubEvent ev) {
        if (this.subscriptions.remove(ev.key())) {
            logger.infof("Unsubscribing from quotes for %s", ev.key());
            this.session.getAsyncRemote().sendText(ev.toMessage());
        } else {
            logger.infof("Not subscribed to quotes for %s", ev.key());
        }
    }
}
