# Verteilte Anwendungen Übungsaufgabe 2

Mögliche Punktzahl: 10 Punkte

## Deadlines

- 1. Zug: 27.11.2025
- 2. Zug: 27.11.2025

## Aufgabenstellung
In dieser Aufgabe erhalten Sie ein vorkonfiguriertes Projekt, das aus zwei Komponenten besteht:

- **Frontend:** Eine Single-Page-Application programmiert mit Vue.js
- **Backend:** Eine Quarkus Applikation, die Daten für das Frontend bereitstellt

Das Frontend ist bereits vollständig implementiert und muss nicht verändert werden. Das Backend hingegen ist nur teilweise implementiert
und soll im Rahmen dieser Übungsaufgabe erweitert werden.
Ziel dieser Übung ist es, dass Sie ein tiefes Verständnis für Websocket-Kommunikation, sowohl Client- als auch Serverseitig, erlangen.
Außerdem soll Ihnen diese Übung zusätzliche Erfahrung im Umgang mit Quarkus vermitteln, indem verschiedene Pattern und Konzepte
des Jakarta EE Standards umgesetzt werden.
Ihre Aufgabe ist die Entwicklung einer Trading-Applikation, die es ermöglicht, Aktienkurse in Echtzeit zu verfolgen.
In dieser Übung gibt es KEINE Datenbank und KEINE Rest-API. Die gesamte Kommunikation zwischen Frontend und Backend erfolgt über Websockets.

Das Backend ist im wesentlichen in zwei Teile gegliedert:
1. **Boundary:** Diese Schicht stellt die Kommunikation mit dem Frontend, sowie mit externen Diensten sicher. Hierzu zählen die Websocket-Endpunkte, sowohl als Server, als auch als Client. Sie finden hier bereits alle notwendigen Dateien. Einige der Dateien sind allerdings nur teilweise implementiert und müssen von Ihnen vervollständigt werden.
2. **Trading:** Diese Schicht stellt die Geschäftslogik der Applikation dar. Hier werden die Aktienkurse verwaltet und aktualisiert. Diese Schicht ist im Wesentlichen bereits vollständig implementiert. Sie müssen hier nur wenige Änderungen vornehmen, um die Aufgabe komplett zu lösen.

## Aufgabenstellung
1. (2P) Ihnen ist sicherlich aufgefallen, dass das Frontend aktuell keine Verbindung zum Backend aufbauen kann. Dies liegt daran, dass der Websocket-Endpunkt im Backend noch nicht implementiert ist. Implementieren Sie den Websocket-Server-Endpunkt in der Klasse `WebsocketServer.java`, sodass das Frontend eine Verbindung herstellen kann. Orientieren Sie sich hierzu am Frontend-Code. Sie finden dort die erwarteten Datenformate und Message-Definitionen. Nutzen Sie die bereits vorhandenen Methoden aus dem `WebsocketServer`.
2. (4P) Nachdem Sie das Frontend erfolgreich mit dem Backend verbunden haben, und dieses nun Aktienkurse abonnieren kann, müssen Sie sich als Nächstes darum kümmern, dass das Backend auch tatsächlich Aktienkurse empfängt und an die Clients weiterleitet. Implementieren Sie hierzu den Websocket-Client. Dazu stehen Ihnen die Dateien `QuoteClient.java` und `QuoteController.java` zur Verfügung.
Um die Aktienkurse zu empfangen, brauchen Sie natürlich auch eine Verbindung zu einem externen Dienst. Glücklicherweise stellt uns [Stock3](stock3.com) in Zusammenarbeit mit [brokerize](brokerize.com) einen kostenlosen Websocket-Dienst zur Verfügung, über den Sie Echtzeit-Aktienkurse empfangen können.
Die URL für den Websocket-Dienst lautet: `wss://quotepush.stock3.com/delta`. Weiter unten in dieser Datei finden Sie eine kurze Dokumentation, wie Sie den Dienst nutzen können. Nutzen Sie diese Informationen, um den Websocket-Client fertigzustellen.
3. (2P) Nachdem Sie den Websocket-Client erfolgreich implementiert haben, sollten Sie in der Lage sein, Echtzeit-Aktienkurse zu empfangen und an die verbundenen Frontend-Clients weiterzuleiten. Ihnen ist vielleicht bereits aufgefallen, dass Sie zwar Candlesticks empfangen und betrachten können, der Kursticker im Frontend jedoch mit leeren Werten befüllt wird und Sie auch keine Line-Charts betrachten können. Dies liegt daran, dass Ihnen wichtige Bestandteile im `SimpleQuoteConsumer.java` fehlen. Vervollständigen Sie diese Klasse so, dass der Kursticker und die Line-Charts korrekt mit Daten befüllt werden. Nehmen Sie sich dazu ein Beispiel an der bereits implementierten `CandleQuoteConsumer.java`.
4. (2P) Selbst der beste Code hat Fehler. Daher ist es wichtig, dass Sie Ihre Implementierung ausreichend testen. Schreiben Sie mindestens 3 Integration-Tests für den `WebsocketServer`, um die Funktionalität Ihres Websocket-Servers zu überprüfen. Schreiben Sie außerdem mindestens 3 Integration-Tests für den `QuoteClient`, um die Funktionalität Ihres Websocket-Clients zu überprüfen.


# Quarkus Get Started

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw quarkus:dev
```

## Related Guides

- WebSockets Client ([guide](https://quarkus.io/guides/websockets)): Client for WebSocket communication channel
- WebSockets ([guide](https://quarkus.io/guides/websockets)): WebSocket communication channel support

### WebSockets

WebSocket communication channel starter code

[Related guide section...](https://quarkus.io/guides/websockets)

# Frontend

To run the frontend application locally, follow these steps:
1. **Install Dependencies**: Navigate to the `frontend` directory and install the required dependencies using npm:
   ```bash
   npm install
   ```
2. **Environment Variables**: Create a `.env.local` file in the `frontend` directory to configure environment-specific variables. You can use the provided `.env.local.example` as a template
3. **Run the Application**: Start the development server with the following command:
   ```bash
   npm run dev
   ```
4. **Access the Application**: Open your web browser and navigate to `http://localhost:5173` to view the application.

# Stock3 WebSocket API Documentation

Um Echtzeit-Aktienkurse von Stock3 zu empfangen, müssen Sie eine WebSocket-Verbindung zu folgendem Endpunkt herstellen:

```wss://quotepush.stock3.com/delta```

### Aktienkurse abonnieren
Um Aktienkurse zu abonnieren, müssen Sie eine Nachricht im folgenden Format senden:

```text
SYMBOLID:VENUEID:CHANNEL
```

`SYMBOLID` ist die eindeutige Kennung der Aktie, `VENUEID` die Handelsplattform (z.B. XETRA, NASDAQ) und `CHANNEL` der gewünschte Datenkanal (`last` oder `bid`).

Dabei muss Ihre Nachricht mit einem `a` als Präfix für eine Abonnement-Nachricht starten:

```text
a133962:22:last
```
Sie können auch mehrere Aktien gleichzeitig abonnieren, indem Sie die Subskriptionsnachrichten per Komma trennen:

```text
a133962:22:last,133965:119:last,133954:119:last,133955:119:last,133958:119:last,133979:98:bid,134000:27:bid,23087055:117:last,23087058:117:last,133978:98:bid,134018:119:last
```

### Aktienkurse abbestellen
Um Aktienkurse abzubestellen, müssen Sie eine Nachricht im folgenden Format senden:

```text
SYMBOLID:VENUEID:CHANNEL
```

Dabei muss Ihre Nachricht mit einem `r` als Präfix für eine Abbestellungs-Nachricht starten:

```text
r133962:22:last
```
Sie können auch mehrere Aktien gleichzeitig abbestellen, indem Sie die Abbestellungsnachrichten per Komma trennen:  

```text
r133962:22:last,133965:119:last,133954:119:last,133955:119:last,133958:119:last,133979:98:bid,134000:27:bid,23087055:117:last,23087058:117:last,133978:98:bid,134018:119:last
```
### Empfangene Nachrichten
HINWEIS: Manchmal kann vor dieser Nachricht einmalig, beim Verbinden mit dem Websocket-Server, eine Willkommensnachricht empfangen werden. Diese Nachricht beinhaltet unter anderem:
```text
[stock3-
```
und kann entsprechend gefiltert werden.

Wenn Sie eine Aktie abonnieren, erhalten Sie ein einziges mal eine Nachrichten im folgenden JSON-Format:

```json
{
"q":24267.0,
"h":24358.5,
"l":24251.5,
"pc":24330.03,
"o":24313.5,
"ts":1761117803,
"t":2469,
"tickSize":0.5,
"precision":2,
"abs":-63.03,
"rel":-0.0025906,
"active":true,
"i":6,
"s":"133962:22:last"
}
```

q: aktueller Kurs  
h: Tageshoch  
l: Tagestief  
pc: Vortageskurs  
o: Eröffnungskurs  
ts: Zeitstempel in Sekunden in Unix Epoch  
t: Ticknummer  
tickSize: Tickgröße  
precision: Nachkommastellen  
abs: absoluter Kursunterschied zum Vortag  
rel: relativer Kursunterschied zum Vortag  
active: ist die Börse gerade geöffnet (sonst gibt es meist keine Updates)
i: Eine eindeutige Subscription-ID+
s: Der abonnierte Symbol-String (was Sie zuvor in der Abonnement-Nachricht gesendet haben)

Anschließend erhalten Sie nur noch sogenannte Delta-Nachrichten, wenn sich der Kurs ändert. Diese Nachrichten haben folgendes Format:

```text
22:49032.7196395:3:::::
```
Die Felder sind durch Doppelpunkte getrennt und haben folgende Bedeutung:
1. Feld: Die eindeutige Subscription-ID (i) aus der vorherigen Nachricht  
2. Feld: Der neue Kurs (q)  
3. Sekunden seit der letzten Nachricht
4. Anzahl der Ticks seit der letzten Nachricht
5. (optional) Neues Tageshoch (h), wenn es aktualisiert wurde
6. (optional) Neues Tagestief (l), wenn es aktualisiert wurde
7. Kann ignoriert werden (optional, fehlt manchmal)
8. Kann ignoriert werden (optional, fehlt manchmal)

## Liste von Aktien-Symbolen
Folgende Symbole sind im Frontend bereits vorkonfiguriert und können direkt verwendet werden:

| Asset             | Symbol ID  | Venue ID| Channel |
|-------------------|------------|---------|---------|
| DAX               | 133962     | 22      | last   |
| DOW JONES         | 133965     | 119     | last   |
| S&P 500           | 133954     | 119     | last   |
| NASDAQ 100        | 133955     | 119     | last   |
| NIKKEI225         | 133958     | 119     | last   |
| GOLD              | 133979     | 98      | bid    |
| EUR/USD           | 134000     | 27      | bid    |
| BTC/USD           | 23087055   | 117     | last   |
| ETH/USD           | 23087058   | 117     | last   |
| BRENT CRUDE ÖL    | 133978     | 98      | bid    |
| EURO BOND FUTURES | 134018     | 119     | last    |


Durch Reverse-Engineering des [Stock3-Terminals](https://terminal.stock3.com/) lassen sich jedoch sehr einfach weitere Symbole finden.
Sie können das Terminal außerdem verwenden um die Funktionsweise des Websocket-Dienstes zu verstehen und zu testen, sowie Ihre Ergebnisse zu validieren.