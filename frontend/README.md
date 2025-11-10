# Market Viewer
This frontend application allows your users to view real-time market data and manage their subscriptions to various financial assets. It is built using Vue.js and leverages the Naive UI component library for a sleek and responsive user interface.

Currently, the frontend is not working correctly due to a missing websocket connection to the backend server. You need to implement the websocket connection in the backend server to enable real-time data updates.

## Websocket Connection - What to Implement
Das Frontend besitzt bereits alle notwendigen Komponenten, um eine Websocket-Verbindung herzustellen und Daten in Echtzeit zu empfangen.
Aus diesem Grund muss die Backend-Implementierung der Websocket-Verbindung auf die bereits bestehenden Schnittstelle und dessen Datenformate im Frontend abgestimmt werden.
Stellen Sie sicher, dass die Websocket-Verbindung folgende Anforderungen erfüllt:
- **Endpoint**: Der Websocket-Server muss unter dem Pfad erreichbar sein, welchen Sie in Ihrer .env.local Datei unter VITE_WS_URL konfiguriert haben.
- **Datenformat**: Die gesendeten und empfangenen Nachrichten müssen dem in der Frontend-Dokumentation beschriebenen Format entsprechen. Sie finden die entsprechenden Typendefinitionen in der Datei `frontend/src/types/trading.ts`. Außerdem sollten Sie sich an der bestehenden Implementierung in der Datei `frontend/src/services/ws.ts` orientieren.


## Getting Started
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