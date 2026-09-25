# Prototipo: Mappatura BSPL su MCP (Modello a Hub Centrale)

Questo repository contiene il prototipo di riferimento per la mappatura del protocollo BSPL **`PurchaseWithDelivery`** sull'infrastruttura **Model Context Protocol (MCP)**, utilizzando l'SDK ufficiale per Python (`mcp>=2.0.0`) su trasporto **Streamable HTTP**.

Il prototipo realizza il **secondo modello di mappatura (Hub Centrale)** definito nel Capitolo 3 (Sezione 3.2) della tesi di laurea:
- Un **unico Server MCP centrale (Hub)** gestisce l'intera infrastruttura: esposizione dei Tool dei messaggi BSPL, code FIFO per i destinatari, emissione di notifiche su risorse dedicate (`inbox://{role}`) e integrazione dell'adattatore LoST centralizzato.
- I partecipanti (**Buyer**, **Seller**, **Shipper**) operano come **Client MCP puri** (*thin client*), connettendosi all'Hub e sottoscrivendosi alla propria risorsa inbox via `client.listen()`.
- Lo stato relazionale locale di ciascun ruolo è tracciato in modo indipendente all'interno dell'adattatore dell'Hub, con tabelle separate $R(m)$ per ciascun ruolo $x$ e schema $m$.
- L'emissione dei messaggi è vincolata ai **controlli formali di viabilità LoST**: l'Hub verifica che tutti i parametri `[in]` siano già vincolati nello stato del mittente e ne estrae i valori, controllando che nessun parametro `[out]` o `[nil]` risulti già vincolato.
- La consegna verso il destinatario avviene in due passaggi: notifica `ResourceUpdated` sulla risorsa inbox del destinatario -> prelievo del messaggio tramite il Tool `get_next_message(role)`. All'atto del prelievo, la tupla viene registrata nello stato del destinatario.

---

## 1. Il Protocollo BSPL di Riferimento

```bspl
PurchaseWithDelivery {
    role Buyer, Seller, Shipper
    parameter out ID key, out item, out price, out outcome

    Buyer -> Seller: rfq[out ID, out item]
    Seller -> Buyer: quote[in ID, in item, out price]
    Buyer -> Seller: accept[in ID, in item, in price, out address, out response]
    Buyer -> Seller: reject[in ID, in item, in price, out outcome, out response]

    Seller -> Shipper: ship[in ID, in item, in address]
    Shipper -> Buyer: deliver[in ID, in item, in address, out outcome]
}
```

---

## 2. Architettura a Stella del Prototipo

```
                    ┌────────────────────────────────────────┐
                    │            BSPL Central Hub            │
                    │         (Streamable HTTP :8000)        │
                    │                                        │
                    │  ┌──────────────────────────────────┐  │
                    │  │      LoST Central Adapter        │  │
                    │  │  - Buyer Relations               │  │
                    │  │  - Seller Relations              │  │
                    │  │  - Shipper Relations             │  │
                    │  │  - check_viability(role, ID, ...)│  │
                    │  └──────────────────────────────────┘  │
                    │                                        │
                    │  ┌──────────────────────────────────┐  │
                    │  │      Queues & Notification       │  │
                    │  │  - inbox://Buyer   (Queue)       │  │
                    │  │  - inbox://Seller  (Queue)       │  │
                    │  │  - inbox://Shipper (Queue)       │  │
                    │  │  - ctx.notify_resource_updated   │  │
                    │  └──────────────────────────────────┘  │
                    └───────────────────┬────────────────────┘
                                        │
            ┌───────────────────────────┼───────────────────────────┐
            │                           │                           │
            ▼                           ▼                           ▼
      [Buyer Client]              [Seller Client]             [Shipper Client]
      - listen("inbox://Buyer")   - listen("inbox://Seller")  - listen("inbox://Shipper")
      - send_rfq, accept, reject  - send_quote, ship          - send_deliver
```

1. **Adattatore LoST Centralizzato**:
   - Mantiene lo stato relazionale separato per ciascun ruolo partecipante ($R(m)$ per ciascun ruolo $x$).
   - Esegue la verifica formale di viabilità LoST prima di accodare ogni messaggio.
   - Fornisce l'estrazione della storia locale per la ricostruzione dell'History Vector $H = [H_{Buyer}, H_{Seller}, H_{Shipper}]$.
2. **Tool dei Messaggi sull'Hub**:
   - I Tool esposti dall'Hub richiedono unicamente la chiave `ID` e i parametri `[out]` generati dal mittente.
   - I parametri `[in]` vengono verificati e risolti direttamente dallo stato locale del mittente all'interno dell'Hub, azzerando il rischio di incongruenze o manipolazioni da parte del client.
3. **Consegna con Primitiva Risorse e Tool `get_next_message`**:
   - Ciascun client si sottoscrive alla risorsa `inbox://{role}`.
   - All'arrivo di una notifica `ResourceUpdated`, il client invoca `get_next_message(role=...)`.
   - L'Hub preleva la comunicazione dalla coda FIFO e aggiorna contestualmente lo stato relazionale del destinatario, rendendo i nuovi parametri disponibili per le sue successive comunicazioni.

---

## 3. Tabella dei Messaggi, Tool e Metodi Send

| Messaggio BSPL | Mittente | Destinatario | Tool MCP (Hub) | Parametri Input Tool (`[out]` + ID) | Parametri Risolti dall'Hub (`[in]`) | Metodo Client (`send_*`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `rfq` | Buyer | Seller | `rfq` | `ID: [out key]`, `item: [out]` | *(nessuno)* | `buyer.send_rfq(ID, item)` |
| `quote` | Seller | Buyer | `quote` | `ID: [in key]`, `price: [out]` | `item: [in]` | `seller.send_quote(ID, price)` |
| `accept` | Buyer | Seller | `accept` | `ID: [in key]`, `address: [out]`, `response: [out]` | `item: [in]`, `price: [in]` | `buyer.send_accept(ID, address, response)` |
| `reject` | Buyer | Seller | `reject` | `ID: [in key]`, `outcome: [out]`, `response: [out]` | `item: [in]`, `price: [in]` | `buyer.send_reject(ID, outcome, response)` |
| `ship` | Seller | Shipper | `ship` | `ID: [in key]` | `item: [in]`, `address: [in]` | `seller.send_ship(ID)` |
| `deliver` | Shipper | Buyer | `deliver` | `ID: [in key]`, `outcome: [out]` | `item: [in]`, `address: [in]` | `shipper.send_deliver(ID, outcome)` |
| *(Prelievo)* | *(qualsiasi)* | *(Hub)* | `get_next_message` | `role: str` | *(estrae da coda e aggiorna stato)* | *(gestito dal loop di ascolto del client)* |

---

## 4. Struttura del Repository

```
.
├── config.py                 # Configurazione endpoint Hub (:8000), ruoli e funzione get_inbox_uri
├── LICENSE                   # Licenza open-source MIT
├── main.py                   # Simulazione del protocollo e ispezione relazioni LoST sull'Hub
├── pytest.ini                # Configurazione per pytest-asyncio
├── requirements.txt          # Dipendenze Python (mcp>=2.0.0, uvicorn, pytest)
├── hub/
│   ├── __init__.py           # Export di server, adattatore ed eccezioni BSPL
│   ├── adapter.py            # BSPLHubAdapter (stato relazionale multi-ruolo e check_viability)
│   ├── exceptions.py         # Gerarchia eccezioni BSPL derivata da ToolError
│   └── server.py             # CentralHubServer (MCPServer, risorse inbox, tool BSPL, get_next_message)
├── roles/
│   ├── __init__.py           # Export dei client dei ruoli
│   ├── base.py               # BaseRoleClient (Client MCP, ascolto inbox, invocazione tool)
│   ├── buyer.py              # BuyerClient
│   ├── seller.py             # SellerClient
│   └── shipper.py            # ShipperClient
├── tests/
│   ├── __init__.py
│   └── test_choreography.py  # Test suite (Happy path, Reject, Concorrenza, Schemi, Viabilità, Idempotenza)
└── README.md
```

---

## 5. Installazione ed Esecuzione

### Prerequisiti
- Python 3.10+ (consigliato Python 3.12 o 3.13)
- Ambiente virtuale configurato

```bash
# Creazione e attivazione del virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Installazione delle dipendenze
pip install -r requirements.txt
```

### Esecuzione della Simulazione Principale
```bash
python3 main.py
```

### Esecuzione dei Test Automatizzati
```bash
pytest tests/ -v
```
