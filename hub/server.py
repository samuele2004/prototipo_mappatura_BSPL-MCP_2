"""
Server MCP Centrale (Hub) per la coreografia BSPL PurchaseWithDelivery.

In questa architettura a stella (Modello 2 della tesi):
- L'Hub centrale espone i Tool per tutti i messaggi previsti dal protocollo BSPL:
  * rfq(ID, item) [Buyer -> Seller]
  * quote(ID, price) [Seller -> Buyer]
  * accept(ID, address, response) [Buyer -> Seller]
  * reject(ID, outcome, response) [Buyer -> Seller]
  * ship(ID) [Seller -> Shipper]
  * deliver(ID, outcome) [Shipper -> Buyer]
- I Tool accettano solo la chiave ID e i parametri [out] generati dal mittente;
  l'adattatore centrale risolve e valida i parametri [in] dallo stato del mittente.
- L'Hub mantiene le code FIFO per ciascun ruolo destinatario e pubblica le notifiche
  di aggiornamento sulla risorsa inbox://{role} tramite Context.notify_resource_updated.
- L'Hub espone il Tool get_next_message(role) per consentire ai client di prelevare
  i messaggi e aggiornare conseguentemente lo stato relazionale del destinatario.
"""

import asyncio
from collections import deque
import logging
from typing import Annotated, Any, Dict, Optional, TypedDict
from pydantic import Field
import uvicorn
from mcp.server import MCPServer
from mcp.server.mcpserver import Context

from config import (
    HUB_HOST,
    HUB_PORT,
    ROLE_BUYER,
    ROLE_SELLER,
    ROLE_SHIPPER,
    ROLES,
    get_inbox_uri,
)
from hub.adapter import BSPLHubAdapter

logger = logging.getLogger("CentralHubServer")


class MessagePayload(TypedDict):
    """Rappresentazione tipata del messaggio estratto dalla coda inbox dell'hub."""
    schema: Annotated[str, Field(description="Nome dello schema BSPL del messaggio")]
    params: Annotated[Dict[str, Any], Field(description="Parametri associati al messaggio")]


class CentralHubServer:
    """
    Server MCP Hub centrale con adattatore LoST e code di messaggi per ruolo.
    """

    def __init__(self, host: str = HUB_HOST, port: int = HUB_PORT):
        self.host = host
        self.port = port
        self.server = MCPServer("BSPLCentralHub")
        self.adapter = BSPLHubAdapter()
        self.uvicorn_server: Optional[uvicorn.Server] = None

        # Code di messaggi per ciascun ruolo destinatario
        self.message_queues: Dict[str, deque] = {role: deque() for role in ROLES}

        # Registrazione delle risorse e dei tool MCP
        self._register_resources()
        self._register_tools()

    # ----------------------------------------------------------------------
    # Risorse MCP (Segnali di notifica inbox per i ruoli)
    # ----------------------------------------------------------------------

    def _register_resources(self):
        """Registra la risorsa inbox://{role} a cui i client si sottoscrivono."""

        @self.server.resource("inbox://{role}")
        def inbox(role: str) -> str:
            """
            Risorsa di comodo associata al ruolo: funge da segnale di avviso
            per l'arrivo di nuovi messaggi nella coda del destinatario.
            """
            count = len(self.message_queues.get(role, []))
            return f"Inbox per {role}: {count} messaggi in attesa."

    # ----------------------------------------------------------------------
    # Tool MCP (Emissione messaggi BSPL e prelievo)
    # ----------------------------------------------------------------------

    def _register_tools(self):
        """Registra i Tool per i messaggi BSPL e il Tool get_next_message."""

        # 1. Messaggio: rfq [out ID, out item]
        # Mittente: Buyer -> Destinatario: Seller
        @self.server.tool()
        async def rfq(
            ID: Annotated[str, Field(description="[BSPL: out key] Identificativo univoco della transazione generato dal Buyer")],
            item: Annotated[str, Field(description="[BSPL: out] Articolo richiesto dal Buyer")],
            ctx: Context,
        ) -> str:
            """
            Messaggio BSPL: Buyer -> Seller: rfq [out ID, out item]
            Riceve la Request For Quote dal Buyer e la inoltra al Seller.
            """
            logger.info(f"[Hub] Ricevuta invocazione Tool 'rfq': ID={ID!r}, item={item!r}")
            params = {"ID": ID, "item": item}

            # Controllo duplicati idempotenti per il mittente
            if self.adapter.is_duplicate(ROLE_BUYER, "rfq", ID, params):
                logger.info(f"[Hub] Messaggio 'rfq' già registrato per ID={ID!r} (duplicato idempotente)")
                return f"Messaggio 'rfq' già registrato per ID={ID}."

            # Verifica di viabilità LoST sullo stato del Buyer
            self.adapter.check_viability(
                role=ROLE_BUYER,
                ID=ID,
                in_params=[],
                out_params=["ID", "item"],
            )

            # Inserimento nella relazione locale del mittente
            self.adapter.insert_relation(ROLE_BUYER, "rfq", ID, params)

            # Inserimento nella coda del destinatario (Seller)
            self.message_queues[ROLE_SELLER].append({"schema": "rfq", "params": params})

            # Notifica di aggiornamento della risorsa del destinatario
            await ctx.notify_resource_updated(get_inbox_uri(ROLE_SELLER))
            logger.info(f"[Hub] 'rfq' accodato per {ROLE_SELLER} e notifica inviata su {get_inbox_uri(ROLE_SELLER)}")

            return f"RFQ registrata per ID={ID} e accodata per {ROLE_SELLER}."

        # 2. Messaggio: quote [in ID, in item, out price]
        # Mittente: Seller -> Destinatario: Buyer
        @self.server.tool()
        async def quote(
            ID: Annotated[str, Field(description="[BSPL: in key] Identificativo univoco della transazione")],
            price: Annotated[float, Field(description="[BSPL: out] Prezzo quotato dal Seller")],
            ctx: Context,
        ) -> str:
            """
            Messaggio BSPL: Seller -> Buyer: quote [in ID, in item, out price]
            Riceve la quotazione dal Seller e la inoltra al Buyer.
            """
            logger.info(f"[Hub] Ricevuta invocazione Tool 'quote': ID={ID!r}, price={price}")

            # Controllo duplicati idempotenti prima della verifica di viabilità
            if self.adapter.is_duplicate(ROLE_SELLER, "quote", ID, {"price": price}):
                logger.info(f"[Hub] Messaggio 'quote' già registrato per ID={ID!r} (duplicato idempotente)")
                return f"Messaggio 'quote' già registrato per ID={ID}."

            # Verifica di viabilità LoST sullo stato del Seller (ID e item devono essere noti, price deve essere out)
            in_values = self.adapter.check_viability(
                role=ROLE_SELLER,
                ID=ID,
                in_params=["ID", "item"],
                out_params=["price"],
            )
            params = {**in_values, "price": price}

            # Inserimento nella relazione locale del mittente
            self.adapter.insert_relation(ROLE_SELLER, "quote", ID, params)

            # Inserimento nella coda del destinatario (Buyer)
            self.message_queues[ROLE_BUYER].append({"schema": "quote", "params": params})

            # Notifica al Buyer
            await ctx.notify_resource_updated(get_inbox_uri(ROLE_BUYER))
            logger.info(f"[Hub] 'quote' accodato per {ROLE_BUYER} e notifica inviata su {get_inbox_uri(ROLE_BUYER)}")

            return f"Quote registrata per ID={ID} con prezzo {price} e accodata per {ROLE_BUYER}."

        # 3. Messaggio: accept [in ID, in item, in price, out address, out response]
        # Mittente: Buyer -> Destinatario: Seller
        @self.server.tool()
        async def accept(
            ID: Annotated[str, Field(description="[BSPL: in key] Identificativo univoco della transazione")],
            address: Annotated[str, Field(description="[BSPL: out] Indirizzo di consegna specificato dal Buyer")],
            ctx: Context,
            response: Annotated[str, Field(description="[BSPL: out] Risposta di accettazione ('accepted')")] = "accepted",
        ) -> str:
            """
            Messaggio BSPL: Buyer -> Seller: accept [in ID, in item, in price, out address, out response]
            Riceve l'accettazione dal Buyer e la inoltra al Seller.
            """
            logger.info(f"[Hub] Ricevuta invocazione Tool 'accept': ID={ID!r}, address={address!r}, response={response!r}")

            # Controllo duplicati idempotenti prima della verifica di viabilità
            if self.adapter.is_duplicate(ROLE_BUYER, "accept", ID, {"address": address, "response": response}):
                logger.info(f"[Hub] Messaggio 'accept' già registrato per ID={ID!r} (duplicato idempotente)")
                return f"Messaggio 'accept' già registrato per ID={ID}."

            # Verifica di viabilità LoST sullo stato del Buyer
            in_values = self.adapter.check_viability(
                role=ROLE_BUYER,
                ID=ID,
                in_params=["ID", "item", "price"],
                out_params=["address", "response"],
            )
            params = {**in_values, "address": address, "response": response}

            # Inserimento nella relazione locale del mittente
            self.adapter.insert_relation(ROLE_BUYER, "accept", ID, params)

            # Inserimento nella coda del destinatario (Seller)
            self.message_queues[ROLE_SELLER].append({"schema": "accept", "params": params})

            # Notifica al Seller
            await ctx.notify_resource_updated(get_inbox_uri(ROLE_SELLER))
            logger.info(f"[Hub] 'accept' accodato per {ROLE_SELLER} e notifica inviata su {get_inbox_uri(ROLE_SELLER)}")

            return f"Accettazione registrata per ID={ID} e accodata per {ROLE_SELLER}."

        # 4. Messaggio: reject [in ID, in item, in price, out outcome, out response]
        # Mittente: Buyer -> Destinatario: Seller
        @self.server.tool()
        async def reject(
            ID: Annotated[str, Field(description="[BSPL: in key] Identificativo univoco della transazione")],
            ctx: Context,
            outcome: Annotated[str, Field(description="[BSPL: out] Esito negativo ('rejected')")] = "rejected",
            response: Annotated[str, Field(description="[BSPL: out] Risposta di rifiuto ('rejected')")] = "rejected",
        ) -> str:
            """
            Messaggio BSPL: Buyer -> Seller: reject [in ID, in item, in price, out outcome, out response]
            Riceve il rifiuto dal Buyer e lo inoltra al Seller.
            """
            logger.info(f"[Hub] Ricevuta invocazione Tool 'reject': ID={ID!r}, outcome={outcome!r}, response={response!r}")

            # Controllo duplicati idempotenti prima della verifica di viabilità
            if self.adapter.is_duplicate(ROLE_BUYER, "reject", ID, {"outcome": outcome, "response": response}):
                logger.info(f"[Hub] Messaggio 'reject' già registrato per ID={ID!r} (duplicato idempotente)")
                return f"Messaggio 'reject' già registrato per ID={ID}."

            # Verifica di viabilità LoST sullo stato del Buyer
            in_values = self.adapter.check_viability(
                role=ROLE_BUYER,
                ID=ID,
                in_params=["ID", "item", "price"],
                out_params=["outcome", "response"],
            )
            params = {**in_values, "outcome": outcome, "response": response}

            # Inserimento nella relazione locale del mittente
            self.adapter.insert_relation(ROLE_BUYER, "reject", ID, params)

            # Inserimento nella coda del destinatario (Seller)
            self.message_queues[ROLE_SELLER].append({"schema": "reject", "params": params})

            # Notifica al Seller
            await ctx.notify_resource_updated(get_inbox_uri(ROLE_SELLER))
            logger.info(f"[Hub] 'reject' accodato per {ROLE_SELLER} e notifica inviata su {get_inbox_uri(ROLE_SELLER)}")

            return f"Rifiuto registrato per ID={ID} e accodato per {ROLE_SELLER}."

        # 5. Messaggio: ship [in ID, in item, in address]
        # Mittente: Seller -> Destinatario: Shipper
        @self.server.tool()
        async def ship(
            ID: Annotated[str, Field(description="[BSPL: in key] Identificativo univoco della transazione")],
            ctx: Context,
        ) -> str:
            """
            Messaggio BSPL: Seller -> Shipper: ship [in ID, in item, in address]
            Riceve l'ordine di spedizione dal Seller e lo inoltra allo Shipper.
            Tutti i parametri sono [in], risolti dallo stato del Seller.
            """
            logger.info(f"[Hub] Ricevuta invocazione Tool 'ship': ID={ID!r}")

            # Controllo duplicati idempotenti prima della verifica di viabilità
            if self.adapter.is_duplicate(ROLE_SELLER, "ship", ID, {}):
                logger.info(f"[Hub] Messaggio 'ship' già registrato per ID={ID!r} (duplicato idempotente)")
                return f"Messaggio 'ship' già registrato per ID={ID}."

            # Verifica di viabilità LoST sullo stato del Seller
            in_values = self.adapter.check_viability(
                role=ROLE_SELLER,
                ID=ID,
                in_params=["ID", "item", "address"],
                out_params=[],
            )
            params = dict(in_values)

            # Inserimento nella relazione locale del mittente
            self.adapter.insert_relation(ROLE_SELLER, "ship", ID, params)

            # Inserimento nella coda del destinatario (Shipper)
            self.message_queues[ROLE_SHIPPER].append({"schema": "ship", "params": params})

            # Notifica allo Shipper
            await ctx.notify_resource_updated(get_inbox_uri(ROLE_SHIPPER))
            logger.info(f"[Hub] 'ship' accodato per {ROLE_SHIPPER} e notifica inviata su {get_inbox_uri(ROLE_SHIPPER)}")

            return f"Spedizione registrata per ID={ID} e accodata per {ROLE_SHIPPER}."

        # 6. Messaggio: deliver [in ID, in item, in address, out outcome]
        # Mittente: Shipper -> Destinatario: Buyer
        @self.server.tool()
        async def deliver(
            ID: Annotated[str, Field(description="[BSPL: in key] Identificativo univoco della transazione")],
            ctx: Context,
            outcome: Annotated[str, Field(description="[BSPL: out] Esito finale della consegna ('delivered')")] = "delivered",
        ) -> str:
            """
            Messaggio BSPL: Shipper -> Buyer: deliver [in ID, in item, in address, out outcome]
            Riceve la notifica di consegna dallo Shipper e la inoltra al Buyer.
            """
            logger.info(f"[Hub] Ricevuta invocazione Tool 'deliver': ID={ID!r}, outcome={outcome!r}")

            # Controllo duplicati idempotenti prima della verifica di viabilità
            if self.adapter.is_duplicate(ROLE_SHIPPER, "deliver", ID, {"outcome": outcome}):
                logger.info(f"[Hub] Messaggio 'deliver' già registrato per ID={ID!r} (duplicato idempotente)")
                return f"Messaggio 'deliver' già registrato per ID={ID}."

            # Verifica di viabilità LoST sullo stato dello Shipper
            in_values = self.adapter.check_viability(
                role=ROLE_SHIPPER,
                ID=ID,
                in_params=["ID", "item", "address"],
                out_params=["outcome"],
            )
            params = {**in_values, "outcome": outcome}

            # Inserimento nella relazione locale del mittente
            self.adapter.insert_relation(ROLE_SHIPPER, "deliver", ID, params)

            # Inserimento nella coda del destinatario (Buyer)
            self.message_queues[ROLE_BUYER].append({"schema": "deliver", "params": params})

            # Notifica al Buyer
            await ctx.notify_resource_updated(get_inbox_uri(ROLE_BUYER))
            logger.info(f"[Hub] 'deliver' accodato per {ROLE_BUYER} e notifica inviata su {get_inbox_uri(ROLE_BUYER)}")

            return f"Consegna registrata per ID={ID} e accodata per {ROLE_BUYER}."

        # 7. Tool di prelievo messaggi: get_next_message
        @self.server.tool()
        async def get_next_message(
            role: Annotated[str, Field(description="Ruolo del client che richiede il messaggio dalla propria coda inbox")],
        ) -> Optional[MessagePayload]:
            """
            Estrae il prossimo messaggio in testa alla coda del ruolo chiamante.
            Al momento dell'estrazione, la tupla del messaggio viene registrata
            nello stato relazionale locale del destinatario all'interno dell'adattatore.
            Restituisce un payload tipato con schema e parametri, oppure None se la coda è vuota.
            """
            queue = self.message_queues.get(role)
            if queue is None or len(queue) == 0:
                logger.debug(f"[Hub] Nessun messaggio in coda per il ruolo {role!r}")
                return None

            msg = queue.popleft()
            schema = msg["schema"]
            params = msg["params"]
            ID = params["ID"]

            # Registrazione della tupla ricevuta nella relazione locale del destinatario
            self.adapter.insert_relation(role, schema, ID, params)
            logger.info(
                f"[Hub] Messaggio '{schema}' prelevato da {role!r} per ID={ID!r}. "
                f"Stato relazionale di {role!r} aggiornato con {params}"
            )

            return {
                "schema": schema,
                "params": params,
            }

    # ----------------------------------------------------------------------
    # Ciclo di Vita del Server HTTP (Streamable HTTP tramite uvicorn)
    # ----------------------------------------------------------------------

    async def wait_ready(self, timeout: float = 5.0):
        """Attende che il server HTTP sia in ascolto e pronto a ricevere richieste."""
        start = asyncio.get_event_loop().time()
        while not getattr(self.uvicorn_server, "started", False):
            if asyncio.get_event_loop().time() - start > timeout:
                raise TimeoutError(f"Il server Hub non è pronto entro {timeout}s")
            await asyncio.sleep(0.05)

    async def run(self):
        """Avvia il Server MCP Hub su trasporto Streamable HTTP."""
        app = self.server.streamable_http_app()
        config = uvicorn.Config(app, host=self.host, port=self.port, log_level="warning")
        self.uvicorn_server = uvicorn.Server(config)
        try:
            await self.uvicorn_server.serve()
        except asyncio.CancelledError:
            pass

    def stop(self):
        """Arresta in modo ordinato il server HTTP."""
        if self.uvicorn_server:
            self.uvicorn_server.should_exit = True
