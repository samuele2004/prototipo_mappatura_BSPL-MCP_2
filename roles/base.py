"""
Classe base per un client di ruolo (Client MCP puro) nella coreografia BSPL su Hub centrale.

Incapsula:
- La connessione Client MCP verso l'Hub centrale su trasporto Streamable HTTP.
- L'ascolto reattivo in background della risorsa di notifica inbox://{role} via client.listen().
- Il prelievo dei messaggi dalla coda dell'Hub tramite invocazione del Tool get_next_message.
- La sincronizzazione asincrona reattiva (asyncio.Event) per coordinare test e scenari.
- L'invocazione protetta dei Tool dell'Hub con gestione degli errori remoti (BSPLExecutionError).
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from mcp import Client
from mcp.client.subscriptions import ResourceUpdated

from config import HUB_URL, get_inbox_uri
from hub.exceptions import BSPLExecutionError

logger = logging.getLogger("BaseRoleClient")


class BaseRoleClient:
    """
    Client MCP puro associato a un ruolo della coreografia.
    Non avvia un proprio server HTTP; si connette all'Hub centrale,
    si sottoscrive alla propria inbox e consuma i messaggi notificati.
    """

    def __init__(self, name: str, hub_url: str = HUB_URL):
        self.name = name
        self.hub_url = hub_url
        self.inbox_uri = get_inbox_uri(name)

        self.client: Optional[Client] = None
        self._ready_event = asyncio.Event()
        self._listen_task: Optional[asyncio.Task] = None
        self._running = False

        # Registro dei messaggi ricevuti: lista di dizionari {"schema": ..., "params": ...}
        self.received_messages: List[Dict[str, Any]] = []

        # Mappa per sincronizzazione eventi: (schema_name, transaction_ID) -> asyncio.Event
        self._message_events: Dict[Tuple[str, str], asyncio.Event] = {}

    # ----------------------------------------------------------------------
    # Sincronizzazione a Eventi
    # ----------------------------------------------------------------------

    def _notify_message_received(self, schema_name: str, transaction_id: str):
        """Notifica che uno specifico messaggio BSPL è stato prelevato e reso disponibile."""
        key = (schema_name, transaction_id)
        if key not in self._message_events:
            self._message_events[key] = asyncio.Event()
        self._message_events[key].set()

    async def wait_for_message(self, schema_name: str, transaction_id: str, timeout: float = 10.0) -> bool:
        """
        Attende in modo asincrono che il client riceva un messaggio BSPL per la transazione indicata.
        Ritorna True se l'evento è avvenuto entro il timeout, altrimenti solleva TimeoutError.
        """
        key = (schema_name, transaction_id)
        if key not in self._message_events:
            self._message_events[key] = asyncio.Event()
        try:
            await asyncio.wait_for(self._message_events[key].wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Client '{self.name}' non ha ricevuto il messaggio '{schema_name}' "
                f"per ID='{transaction_id}' entro {timeout}s"
            )

    # ----------------------------------------------------------------------
    # Ciclo di Vita del Client e Ascolto Inbox
    # ----------------------------------------------------------------------

    async def wait_ready(self, timeout: float = 5.0):
        """Attende che il client sia connesso all'Hub e con la sottoscrizione attiva."""
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Client '{self.name}' non è pronto entro {timeout}s")

    async def run(self):
        """
        Mantiene aperta la sessione Client e ascolta le notifiche sulla risorsa inbox://{role}.
        """
        self._running = True
        logger.info(f"[{self.name}] Connessione all'Hub su {self.hub_url}...")
        try:
            async with Client(self.hub_url) as client:
                self.client = client
                logger.info(f"[{self.name}] Sottoscrizione a {self.inbox_uri}...")
                async with client.listen(resource_subscriptions=[self.inbox_uri]) as sub:
                    self._ready_event.set()
                    logger.info(f"[{self.name}] Sottoscrizione attiva e pronta.")
                    async for event in sub:
                        if not self._running:
                            break
                        if isinstance(event, ResourceUpdated) and event.uri == self.inbox_uri:
                            logger.info(f"[{self.name}] Notifica ricevuta per {event.uri}, prelievo messaggi in coda...")
                            await self._fetch_all_pending_messages()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self._running:
                logger.error(f"[{self.name}] Errore imprevisto nel loop di ascolto: {e}", exc_info=True)
        finally:
            self._running = False
            self.client = None

    async def _fetch_all_pending_messages(self):
        """Preleva tutti i messaggi disponibili nella coda dell'Hub fino a svuotarla."""
        while self._running and self.client:
            res = await self.client.call_tool("get_next_message", {"role": self.name})
            if res.is_error:
                logger.error(f"[{self.name}] Errore durante get_next_message: {res.content}")
                break

            if not res.content or len(res.content) == 0:
                break

            text_data = res.content[0].text
            try:
                data = json.loads(text_data)
            except Exception:
                logger.warning(f"[{self.name}] Impossibile parsare output di get_next_message: {text_data}")
                break

            if data.get("status") != "ok":
                break

            schema = data["schema"]
            params = data["params"]
            tx_id = params.get("ID", "")
            self.received_messages.append({"schema": schema, "params": params})
            logger.info(f"[{self.name}] Ricevuto messaggio '{schema}' per ID={tx_id!r}: {params}")
            self._notify_message_received(schema, tx_id)

    async def call_hub_tool(self, name: str, params: Dict[str, Any]) -> Any:
        """
        Invoca un Tool sull'Hub centrale verificando lo stato di risposta.
        Solleva BSPLExecutionError se l'Hub restituisce is_error=True.
        """
        await self.wait_ready()
        if not self.client:
            raise RuntimeError(f"Client '{self.name}' non connesso all'Hub")

        result = await self.client.call_tool(name, params)
        if result.is_error:
            error_msg = str(result.content)
            logger.error(f"❌ [{self.name}] Errore dall'Hub su tool '{name}': {error_msg}")
            raise BSPLExecutionError(f"Errore remoto dall'Hub su '{name}': {error_msg}")

        return result

    def stop(self):
        """Arresta il loop di ascolto del client."""
        self._running = False
