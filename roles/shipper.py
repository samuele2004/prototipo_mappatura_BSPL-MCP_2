"""
Implementazione del client per il ruolo 'Shipper' per il protocollo BSPL PurchaseWithDelivery.

In questa architettura (Modello 2 della tesi):
- Lo Shipper è un Client MCP puro che si connette all'Hub centrale.
- EMISSIONE (Invocazione dei Tool dell'Hub):
  * send_deliver(ID, outcome) [Shipper -> Buyer: deliver]
- RICEZIONE:
  * Riceve notifiche dall'Hub su inbox://Shipper quando Seller emette 'ship',
    prelevando automaticamente la comunicazione tramite get_next_message.
"""

import asyncio
import logging
from config import HUB_URL, ROLE_SHIPPER
from roles.base import BaseRoleClient

logger = logging.getLogger("Shipper")


class ShipperClient(BaseRoleClient):
    """
    Client MCP puro per il ruolo Shipper.
    Espone il metodo pubblico di emissione send_deliver verso l'Hub centrale.
    """

    def __init__(self, hub_url: str = HUB_URL):
        super().__init__(name=ROLE_SHIPPER, hub_url=hub_url)

    # ----------------------------------------------------------------------
    # Metodi Pubblici di Invio (Emissione messaggi BSPL tramite Tool dell'Hub)
    # ----------------------------------------------------------------------

    async def send_deliver(self, ID: str, outcome: str = "delivered"):
        """
        Messaggio BSPL: Shipper -> Buyer: deliver [in ID, in item, in address, out outcome]
        Invia la notifica di avvenuta consegna invocando il Tool 'deliver' sull'Hub centrale.
        Fornisce solo la chiave ID e il parametro [out] outcome;
        i parametri [in] (item, address) sono verificati e risolti dall'Hub dallo stato dello Shipper.
        """
        await asyncio.sleep(0.05)
        params = {"ID": ID, "outcome": outcome}
        logger.info(f"[Shipper -> Hub] Invocazione Tool 'deliver': ID={ID!r}, outcome={outcome!r}")
        result = await self.call_hub_tool("deliver", params)
        logger.info(f"[Shipper] Risposta dall'Hub per 'deliver': {result.content}")
        return result
