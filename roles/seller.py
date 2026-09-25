"""
Implementazione del client per il ruolo 'Seller' per il protocollo BSPL PurchaseWithDelivery.

In questa architettura (Modello 2 della tesi):
- Il Seller è un Client MCP puro che si connette all'Hub centrale.
- EMISSIONE (Invocazione dei Tool dell'Hub):
  * send_quote(ID, price) [Seller -> Buyer: quote]
  * send_ship(ID) [Seller -> Shipper: ship]
- RICEZIONE:
  * Riceve notifiche dall'Hub su inbox://Seller quando Buyer emette 'rfq', 'accept' o 'reject',
    prelevando automaticamente la comunicazione tramite get_next_message.
"""

import asyncio
import logging
from config import HUB_URL, ROLE_SELLER
from roles.base import BaseRoleClient

logger = logging.getLogger("Seller")


class SellerClient(BaseRoleClient):
    """
    Client MCP puro per il ruolo Seller.
    Espone i metodi pubblici di emissione verso i Tool dell'Hub centrale.
    """

    def __init__(self, hub_url: str = HUB_URL):
        super().__init__(name=ROLE_SELLER, hub_url=hub_url)

    # ----------------------------------------------------------------------
    # Metodi Pubblici di Invio (Emissione messaggi BSPL tramite Tool dell'Hub)
    # ----------------------------------------------------------------------

    async def send_quote(self, ID: str, price: float):
        """
        Messaggio BSPL: Seller -> Buyer: quote [in ID, in item, out price]
        Invia la quotazione di prezzo invocando il Tool 'quote' sull'Hub centrale.
        Fornisce solo la chiave ID e il parametro [out] price;
        il parametro [in] item è verificato e risolto dall'Hub dallo stato del Seller.
        """
        await asyncio.sleep(0.05)
        params = {"ID": ID, "price": price}
        logger.info(f"[Seller -> Hub] Invocazione Tool 'quote': ID={ID!r}, price={price}")
        result = await self.call_hub_tool("quote", params)
        logger.info(f"[Seller] Risposta dall'Hub per 'quote': {result.content}")
        return result

    async def send_ship(self, ID: str):
        """
        Messaggio BSPL: Seller -> Shipper: ship [in ID, in item, in address]
        Invia l'ordine di spedizione invocando il Tool 'ship' sull'Hub centrale.
        Tutti i parametri (item, address) sono [in], verificati e risolti dall'Hub dallo stato del Seller.
        """
        await asyncio.sleep(0.05)
        params = {"ID": ID}
        logger.info(f"[Seller -> Hub] Invocazione Tool 'ship': ID={ID!r}")
        result = await self.call_hub_tool("ship", params)
        logger.info(f"[Seller] Risposta dall'Hub per 'ship': {result.content}")
        return result
