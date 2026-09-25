"""
Implementazione del client per il ruolo 'Buyer' per il protocollo BSPL PurchaseWithDelivery.

In questa architettura (Modello 2 della tesi):
- Il Buyer è un Client MCP puro che si connette all'Hub centrale.
- EMISSIONE (Invocazione dei Tool dell'Hub):
  * send_rfq(ID, item) [Buyer -> Seller: rfq]
  * send_accept(ID, address, response) [Buyer -> Seller: accept]
  * send_reject(ID, outcome, response) [Buyer -> Seller: reject]
- RICEZIONE:
  * Riceve notifiche dall'Hub su inbox://Buyer quando Seller emette 'quote' o Shipper emette 'deliver',
    prelevando automaticamente la comunicazione tramite get_next_message.
"""

import asyncio
import logging
from config import HUB_URL, ROLE_BUYER
from roles.base import BaseRoleClient

logger = logging.getLogger("Buyer")


class BuyerClient(BaseRoleClient):
    """
    Client MCP puro per il ruolo Buyer.
    Espone i metodi pubblici di emissione verso i Tool dell'Hub centrale.
    """

    def __init__(self, hub_url: str = HUB_URL):
        super().__init__(name=ROLE_BUYER, hub_url=hub_url)

    # ----------------------------------------------------------------------
    # Metodi Pubblici di Invio (Emissione messaggi BSPL tramite Tool dell'Hub)
    # ----------------------------------------------------------------------

    async def send_rfq(self, ID: str, item: str):
        """
        Messaggio BSPL: Buyer -> Seller: rfq [out ID, out item]
        Invia una Request For Quote invocando il Tool 'rfq' sull'Hub centrale.
        """
        params = {"ID": ID, "item": item}
        logger.info(f"[Buyer -> Hub] Invocazione Tool 'rfq': ID={ID!r}, item={item!r}")
        result = await self.call_hub_tool("rfq", params)
        logger.info(f"[Buyer] Risposta dall'Hub per 'rfq': {result.content}")
        return result

    async def send_accept(
        self,
        ID: str,
        address: str,
        response: str = "accepted",
    ):
        """
        Messaggio BSPL: Buyer -> Seller: accept [in ID, in item, in price, out address, out response]
        Invia l'accettazione dell'offerta invocando il Tool 'accept' sull'Hub centrale.
        Fornisce solo la chiave ID e i parametri [out] (address, response);
        i parametri [in] (item, price) sono verificati e risolti dall'Hub.
        """
        await asyncio.sleep(0.05)
        params = {"ID": ID, "address": address, "response": response}
        logger.info(f"[Buyer -> Hub] Invocazione Tool 'accept': ID={ID!r}, address={address!r}, response={response!r}")
        result = await self.call_hub_tool("accept", params)
        logger.info(f"[Buyer] Risposta dall'Hub per 'accept': {result.content}")
        return result

    async def send_reject(
        self,
        ID: str,
        outcome: str = "rejected",
        response: str = "rejected",
    ):
        """
        Messaggio BSPL: Buyer -> Seller: reject [in ID, in item, in price, out outcome, out response]
        Invia il rifiuto dell'offerta invocando il Tool 'reject' sull'Hub centrale.
        Fornisce solo la chiave ID e i parametri [out] (outcome, response);
        i parametri [in] (item, price) sono verificati e risolti dall'Hub.
        """
        await asyncio.sleep(0.05)
        params = {"ID": ID, "outcome": outcome, "response": response}
        logger.info(f"[Buyer -> Hub] Invocazione Tool 'reject': ID={ID!r}, outcome={outcome!r}, response={response!r}")
        result = await self.call_hub_tool("reject", params)
        logger.info(f"[Buyer] Risposta dall'Hub per 'reject': {result.content}")
        return result
