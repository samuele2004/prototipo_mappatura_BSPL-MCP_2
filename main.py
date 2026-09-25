"""
Script Principale: Simulazione della Coreografia BSPL 'PurchaseWithDelivery' su MCP v2.

Questo script simula l'interazione tra i tre ruoli (Buyer, Seller, Shipper) secondo
il modello di mappatura a Hub centrale (Modello 2 della tesi):
1. Avvia l'unico Server MCP centrale (Hub) su trasporto Streamable HTTP (:8000).
2. Connette i tre Client MCP (Buyer, Seller, Shipper), avviando le rispettive sottoscrizioni
   alla risorsa di avviso inbox://{role}.
3. Coordina i passaggi della transazione invocando i metodi di invio pubblici dei ruoli
   (passando solo la chiave ID e i parametri [out] generati; i parametri [in]
   sono risolti e verificati automaticamente dall'adattatore LoST centrale dell'Hub):
   * Step 1: Buyer   --[rfq(ID, item)]-------------------------> Hub -> Seller
   * Step 2: Seller  --[quote(ID, price)]----------------------> Hub -> Buyer
   * Step 3: Buyer   --[accept(ID, address, response)]---------> Hub -> Seller
   * Step 4: Seller  --[ship(ID)]------------------------------> Hub -> Shipper
   * Step 5: Shipper --[deliver(ID, outcome)]------------------> Hub -> Buyer
4. Ispeziona e stampa lo stato finale delle relazioni locali LoST R(m) mantenute sull'Hub per ciascun ruolo.
5. Convalida la consistenza dell'History Vector distribuito H = [H_Buyer, H_Seller, H_Shipper]
   e arresta client e server in modo pulito.
"""

import asyncio
import json
import logging
from config import ROLE_BUYER, ROLE_SELLER, ROLE_SHIPPER
from hub import CentralHubServer
from roles import BuyerClient, SellerClient, ShipperClient

# Configurazione del logging console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Simulation")


async def run_choreography_scenario():
    logger.info("=" * 75)
    logger.info("   SIMULAZIONE PROTOCOLLO BSPL 'PurchaseWithDelivery' (HUB CENTRALE MCP v2)")
    logger.info("=" * 75)

    # 1. Istanziazione dell'Hub centrale e dei client dei ruoli
    hub = CentralHubServer()
    buyer = BuyerClient()
    seller = SellerClient()
    shipper = ShipperClient()

    # 2. Avvio dell'Hub centrale in background
    t_hub = asyncio.create_task(hub.run())
    logger.info("Avvio del Server MCP centrale (Hub su porta :8000)...")
    await hub.wait_ready()
    logger.info("Hub centrale attivo e pronto su Streamable HTTP.\n")

    # 3. Avvio dei task di ascolto inbox dei tre ruoli
    t_buyer = asyncio.create_task(buyer.run())
    t_seller = asyncio.create_task(seller.run())
    t_shipper = asyncio.create_task(shipper.run())

    try:
        # Attesa attivazione di tutte le sottoscrizioni client
        logger.info("Connessione dei client e sottoscrizione alle rispettive inbox...")
        await asyncio.gather(buyer.wait_ready(), seller.wait_ready(), shipper.wait_ready())
        logger.info("Tutti i client sono connessi e in ascolto delle notifiche inbox.\n")

        # Dati specifici dell'istanza di acquisto
        tx_id = "ORDER-2026-001"
        item_name = "MacBook Pro 16 M3 Max"
        offered_price = 1200.0
        delivery_address = "Via Roma 10, 20121 Milano, Italia"

        # ------------------------------------------------------------------
        # FASE 1: Buyer invia RFQ al Seller tramite l'Hub
        # BSPL: Buyer -> Seller: rfq [out ID key, out item]
        # In LoST il mittente fornisce la chiave e i parametri [out]
        # ------------------------------------------------------------------
        logger.info("-" * 75)
        logger.info(f"[FASE 1] Buyer emette RFQ: ID='{tx_id}', item='{item_name}'")
        logger.info("-" * 75)
        await buyer.send_rfq(ID=tx_id, item=item_name)

        # ------------------------------------------------------------------
        # FASE 2: Seller riceve la notifica, preleva RFQ ed invia Quote al Buyer
        # BSPL: Seller -> Buyer: quote [in ID key, in item, out price]
        # Seller fornisce ID e out 'price'; 'item' è risolto dall'Hub dallo stato del Seller
        # ------------------------------------------------------------------
        await seller.wait_for_message("rfq", tx_id)
        logger.info("-" * 75)
        logger.info(f"[FASE 2] Seller ha ricevuto notifica per RFQ ed emette Quote: price={offered_price} EUR")
        logger.info("-" * 75)
        await seller.send_quote(ID=tx_id, price=offered_price)

        # ------------------------------------------------------------------
        # FASE 3: Buyer riceve la notifica, preleva Quote, accetta ed invia Accept al Seller
        # BSPL: Buyer -> Seller: accept [in ID, in item, in price, out address, out response]
        # Buyer fornisce out 'address' e 'response'; [in] 'item' e 'price' risolti dall'Hub
        # ------------------------------------------------------------------
        await buyer.wait_for_message("quote", tx_id)
        logger.info("-" * 75)
        logger.info(f"[FASE 3] Buyer accetta l'offerta ed emette Accept verso Seller")
        logger.info("-" * 75)
        await buyer.send_accept(
            ID=tx_id,
            address=delivery_address,
            response="accepted",
        )

        # ------------------------------------------------------------------
        # FASE 4: Seller riceve notifica, preleva Accept ed invia Ship allo Shipper
        # BSPL: Seller -> Shipper: ship [in ID, in item, in address]
        # Tutti i parametri sono [in], risolti dall'Hub dallo stato del Seller
        # ------------------------------------------------------------------
        await seller.wait_for_message("accept", tx_id)
        logger.info("-" * 75)
        logger.info("[FASE 4] Seller riceve notifica per Accept ed emette ordine di spedizione Ship allo Shipper")
        logger.info("-" * 75)
        await seller.send_ship(ID=tx_id)

        # ------------------------------------------------------------------
        # FASE 5: Shipper riceve notifica, preleva Ship ed invia Deliver al Buyer
        # BSPL: Shipper -> Buyer: deliver [in ID, in item, in address, out outcome]
        # Shipper fornisce out 'outcome'; [in] 'item' e 'address' risolti dall'Hub
        # ------------------------------------------------------------------
        await shipper.wait_for_message("ship", tx_id)
        logger.info("-" * 75)
        logger.info("[FASE 5] Shipper prende in carico la merce ed emette Deliver verso Buyer")
        logger.info("-" * 75)
        await shipper.send_deliver(
            ID=tx_id,
            outcome="delivered",
        )

        # Attesa ricezione notifica di consegna sul Buyer
        await buyer.wait_for_message("deliver", tx_id)
        logger.info("-" * 75)
        logger.info("COREOGRAFIA COMPLETATA CON SUCCESSO SULL'HUB CENTRALE!")
        logger.info("-" * 75)

        # ------------------------------------------------------------------
        # Ispezione dello stato locale relazionale LoST R(m) mantenuto sull'Hub
        # ------------------------------------------------------------------
        print("\n" + "=" * 75)
        print("  STATO DELLE RELAZIONI LOCALI LoST SULL'HUB (R(m) per ciascun Ruolo)")
        print("=" * 75)

        print("\n[BUYER RELATIONS SULL'HUB]:")
        for rel_name, table in hub.adapter._get_role_tables(ROLE_BUYER).items():
            print(f"  R({rel_name}): {json.dumps(table.get(tx_id, {}), ensure_ascii=False)}")

        print("\n[SELLER RELATIONS SULL'HUB]:")
        for rel_name, table in hub.adapter._get_role_tables(ROLE_SELLER).items():
            print(f"  R({rel_name}): {json.dumps(table.get(tx_id, {}), ensure_ascii=False)}")

        print("\n[SHIPPER RELATIONS SULL'HUB]:")
        for rel_name, table in hub.adapter._get_role_tables(ROLE_SHIPPER).items():
            print(f"  R({rel_name}): {json.dumps(table.get(tx_id, {}), ensure_ascii=False)}")
        print("=" * 75)

        # ------------------------------------------------------------------
        # History Vector Distribuito H = [H_Buyer, H_Seller, H_Shipper]
        # ------------------------------------------------------------------
        print("\n" + "=" * 75)
        print(f"  HISTORY VECTOR RICOSTRUITO DALL'HUB H = [H_Buyer, H_Seller, H_Shipper] (ID='{tx_id}')")
        print("=" * 75)
        print(f"  H_Buyer:   {list(hub.adapter.get_history(ROLE_BUYER, tx_id).keys())}")
        print(f"  H_Seller:  {list(hub.adapter.get_history(ROLE_SELLER, tx_id).keys())}")
        print(f"  H_Shipper: {list(hub.adapter.get_history(ROLE_SHIPPER, tx_id).keys())}")
        print("=" * 75 + "\n")

        # ------------------------------------------------------------------
        # Asserzioni di conformità e consistenza dell'History Vector
        # ------------------------------------------------------------------
        b_hist = hub.adapter.get_history(ROLE_BUYER, tx_id)
        s_hist = hub.adapter.get_history(ROLE_SELLER, tx_id)
        sh_hist = hub.adapter.get_history(ROLE_SHIPPER, tx_id)

        # 1. Verifica Buyer
        assert b_hist["rfq"]["item"] == item_name
        assert b_hist["quote"]["price"] == offered_price
        assert b_hist["accept"]["response"] == "accepted"
        assert b_hist["deliver"]["outcome"] == "delivered"

        # 2. Verifica Seller
        assert s_hist["rfq"]["item"] == item_name
        assert s_hist["quote"]["price"] == offered_price
        assert s_hist["accept"]["address"] == delivery_address
        assert s_hist["ship"]["address"] == delivery_address

        # 3. Verifica Shipper
        assert sh_hist["ship"]["address"] == delivery_address
        assert sh_hist["deliver"]["outcome"] == "delivered"

        logger.info("Tutte le asserzioni sull'History Vector centrale sono verificate con successo.")

    finally:
        logger.info("Arresto dei client e dell'Hub MCP...")
        buyer.stop()
        seller.stop()
        shipper.stop()
        hub.stop()

        t_buyer.cancel()
        t_seller.cancel()
        t_shipper.cancel()
        await asyncio.gather(t_hub, t_buyer, t_seller, t_shipper, return_exceptions=True)
        logger.info("Simulazione terminata regolarmente.")


if __name__ == "__main__":
    asyncio.run(run_choreography_scenario())
