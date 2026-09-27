"""
Test suite per la coreografia BSPL PurchaseWithDelivery su architettura MCP v2 ad Hub centrale.

Include test per:
1. Scenario completo con esito positivo (Accept -> Ship -> Deliver)
2. Scenario alternativo con rifiuto (Reject) e non coinvolgimento dello Shipper
3. Concorrenza e isolamento dello stato tra transazioni multiple indipendenti
4. Validazione degli schemi sintattici tramite JSON Schema / Pydantic di MCP
5. Enforcement delle regole di viabilità LoST in emissione da parte dell'Hub (in, out, nil)
6. Gestione dell'invocazione di get_next_message su coda vuota
7. Gestione dell'idempotenza su messaggi duplicati identici
"""

import asyncio
import pytest
import pytest_asyncio
from mcp import Client
from config import HUB_URL, ROLE_BUYER, ROLE_SELLER, ROLE_SHIPPER
from hub import CentralHubServer, BSPLViabilityError, BSPLExecutionError
from roles import BuyerClient, SellerClient, ShipperClient


@pytest_asyncio.fixture
async def running_environment():
    """Fixture che avvia in background l'Hub centrale e i tre client di ruolo."""
    hub = CentralHubServer()
    buyer = BuyerClient()
    seller = SellerClient()
    shipper = ShipperClient()

    t_hub = asyncio.create_task(hub.run())
    await hub.wait_ready()

    t_buyer = asyncio.create_task(buyer.run())
    t_seller = asyncio.create_task(seller.run())
    t_shipper = asyncio.create_task(shipper.run())

    await asyncio.gather(
        buyer.wait_ready(),
        seller.wait_ready(),
        shipper.wait_ready(),
    )

    yield hub, buyer, seller, shipper

    buyer.stop()
    seller.stop()
    shipper.stop()
    hub.stop()

    t_buyer.cancel()
    t_seller.cancel()
    t_shipper.cancel()
    await asyncio.gather(t_hub, t_buyer, t_seller, t_shipper, return_exceptions=True)


@pytest.mark.asyncio
async def test_purchase_happy_path(running_environment):
    """Verifica il percorso felice con accettazione e consegna."""
    hub, buyer, seller, shipper = running_environment
    tx_id = "TEST-ORDER-ACCEPT-01"
    item = "MacBook Pro M3 Max"
    price = 1500.0
    address = "Via Roma 10, 20121 Milano, Italia"

    # 1. Buyer -> Seller: rfq (out ID, out item)
    await buyer.send_rfq(ID=tx_id, item=item)

    # 2. Seller riceve notifica RFQ e invia quote (in ID, in item, out price)
    await seller.wait_for_message("rfq", tx_id)
    await seller.send_quote(ID=tx_id, price=price)

    # 3. Buyer riceve notifica Quote e invia accept (in ID, in item, in price, out address, out response)
    await buyer.wait_for_message("quote", tx_id)
    await buyer.send_accept(ID=tx_id, address=address, response="accepted")

    # 4. Seller riceve notifica Accept e invia ship (in ID, in item, in address)
    await seller.wait_for_message("accept", tx_id)
    await seller.send_ship(ID=tx_id)

    # 5. Shipper riceve notifica Ship e invia deliver (in ID, in item, in address, out outcome)
    await shipper.wait_for_message("ship", tx_id)
    await shipper.send_deliver(ID=tx_id, outcome="delivered")

    # 6. Attesa ricezione notifica di consegna su Buyer
    await buyer.wait_for_message("deliver", tx_id)

    # Verifica stato finale relazioni LoST registrate sull'Hub per il Buyer
    b_hist = hub.adapter.get_history(ROLE_BUYER, tx_id)
    assert b_hist["rfq"] == {"ID": tx_id, "item": item}
    assert b_hist["quote"] == {"ID": tx_id, "item": item, "price": price}
    assert b_hist["accept"]["response"] == "accepted"
    assert b_hist["deliver"]["outcome"] == "delivered"

    # Verifica stato finale relazioni LoST registrate sull'Hub per il Seller
    s_hist = hub.adapter.get_history(ROLE_SELLER, tx_id)
    assert s_hist["rfq"] == {"ID": tx_id, "item": item}
    assert s_hist["quote"] == {"ID": tx_id, "item": item, "price": price}
    assert s_hist["accept"]["address"] == address
    assert s_hist["ship"]["address"] == address

    # Verifica stato finale relazioni LoST registrate sull'Hub per lo Shipper
    sh_hist = hub.adapter.get_history(ROLE_SHIPPER, tx_id)
    assert sh_hist["ship"] == {"ID": tx_id, "item": item, "address": address}
    assert sh_hist["deliver"]["outcome"] == "delivered"

    # Verifica History Vector ricostruito dall'Hub H = [H_Buyer, H_Seller, H_Shipper]
    assert set(b_hist.keys()) == {"rfq", "quote", "accept", "deliver"}
    assert set(s_hist.keys()) == {"rfq", "quote", "accept", "ship"}
    assert set(sh_hist.keys()) == {"ship", "deliver"}


@pytest.mark.asyncio
async def test_purchase_reject_path(running_environment):
    """Verifica il percorso alternativo con rifiuto del preventivo da parte del Buyer."""
    hub, buyer, seller, shipper = running_environment
    tx_id = "TEST-ORDER-REJECT-01"
    item = "Overpriced Item"
    price = 5000.0

    # 1. Buyer -> Seller: rfq
    await buyer.send_rfq(ID=tx_id, item=item)

    # 2. Seller riceve notifica RFQ e invia quote
    await seller.wait_for_message("rfq", tx_id)
    await seller.send_quote(ID=tx_id, price=price)

    # 3. Buyer decide di rifiutare ed invia 'reject'
    await buyer.wait_for_message("quote", tx_id)
    await buyer.send_reject(ID=tx_id, outcome="rejected", response="rejected")

    # 4. Seller attende la notifica del rifiuto
    await seller.wait_for_message("reject", tx_id)

    # Verifica stato Buyer e Seller
    b_hist = hub.adapter.get_history(ROLE_BUYER, tx_id)
    s_hist = hub.adapter.get_history(ROLE_SELLER, tx_id)
    assert b_hist["reject"]["response"] == "rejected"
    assert b_hist["reject"]["outcome"] == "rejected"
    assert s_hist["reject"]["response"] == "rejected"
    assert s_hist["reject"]["outcome"] == "rejected"

    # Verifica che lo Shipper NON abbia alcuna relazione registrata per questa transazione
    sh_hist = hub.adapter.get_history(ROLE_SHIPPER, tx_id)
    assert len(sh_hist) == 0, "Lo Shipper non doveva ricevere alcun ordine di spedizione"


@pytest.mark.asyncio
async def test_concurrent_transactions_isolation(running_environment):
    """Verifica l'esecuzione concorrente di più transazioni indipendenti correlate per ID."""
    hub, buyer, seller, shipper = running_environment

    orders = [
        ("CONC-001", "Item A", 100.0, "Address A"),
        ("CONC-002", "Item B", 200.0, "Address B"),
        ("CONC-003", "Item C", 300.0, "Address C"),
    ]

    async def execute_transaction(tx_id, item, price, address):
        await buyer.send_rfq(ID=tx_id, item=item)
        await seller.wait_for_message("rfq", tx_id)
        await seller.send_quote(ID=tx_id, price=price)
        await buyer.wait_for_message("quote", tx_id)
        await buyer.send_accept(ID=tx_id, address=address, response="accepted")
        await seller.wait_for_message("accept", tx_id)
        await seller.send_ship(ID=tx_id)
        await shipper.wait_for_message("ship", tx_id)
        await shipper.send_deliver(ID=tx_id, outcome="delivered")
        await buyer.wait_for_message("deliver", tx_id)

    # Esecuzione in parallelo delle 3 transazioni
    await asyncio.gather(*(execute_transaction(*order) for order in orders))

    # Verifica dell'isolamento dei dati nelle tabelle relazionali dell'Hub
    for tx_id, item, expected_price, address in orders:
        b_hist = hub.adapter.get_history(ROLE_BUYER, tx_id)
        s_hist = hub.adapter.get_history(ROLE_SELLER, tx_id)
        sh_hist = hub.adapter.get_history(ROLE_SHIPPER, tx_id)

        assert b_hist["rfq"]["item"] == item
        assert b_hist["quote"]["price"] == expected_price
        assert b_hist["deliver"]["outcome"] == "delivered"

        assert s_hist["quote"]["price"] == expected_price
        assert s_hist["ship"]["address"] == address

        assert sh_hist["deliver"]["outcome"] == "delivered"


@pytest.mark.asyncio
async def test_tool_schema_validation(running_environment):
    """Verifica che il server MCP validi la presenza e il tipo dei parametri obbligatori."""
    hub, buyer, seller, shipper = running_environment

    # Chiamata a 'rfq' con parametri mancanti (manca 'item') direttamente all'Hub
    async with Client(HUB_URL) as client:
        result = await client.call_tool("rfq", {"ID": "INVALID-SCHEMA-01"})
        assert result.is_error is True, "L'Hub MCP avrebbe dovuto rifiutare la chiamata con schema invalido"
        assert "item" in str(result.content).lower()


@pytest.mark.asyncio
async def test_viability_emission_check(running_environment):
    """Verifica l'enforcement delle regole di viabilità BSPL in fase di emissione (in, out, nil)."""
    hub, buyer, seller, shipper = running_environment
    tx_id = "TEST-VIABILITY-01"
    item = "Test Phone"

    # 1. Tentativo illegale: Seller prova ad inviare Quote prima dell'arrivo di RFQ
    # (manca il parametro [in] 'item' nello stato locale del Seller sull'Hub)
    with pytest.raises(BSPLExecutionError) as exc_info:
        await seller.send_quote(ID=tx_id, price=300.0)
    assert "non ancora noto" in str(exc_info.value)
    assert any(param in str(exc_info.value) for param in ["ID", "item"])

    # Ora eseguiamo regolarmente RFQ e Quote
    await buyer.send_rfq(ID=tx_id, item=item)
    await seller.wait_for_message("rfq", tx_id)
    await seller.send_quote(ID=tx_id, price=300.0)
    await buyer.wait_for_message("quote", tx_id)

    # Buyer invia Accept (vincolando response="accepted")
    await buyer.send_accept(ID=tx_id, address="Via Test 1", response="accepted")

    # 2. Tentativo illegale: Buyer prova a inviare Reject dopo aver già accettato (mutua esclusione su response)
    with pytest.raises(BSPLExecutionError) as exc_info2:
        await buyer.send_reject(ID=tx_id, outcome="rejected", response="rejected")
    assert "già vincolato" in str(exc_info2.value)
    assert "response" in str(exc_info2.value)

    # 3. Verifica controllo parametri [nil]: se un parametro nil è vincolato, l'emissione deve fallire
    with pytest.raises(BSPLViabilityError) as exc_info3:
        # simuliamo un controllo di viabilità con nil_params=["item"] (dove item è già noto)
        hub.adapter.check_viability(role=ROLE_BUYER, ID=tx_id, nil_params=["item"])
    assert "parametro [nil]" in str(exc_info3.value)


@pytest.mark.asyncio
async def test_get_next_message_empty_queue(running_environment):
    """Verifica che invocare get_next_message con coda vuota restituisca esito empty senza errori."""
    hub, buyer, seller, shipper = running_environment

    async with Client(HUB_URL) as client:
        res = await client.call_tool("get_next_message", {"role": ROLE_BUYER})
        assert res.is_error is False
        assert not res.content or len(res.content) == 0


@pytest.mark.asyncio
async def test_idempotent_duplicate_handling(running_environment):
    """Verifica che l'invio di messaggi duplicati identici sia idempotente e non generi errori."""
    hub, buyer, seller, shipper = running_environment
    tx_id = "TEST-DUP-01"
    item = "Duplicated Item"

    # Buyer invia la prima RFQ
    await buyer.send_rfq(ID=tx_id, item=item)
    await seller.wait_for_message("rfq", tx_id)
    assert "rfq" in hub.adapter.get_history(ROLE_BUYER, tx_id)
    assert hub.adapter.get_history(ROLE_BUYER, tx_id)["rfq"]["ID"] == tx_id

    # Invio di una seconda RFQ identica (simulazione ritrasmissione di rete)
    res = await buyer.send_rfq(ID=tx_id, item=item)
    assert "già registrato" in str(res.content).lower()

    # Lo stato in R(rfq) rimane consistente e non alterato
    assert hub.adapter.get_history(ROLE_BUYER, tx_id)["rfq"]["item"] == item
