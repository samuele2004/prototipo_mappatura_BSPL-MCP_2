"""
Configurazione dell'endpoint di rete e dei ruoli per l'architettura BSPL su MCP a Hub centrale.

In questa implementazione prototipale (Modello 2 della tesi):
- Un unico Server MCP centrale (Hub) opera su trasporto Streamable HTTP su localhost:
  http://127.0.0.1:8000/mcp
- Tutti i partecipanti (Buyer, Seller, Shipper) operano come Client MCP puri,
  connettendosi all'Hub e sottoscrivendosi alla propria risorsa di notifica inbox://{role}.
"""

HUB_HOST = "127.0.0.1"
HUB_PORT = 8000
HUB_URL = f"http://{HUB_HOST}:{HUB_PORT}/mcp"

ROLE_BUYER = "Buyer"
ROLE_SELLER = "Seller"
ROLE_SHIPPER = "Shipper"
ROLES = [ROLE_BUYER, ROLE_SELLER, ROLE_SHIPPER]


def get_inbox_uri(role: str) -> str:
    """Restituisce l'URI della risorsa inbox associata al ruolo specificato."""
    return f"inbox://{role}"
