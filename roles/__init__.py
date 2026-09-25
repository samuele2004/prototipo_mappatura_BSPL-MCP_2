"""
Package roles: esportazione dei client MCP per i ruoli della coreografia.
"""

from roles.base import BaseRoleClient
from roles.buyer import BuyerClient
from roles.seller import SellerClient
from roles.shipper import ShipperClient

__all__ = [
    "BaseRoleClient",
    "BuyerClient",
    "SellerClient",
    "ShipperClient",
]
