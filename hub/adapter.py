"""
Adattatore di protocollo BSPL centralizzato basato sul modello LoST (Local State Transfer).

Gestisce:
- Lo stato relazionale separato per ciascun ruolo partecipante:
  relazioni R(m) per ciascun ruolo x e per ciascuno schema di messaggio m.
- Le regole formali di viabilità per l'emissione dei messaggi:
  * verifica e recupero dei parametri [in] dallo stato locale del ruolo mittente;
  * verifica che nessun parametro [out] sia già vincolato per la chiave ID nello stato del mittente;
  * verifica che nessun parametro [nil] sia già vincolato per la chiave ID nello stato del mittente.
- L'aggiornamento dello stato del destinatario al prelievo della comunicazione.
- L'ispezione della storia locale di ciascun ruolo per la ricostruzione dell'History Vector distribuito.
"""

import logging
from typing import Any, Dict, List, Optional
from hub.exceptions import BSPLViabilityError

logger = logging.getLogger("BSPLHubAdapter")


class BSPLHubAdapter:
    """
    Adattatore LoST centralizzato integrato all'interno del Server Hub.
    Mantiene e valida lo stato informativo di ciascun ruolo in modo indipendente.
    """

    def __init__(self):
        # Mappa delle relazioni LoST: role -> schema_name -> transaction_ID -> { param_name: param_value }
        self.role_relations: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

    def get_role_relations(self, role: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Restituisce le relazioni relazionali associate al ruolo specificato."""
        return self.role_relations.setdefault(role, {})

    def has_known_parameter(self, role: str, param: str, ID: str) -> bool:
        """
        Verifica se il parametro indicato è già vincolato in una qualsiasi delle
        relazioni locali del ruolo specificato per la transazione identificata da ID.
        """
        tables = self.get_role_relations(role)
        for table in tables.values():
            if ID in table and param in table[ID]:
                return True
        return False

    def get_known_parameter(self, role: str, param: str, ID: str) -> Any:
        """
        Restituisce il valore del parametro se già vincolato in una qualsiasi delle
        relazioni locali del ruolo specificato per l'ID indicato, altrimenti None.
        """
        tables = self.get_role_relations(role)
        for table in tables.values():
            if ID in table and param in table[ID]:
                return table[ID][param]
        return None

    def check_viability(
        self,
        role: str,
        ID: str,
        in_params: Optional[List[str]] = None,
        out_params: Optional[List[str]] = None,
        nil_params: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Verifica le condizioni formali di viabilità LoST/BSPL per l'emissione di un messaggio da parte di un ruolo:
        1. Tutti i parametri in_params (se presenti nello schema) devono risultare già vincolati
           nelle relazioni locali del ruolo mittente per ID.
        2. Nessun parametro out_params (se presente nello schema) deve risultare già vincolato
           per la chiave ID nello stato locale del ruolo mittente (assioma di immutabilità).
        3. Nessun parametro nil_params (se presente nello schema) deve risultare già vincolato
           per la chiave ID nello stato locale del ruolo mittente.

        Ritorna il dizionario dei parametri [in] risolti dallo stato locale del ruolo: { param_name: param_value }.
        """
        resolved_in_params: Dict[str, Any] = {}

        # 1. Verifica e recupero parametri [in] dallo stato locale del ruolo mittente
        for param in in_params or []:
            if not self.has_known_parameter(role, param, ID):
                raise BSPLViabilityError(
                    f"[{role}] Emissione non viabile: parametro [in] '{param}' non ancora noto per ID='{ID}'"
                )
            resolved_in_params[param] = self.get_known_parameter(role, param, ID)

        # 2. Verifica che nessun parametro [out] sia già vincolato per il ruolo mittente
        for param in out_params or []:
            if self.has_known_parameter(role, param, ID):
                known = self.get_known_parameter(role, param, ID)
                raise BSPLViabilityError(
                    f"[{role}] Emissione non viabile: parametro [out] '{param}' già vincolato "
                    f"(valore={known!r}) per ID='{ID}'"
                )

        # 3. Verifica che nessun parametro [nil] sia già vincolato per il ruolo mittente
        for param in nil_params or []:
            if self.has_known_parameter(role, param, ID):
                known = self.get_known_parameter(role, param, ID)
                raise BSPLViabilityError(
                    f"[{role}] Emissione non viabile: parametro [nil] '{param}' già vincolato "
                    f"(valore={known!r}) per ID='{ID}'"
                )

        return resolved_in_params

    def is_duplicate(self, role: str, schema: str, ID: str, params: Dict[str, Any]) -> bool:
        """
        Verifica se la tupla del messaggio è un duplicato idempotente già registrato in R(schema) per il ruolo.
        Se params contiene un sottoinsieme di parametri (es. i soli parametri [out]), verifica che per
        quell'ID tutti i valori indicati coincidano esattamente con quelli già memorizzati.
        """
        tables = self.get_role_relations(role)
        table = tables.get(schema, {})
        if ID not in table:
            return False
        return all(table[ID].get(k) == v for k, v in params.items())

    def insert_relation(self, role: str, schema: str, ID: str, params: Dict[str, Any]):
        """
        Inserisce la tupla convalidata all'interno della relazione locale R(schema) del ruolo indicato.
        """
        tables = self.get_role_relations(role)
        if schema not in tables:
            tables[schema] = {}
        tables[schema][ID] = dict(params)

    def remove_relation(self, role: str, schema: str, ID: str):
        """
        Rimuove una tupla da R(schema) per il ruolo indicato in caso di errore (rollback).
        """
        tables = self.get_role_relations(role)
        if schema in tables and ID in tables[schema]:
            del tables[schema][ID]

    def get_history(self, role: str, ID: str) -> Dict[str, Dict[str, Any]]:
        """
        Restituisce la storia locale H_x (le tuple delle relazioni locali popolate)
        per il ruolo specificato e per una data transazione ID.
        """
        tables = self.get_role_relations(role)
        return {
            schema: dict(table[ID])
            for schema, table in tables.items()
            if ID in table
        }

    def get_history_vector(self, ID: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Restituisce l'History Vector distribuito H = [H_x1, ..., H_xn] per l'ID specificato.
        """
        from config import ROLES
        return {
            role: self.get_history(role, ID)
            for role in ROLES
        }
