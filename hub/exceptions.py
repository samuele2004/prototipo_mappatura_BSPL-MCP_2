"""
Definizione delle eccezioni per i vincoli del protocollo BSPL nell'Hub centrale.

Tutte le eccezioni derivano da ToolError dell'SDK di MCP per consentire al Server MCP
di restituire automaticamente al client chiamante una risposta JSON-RPC con isError=True
e il messaggio di errore descrittivo nel payload.
"""

try:
    from mcp.server.mcpserver.exceptions import ToolError
except ImportError:
    class ToolError(Exception):  # type: ignore[no-redef]
        pass


class BSPLProtocolError(ToolError):
    """Classe base per le eccezioni relative ai vincoli BSPL."""
    pass


class BSPLViabilityError(BSPLProtocolError):
    """Sollevata quando l'emissione di un messaggio viola le regole formali di viabilità BSPL/LoST."""
    pass


class BSPLExecutionError(BSPLProtocolError):
    """Sollevata quando una chiamata a un Tool MCP restituisce un errore."""
    pass
