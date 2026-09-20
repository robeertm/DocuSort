"""Local-AI bridge: lets a Mac (or any other host) run the heavy LLM
inference and stream answers back to a DocuSort server that wouldn't
be able to do the work itself.

The Mac client opens an outbound WebSocket to the server, so the only
thing the user has to do is start a script on their Mac. No port
forwarding, no Tailscale ACLs, no firewall rules — that's the whole
point. See ``server.py`` for the in-memory hub and ``mac_client.py``
(top-level under scripts/) for the client side.
"""

from .server import BridgeServer, get_bridge, get_or_create_token

__all__ = ["BridgeServer", "get_bridge", "get_or_create_token"]
