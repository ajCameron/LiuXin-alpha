"""
Expose CoreHttpDaemon as the current Core runtime hosting adapter.

Importing this package does not bind a listener or start a runtime. Direct/RPC
client interfaces are exported from the separate core.proxies package.
"""

from __future__ import annotations

from .http import CoreHttpDaemon

__all__ = ["CoreHttpDaemon"]
