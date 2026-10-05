"""
Re-export the shared OPDS feed/router adapter and its structural host protocol.

Token, grouping, pagination, and XML construction helpers live in the api module.
Package import does not construct a host, query a catalogue, or start an HTTP server.
"""

from __future__ import annotations

from .api import OpdsApi, OpdsHostApi

__all__ = ["OpdsApi", "OpdsHostApi"]
