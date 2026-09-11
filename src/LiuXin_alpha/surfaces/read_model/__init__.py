"""
Re-export the shared catalogue read-model backend and its structural surface-host protocol.

Importing this package makes the adapters available without constructing a host
or issuing Core queries. The backend borrows its host/client and optional image
backend; it does not start, shut down, or own a catalogue runtime.
"""

from __future__ import annotations

from .api import ReadModelBackend, ReadModelHostApi

__all__ = ["ReadModelBackend", "ReadModelHostApi"]
