"""
Re-export the Calibre-compatible acquisition adapter and its structural HTTP host protocol.

Importing this package performs no acquisition query or payload read. The adapter
borrows the host's Core client and response factories; it owns no server lifecycle.
"""

from __future__ import annotations

from .api import AcquisitionCompatApi, AcquisitionHostApi

__all__ = ["AcquisitionCompatApi", "AcquisitionHostApi"]
