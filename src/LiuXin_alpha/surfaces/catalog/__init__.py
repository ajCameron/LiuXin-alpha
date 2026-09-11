"""
Re-export the Calibre-shaped catalogue backend, its host protocol, and built-in fallback PNG bytes.

The backend composes shared read-model and image adapters; package import does
not construct an application, query catalogue rows, or acquire file content.
"""

from __future__ import annotations

from .api import CalibreCatalogBackend, CalibreCatalogHostApi, PLACEHOLDER_PNG

__all__ = ["CalibreCatalogBackend", "CalibreCatalogHostApi", "PLACEHOLDER_PNG"]
