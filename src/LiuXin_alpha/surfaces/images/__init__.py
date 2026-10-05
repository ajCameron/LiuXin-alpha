"""
Re-export the shared Core-backed image backend and its structural host protocol.

Importing this package loads the backend implementation without constructing an
application, acquiring image bytes, or generating a placeholder. The backend
handles discovery, resolution, and SVG fallback, not general raster transformation.
"""

from __future__ import annotations

from .api import ImageBackend, ImageHostApi

__all__ = ["ImageBackend", "ImageHostApi"]
