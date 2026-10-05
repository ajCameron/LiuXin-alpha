"""
Export the Calibre-style WSGI application, configuration, parser, and server runner.

Importing this package loads shared web/catalogue/protocol dependencies without
opening a catalogue or binding a socket. The application offers compatibility
routes and a small HTML UI, not the complete upstream Calibre content server.
"""

from __future__ import annotations

from .app import CalibreReadOnlyWebApplication, CalibreReadOnlyWebConfig, build_arg_parser, main

__all__ = [
    "CalibreReadOnlyWebApplication",
    "CalibreReadOnlyWebConfig",
    "build_arg_parser",
    "main",
]
