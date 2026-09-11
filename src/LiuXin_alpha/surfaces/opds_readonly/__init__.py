"""
Export the standalone OPDS application, frozen configuration, parser, and runner.

Importing this package loads its shared web/OPDS dependencies but does not open a
catalogue or bind a socket. Call main, or run the package with python -m, to open
a configured Core session and serve the OPDS/acquisition routes.
"""

from __future__ import annotations

from .app import OpdsReadOnlyApplication, OpdsReadOnlyConfig, build_arg_parser, main

__all__ = [
    "OpdsReadOnlyApplication",
    "OpdsReadOnlyConfig",
    "build_arg_parser",
    "main",
]
