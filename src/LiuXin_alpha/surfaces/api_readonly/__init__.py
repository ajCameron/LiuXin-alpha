"""
Expose the read-only JSON WSGI application, configuration, parser, and runner.

Package import defines these entrypoints without constructing Core or starting
a server. Catalogue projections and file delivery reuse the generic web host;
read-only routing does not supply authentication or preview-content sanitization.
"""

from __future__ import annotations

from .app import ApiReadOnlyApplication, ApiReadOnlyConfig, build_arg_parser, main

__all__ = ["ApiReadOnlyApplication", "ApiReadOnlyConfig", "build_arg_parser", "main"]
