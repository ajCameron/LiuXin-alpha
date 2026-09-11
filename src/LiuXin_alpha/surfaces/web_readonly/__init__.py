"""
Expose the generic read-only WSGI host, configuration, and shared CLI helpers.

Other web surfaces reuse these metadata-selection and composition entrypoints.
Importing the package loads definitions without starting Core or binding a
listener. Read-only browsing does not imply authentication or sanitization of
HTML/SVG file previews; deployment controls remain the caller's responsibility.
"""

from __future__ import annotations

from .app import (
    ReadOnlyWebApplication,
    ReadOnlyWebConfig,
    add_metadata_read_source_arguments,
    build_arg_parser,
    build_metadata_read_source,
    main,
    metadata_read_source_config_kwargs,
    metadata_read_source_help_epilog,
)

__all__ = [
    "ReadOnlyWebApplication",
    "ReadOnlyWebConfig",
    "add_metadata_read_source_arguments",
    "build_arg_parser",
    "build_metadata_read_source",
    "main",
    "metadata_read_source_config_kwargs",
    "metadata_read_source_help_epilog",
]
