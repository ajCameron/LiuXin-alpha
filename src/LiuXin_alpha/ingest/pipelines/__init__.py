"""
Expose the remote-HTML catalogue-registration pipeline under its shared package name.

The exported function is the implementation object, not a lifecycle wrapper.
Importing this package does not crawl, open a database, or register any files.
"""

from .remote_html import ingest_html_discovery_store_files

__all__ = ["ingest_html_discovery_store_files"]
