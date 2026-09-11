"""
Expose the optional lifecycle-plugin API at the terminal plugin package boundary.

Importing this package makes the base class available without creating plugin
instances, registering hooks, or importing a concrete browser implementation.
"""

from __future__ import annotations

from .base import TerminalLifecyclePluginAPI

__all__ = ["TerminalLifecyclePluginAPI"]
