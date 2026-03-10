"""Transport adapters for hosting/accessing `CoreRuntime`."""

from __future__ import annotations

from .http import CoreHttpDaemon

__all__ = ["CoreHttpDaemon"]
