"""Storage CLI constants ownership."""

from __future__ import annotations


class CLIUsageError(ValueError):
    """An actionable command configuration error."""


EXIT_OK = 0


EXIT_ISSUES = 1


EXIT_USAGE = 2


EXIT_INTERRUPTED = 130


EXIT_TERMINATED = 143


_GIB = 1024**3
