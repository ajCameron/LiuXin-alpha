"""
Share storage-ingest exit categories, configuration errors, and binary GiB units.

Exit codes distinguish success (0), observed issues (1), usage/configuration (2),
SIGINT cancellation (130), and SIGTERM cancellation (143). They describe CLI
outcomes, not whether earlier filesystem or catalogue effects were rolled back.
"""

from __future__ import annotations


class CLIUsageError(ValueError):
    """
    Mark an actionable storage-ingest configuration or control-path refusal.

    Inherit ordinary ValueError construction and text behavior; classification
    and reporting belong to the ingest command boundary.

    Example:
        >>> error = CLIUsageError("report already exists")
        >>> isinstance(error, ValueError), str(error)
        (True, 'report already exists')
    """


EXIT_OK = 0


EXIT_ISSUES = 1


EXIT_USAGE = 2


EXIT_INTERRUPTED = 130


EXIT_TERMINATED = 143


_GIB = 1024**3
