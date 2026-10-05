"""
Expose the supported ipc compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/ipc/test_simple_worker.py
"""

from .simple_worker import WorkerError, fork_job

__all__ = ["WorkerError", "fork_job"]
