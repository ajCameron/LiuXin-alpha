
"""
Expose the supported event logs compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/utils/logging/test_compat_logger.py
"""
from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog
from LiuXin_alpha.utils.logging.event_logs.logging_handler import EventLogHandler

DefaultEventLog = InMemoryEventLog

__all__ = ["DefaultEventLog", "EventLogHandler", "InMemoryEventLog"]
