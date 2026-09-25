
"""
Specify discovery and removal of persistent database triggers.

Shared SQLite operations inspect sqlite_master, exclude TEMP triggers and drop named
triggers with individual commits. A later failure need not undo earlier removals.
"""

import abc

from typing import Iterable, Any


class DriverTriggersMixinAPI(abc.ABC):
    """
    Specify discovery and removal of persistent database triggers.

    Shared SQLite operations inspect sqlite_master, exclude TEMP triggers and drop named
    triggers with individual commits. A later failure need not undo earlier removals.
    Abstract members must be implemented by a backend; their empty bodies return None
    when called directly.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DriverTriggersMixinAPI)
        True
    """

    @abc.abstractmethod
    def direct_drop_triggers(self, triggers: Iterable[str]) -> bool:
        """
        Drop each named trigger and commit after each removal.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        Names become SQL syntax and must be trusted. A missing trigger raises
        OperationalError; earlier removals stay committed. Close on success or that error.

        Example:
            >>> driver.direct_drop_triggers(["sample_audit"])  # doctest: +SKIP


        :param triggers: Iterable of trusted trigger identifiers inserted into DROP TRIGGER
            statements.
        :return: ``True`` after all removals, including an empty input.
        """


    @abc.abstractmethod
    def direct_get_triggers(self) -> list[str]:
        """
        Read trigger names from sqlite_master and close on success or OperationalError.

        Abstract backend hook. The behavior below describes the shared SQL/SQLite
        implementation; this declaration itself has no operational body.

        TEMP triggers are not included and no ordering is specified.

        Example:
            >>> driver.direct_get_triggers()  # doctest: +SKIP


        :return: A list of trigger names.
        """
