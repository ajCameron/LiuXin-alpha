"""
Declare DatabaseTriggerHelpersAPI operations for database facade implementations.

Repeated declarations are retained; the later definition supplies the runtime member. Abstract bodies perform no backend work. Concrete behavior and its limitations are described for callers without changing that implementation.
"""

from __future__ import annotations

import abc
from typing import Any


class DatabaseTriggerHelpersAPI(abc.ABC):
    """
    Specify trigger discovery and removal.

    Implement every abstract member before instantiating this interface. Backend resource and transaction policies remain the concrete implementation responsibility.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseTriggerHelpersAPI)
        True
    """

    @abc.abstractmethod
    def get_triggers(self) -> list[str]:
        """
        Retrieve trigger names through the wrapper and selected driver.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Retrieve trigger names through the wrapper and selected driver.

        Example:
            For an open db, names = db.get_triggers() discovers triggers before selective removal.


        :return: Backend trigger-name list.
        """

    @abc.abstractmethod
    def drop_triggers(self, triggers: list[str]) -> bool:
        """
        Delegate removal of the supplied trigger names.

        Abstract removal hook. The concrete facade delegates through the wrapper to the selected driver; the shared SQL driver commits each successful drop and returns True. Names are interpolated as identifiers; callers must supply trusted trigger names. A later failure does not undo earlier drops.

        Example:
            On a disposable database, db.drop_triggers(names) removes the selected names obtained from db.get_triggers().


        :param triggers: Trigger-name collection accepted by the backend.
        :return: Backend removal result; the shared SQL driver returns True after successful removal, including an empty name list.
        """

    @abc.abstractmethod
    def drop_all_triggers(self) -> Any:
        """
        Discover current trigger names and delegate their removal.

        Abstract removal hook. The concrete facade delegates through the wrapper to the selected driver; the shared SQL driver commits each successful drop and returns True. Names are interpolated as identifiers; callers must supply trusted trigger names. A later failure does not undo earlier drops.

        Example:
            On a disposable database, db.drop_all_triggers() removes every trigger discovered by the selected backend.


        :return: Backend removal result; the shared SQL driver returns True after successful removal, including an empty name list.
        """
