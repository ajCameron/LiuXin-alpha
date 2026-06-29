from __future__ import annotations

import abc
from typing import Any


class DatabaseTriggerHelpersAPI(abc.ABC):
    """
    Typed API for trigger helper passthroughs exposed by ``Database``.

    Helper to deal with triggers.
    """

    @abc.abstractmethod
    def get_triggers(self) -> list[str]:
        """
        Return all the triggers on the database.

        :return:
        """

    @abc.abstractmethod
    def drop_triggers(self, triggers: list[str]) -> bool:
        """
        Drop the given triggers from the database.

        :param triggers:
        :return:
        """

    @abc.abstractmethod
    def drop_all_triggers(self) -> Any:
        """
        Drop all triggers from the database.

        :return:
        """
