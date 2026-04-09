
"""
API for the bits of the driver connected with CRUD triggers on the database.
"""

import abc

from typing import Iterable, Any


class DriverTriggersMixinAPI(abc.ABC):
    """
    Mixin methods to add to the database.
    """

    @abc.abstractmethod
    def direct_drop_triggers(self, triggers: Iterable[str]) -> None:
        """
        Drop triggers directly from the database.

        :param triggers:
        :return:
        """


    @abc.abstractmethod
    def direct_get_triggers(self) -> list[str]:
        """
        Directly get all the triggers off the database.

        :return:
        """

