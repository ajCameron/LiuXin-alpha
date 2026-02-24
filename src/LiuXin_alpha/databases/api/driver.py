"""Low-level database driver API contracts."""

from __future__ import annotations

import abc

from .macros import MacrosAPI

class DatabaseDriverAPI(abc.ABC):
    """
    Every database drive must descend from this class.
    """

    def direct_executescript(self, script: str) -> None:
        """
        Execute a script on the database - should be phased out.

        :param script:
        :return:
        """

    def direct_execute(self, script: str) -> None:
        """
        Execute a script on the database - should be phased out.

        :param script:
        :return:
        """

    @property
    @abc.abstractmethod
    def macros(self) -> MacrosAPI:
        """
        Return the macros for the given driver.

        :return:
        """
