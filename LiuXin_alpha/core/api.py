
"""
API for the core class.

If you are implementing a new core it should respect the API published here.
"""

import abc


class CoreAPI(abc.ABC):
    """
    API class for the core - every core should descend from this.
    """
    @property
    @abc.abstractmethod
    def core_uuid(self) -> str:
        """
        Return the UUID for this core.

        :return:
        """

    @property
    @abc.abstractmethod
    def core_version(self) -> str:
        """
        Return the UUID for this core.

        :return:
        """

    @abc.abstractmethod
    def shutdown(self) -> int:
        """
        Preforms a shutdown and issues a shutdown code.

        :return:
        """

    