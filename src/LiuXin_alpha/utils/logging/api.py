
"""
I don't know how this is actually going to work, so just defining an interface for the moment.
"""


from typing import Optional, Iterable

import abc


class EventLogAPI(abc.ABC):
    """
    Common interface for the event log class.
    """
    @abc.abstractmethod
    def put(self, message: str) -> None:
        """
        Write a message out to the event log.

        :param message:
        :return:
        """

    @abc.abstractmethod
    def get(self, num: Optional[int] = None) -> Iterable[str]:
        """
        Return the last n messages - more advanced filtering to follow.

        :param num:
        :return:
        """

