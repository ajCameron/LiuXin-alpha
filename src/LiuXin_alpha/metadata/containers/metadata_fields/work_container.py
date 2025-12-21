
"""
Container for information from the Works table.

Works are at the top of the tree - everything descends from them.
"""

from typing import Optional, Iterable



class WorkContainer:
    """
    Container for information from the Works table.

    This is distinct from the WorkMetadata - which contains all the information for any given works.
    This just holds the Work row itself, and some information about it.
    """
    _work_id = None
    _work_name = None

    def __init__(self, word_id: Optional[int] = None, work_name: Optional[str] = None) -> None:
        """
        Initialize the WorkContainer.

        :param word_id:
        """
        self._work_id = word_id

        self._work_name = work_name

    @property
    def work_id(self) -> Optional[int]:
        """
        Get the work id.

        :return:
        """
        return self._work_id

    @work_id.setter
    def work_id(self, work_id: Optional[int]) -> None:
        """
        Attempt to set the work id.

        :param work_id:
        :return:
        """
        if self._work_id is None:
            self._work_id = work_id
        else:
            raise AttributeError("Work id is already set.")


class WorksContainer:
    """
    Contains a number of works.
    """
    _works: list[WorkContainer] = []

    def __init__(self, works: Iterable[WorkContainer]) -> None:
        """
        Initialize the WorksContainer.

        :param works:
        """
        self._works = [wc for wc in works]



