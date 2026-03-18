from __future__ import annotations

import abc

from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterable

# Todo: I suspect this is used EVERYWHERE. So let's try and dry out the code base.
class DriverViewMixinAPI(abc.ABC):
    """
    View manipulation methods for the drivers.
    """

    @abc.abstractmethod
    def direct_get_view_column_headings(self, view: str) -> list[str]:
        """
        Get the column headings for the given view.

        :param view:
        :return:
        """

    @abc.abstractmethod
    def direct_get_view_row_dict_from_id(self, view: str, row_id: int) -> dict[str, Any]:
        """
        Get a view row dict for the given id and view.

        :param view:
        :param row_id:
        :return:
        """

