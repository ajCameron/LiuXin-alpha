
"""
Means for the database to notify us when things happen.
"""

# Todo: Rip this out (to the extent it's in) and replace with an event bus

from __future__ import annotations

from typing import Iterable, Optional, Any


def dummy_notify(event: str, ids: Iterable[int], cc_class: Optional[Any]) -> None:
    """
    Dummy for the notify class

    :param cc_class:
    :param event:
    :param ids:
    :return:
    """
    if cc_class.embed:
        raise NotImplementedError("This method should not be called when the class is embedded")
    else:
        pass

# Todo: We also need a ways of noting upodates on arbitary tables
# Todo: We need rules as to when to regenerate metadata and when not to


# Todo: This needs to be replaced with a full system of some sort
def dummy_dirtied(book_ids, commit, cc_class):
    """
    Dummy for the dirtied class - the original notes that this object has changed in the dirtied table of the database.

    :param cc_class:
    :param book_ids:
    :param commit:
    :return:
    """
    if cc_class.embed:
        raise NotImplementedError("This method should not be called when the class is embedded")
    else:
        pass
