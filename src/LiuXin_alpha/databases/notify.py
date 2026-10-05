
"""
Provide inert notification hooks for non-embedded legacy custom-column objects.

CustomColumns installs these partials only when used outside its embedded host. They check cc_class.embed and otherwise discard the event/dirtiness arguments; they do not implement an event bus, dirtied-row persistence or transaction commits.
"""

# Todo: Rip this out (to the extent it's in) and replace with an event bus

from __future__ import annotations

from typing import Iterable, Optional, Any


def dummy_notify(event: str, ids: Iterable[int], cc_class: Optional[Any]) -> None:
    """
    Ignore a notification unless the custom-column object claims to be embedded.

    Example:
        >>> from types import SimpleNamespace
        >>> dummy_notify("updated", [1], SimpleNamespace(embed=False))


    :param event: Event label, ignored without validation.
    :param ids: Affected IDs, neither inspected nor consumed.
    :param cc_class: Object providing embed; None is unsupported despite the Optional annotation.
    :return: None when embed is false.
    :raises NotImplementedError: cc_class.embed is truthy and the real host hook should have been used.
    :raises AttributeError: cc_class does not provide embed, including when it is None.
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
    Ignore a dirtiness request for a non-embedded custom-column object.

    Example:
        >>> from types import SimpleNamespace
        >>> dummy_dirtied([1], True, SimpleNamespace(embed=False))


    :param book_ids: Affected book IDs, neither inspected nor consumed.
    :param commit: Compatibility commit flag, ignored; no transaction is committed.
    :param cc_class: Object providing the embed flag.
    :return: None when embed is false.
    :raises NotImplementedError: cc_class.embed is truthy.
    :raises AttributeError: cc_class does not provide embed.
    """
    if cc_class.embed:
        raise NotImplementedError("This method should not be called when the class is embedded")
    else:
        pass
