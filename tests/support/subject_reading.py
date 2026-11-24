"""
Choose how tests read a document's Subject field, through the ``read_subject_fields_as`` preference.

Example:
    Read the Subject as calibre does for one block::

        with subject_reading("calibre"):
            ...
"""

from __future__ import annotations

from collections import ChainMap
from collections.abc import Iterator
from contextlib import contextmanager

from LiuXin_alpha.configuration import current_preferences, use_preferences
from LiuXin_alpha.metadata.file_sources.subject_fields import SUBJECT_FIELDS_PREFERENCE


@contextmanager
def subject_reading(reading: str) -> Iterator[None]:
    """
    Set the ``read_subject_fields_as`` preference for a block, keeping every other preference.

    Example:
        >>> from LiuXin_alpha.metadata.file_sources.subject_fields import subject_field_reading
        >>> with subject_reading("calibre"):
        ...     subject_field_reading()
        'calibre'


    :param reading: ``subjects`` or ``calibre``.
    :return: An iterator that yields once, inside the block.
    """
    with use_preferences(
        ChainMap({SUBJECT_FIELDS_PREFERENCE: reading}, current_preferences())
    ):
        yield
