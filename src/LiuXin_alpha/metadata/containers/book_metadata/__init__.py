
"""
Provide the minimal book container with a title and independent metadata defaults.

This class initializes storage and a cleanup list; it does not implement the richer
field dispatch or cleanup methods of the Calibre-like container.

Example:
    >>> book = BookMetadata('Example')
    >>> book.title, book._data['title']
    ('Example', '')
    >>> other = BookMetadata('Other')
    >>> book._data['tags'] is other._data['tags']
    False
"""

from copy import deepcopy

from LiuXin_alpha.metadata.constants import METADATA_NULL_VALUES


class BookMetadata:
    """
    Store a title attribute alongside a separate dictionary of metadata defaults.

    The title argument is not copied into _data. Each instance receives its own
    deep-copied defaults and empty _files_for_cleanup list.

    Example:
        >>> book = BookMetadata('Example')
        >>> book.title, book._data['title']
        ('Example', '')
        >>> other = BookMetadata('Other')
        >>> book._data['tags'] is other._data['tags']
        False
    """
    def __init__(self, title: str) -> None:
        """
        Set the title directly and create independent metadata and cleanup storage.

        Example:
            >>> book = BookMetadata('Example')
            >>> book.title, book._data['title']
            ('Example', '')
            >>> other = BookMetadata('Other')
            >>> book._data['tags'] is other._data['tags']
            False


        :param title: Initial title assigned to the ordinary title attribute without
            normalization.
        :return: None.
        """
        self.title = title

        _data = deepcopy(METADATA_NULL_VALUES)

        # The __setattr__ and __getattr__ methods will be overridden - thus sorting the data somewhere else
        object.__setattr__(self, "_data", _data)

        # Needed to that open files can be made safe
        object.__setattr__(self, "_files_for_cleanup", [])



