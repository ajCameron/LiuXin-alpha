
"""
Container for book type works.

We needed a simplified metadata container to make the interfaces easier.
"""

from copy import deepcopy

from LiuXin_alpha.metadata.constants import METADATA_NULL_VALUES

from LiuXin_alpha.metadata.api import BookMetadataContainerAPI


class BookMetadata(BookMetadataContainerAPI):
    """
    Container for book type works metadata.

    It's supposed to look like info for a book - just with a cleaner interface than the calibre version.
    """
    def __init__(self, title: str) -> None:
        """
        Container for all the metadata of the book.

        :param title:
        """
        super().__init__(title=title)

        _data = deepcopy(METADATA_NULL_VALUES)

        # The __setattr__ and __getattr__ methods will be overridden - thus sorting the data somewhere else
        object.__setattr__(self, "_data", _data)

        # Needed to that open files can be made safe
        object.__setattr__(self, "_files_for_cleanup", [])




