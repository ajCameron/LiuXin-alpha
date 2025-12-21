
"""
Container for book type works.

We needed a simplified metadata container to make the interfaces easier.
"""

from LiuXin_alpha.metadata.api import BookMetadataContainerAPI


class BookMetadata(BookMetadataContainerAPI):
    """
    Container for book type works metadata.
    """
    def __init__(self, title: str) -> None:
        """
        Container for all the metadata of the book.

        :param title:
        """



