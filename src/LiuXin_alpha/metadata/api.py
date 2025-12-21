
"""
API for the metadata classes.

These classes include all the metadata containers for all types of metadata
These include
 - document metadata
 - file metadata

It also includes the API for the plugins used to read and write metadata from files.
"""

import abc
import dataclasses

from typing import Optional


class MetadataContainerAPI(abc.ABC):
    """
    Fundamental API for the metadata containers.

    Probably SO fundamental that there's no actual stuff that can be hung on it.
    But it seems neater to have something at the root of the class hierarchy.
    """


class BookMetadataContainerAPI(MetadataContainerAPI):
    """
    Complete metadata for a "book" shaped object.

    Contains all the metadata for a book in various types of container.
    """
    def __init__(self, title: str) -> None:
        """
        Container for all the metadata of the book.

        :param title:
        """
        self.title = title


class FileAddStatusAPI:
    """
    Status of trying to add a file to a store.

    The status should depend on the store itself.
    """
    status: bool                        # - True if we did add, False otherwise
    error_str: Optional[str] = None     # - If something has gone wrong


class WorkContainerAPI:
    """
    API for the work containers.
    """




@dataclasses.dataclass
class FileMetadata:
    """
    Metadata for a single file.

    Contains information such as
     - store id
     - file size
     - file hash
     - file url
    """


@dataclasses.dataclass
class FileLineage:
    """
    Files can be derived from each other, so each file can have a lineage.

    (often this lineage will be one file deep).
    """




@dataclasses.dataclass
class FilesMetadata:
    """
    Metadata for a collection of files - usually a collection of file lineages.
    """




@dataclasses.dataclass
class LibraryMetadata:
    """
    Combined book and file metadata
    """




