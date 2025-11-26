
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


class BookMetadataAPI(abc.ABC):
    """
    Complete metadata for a book.
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




