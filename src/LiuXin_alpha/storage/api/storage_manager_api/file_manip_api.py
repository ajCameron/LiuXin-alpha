
"""
The StorageManager files front end.

One of the purposes of the StorageManager is that all file storage, manipulation and retrieval happens internally.
"""

from __future__ import annotations

import abc


class StorageManagerFilesAPI(abc.ABC):
    """
    The part of the storage manager API concerning with CRUD operations on files themselves.
    """