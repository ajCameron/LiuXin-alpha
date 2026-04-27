
"""
Tools to read and return metadata from the database.

Sometimes called a "hydrator".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import abc

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class WorkMetadataSourceAPI(abc.ABC):
    """
    Source to get work metadata from the database.
    """
    db: "DatabaseAPI"

    def has_id(self, target_id: str) -> bool:
        """
        Check to see if the given id exists in the database.

        :param target_id:
        :return:
        """




class DBMetadataSourceAPI(abc.ABC):
    """
    Single source API for pulling metadata from the database.
    """
    works: "WorkMetadataSourceAPI"












