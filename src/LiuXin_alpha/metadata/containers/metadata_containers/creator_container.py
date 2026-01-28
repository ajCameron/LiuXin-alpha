
"""
Creator container - contains creator information.

The main metadata is split down into different container category types.
This is the creators one.
"""

from typing import Optional

from LiuXin_alpha.databases.api import RowAPI, DatabaseAPI


class CreatorContainer:
    """
    Contains all the creator information for an object.
    """

    title_row: Optional[RowAPI]
    series_row: Optional[RowAPI]

    def __init__(self, db: DatabaseAPI, title_row: Optional[RowAPI] = None, series_row: Optional[RowAPI] = None) -> None:
        """
        Populate a creator container from the database.

        :param title_row:
        :param series_row:
        """
        self.db = db

        self.title_row = title_row
        self.series_row = series_row











