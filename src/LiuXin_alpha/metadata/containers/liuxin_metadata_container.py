
"""
One container to rule them all.

Contains full info as to all metadata for a given work on the database.
"""
from LiuXin_alpha.databases.api import RowAPI, DatabaseAPI


class LiuXinMetadataContainer:
    """
    Contains all the info for a single work.
    """

    _work_row: RowAPI

    _expressions: list[RowAPI]

    def __init__(self, db: DatabaseAPI, work_row: RowAPI) -> None:
        """
        Init the class with the work row using the database.

        :param work_row:
        """

        self._work_row = work_row



    @property
    def work_row(self) -> RowAPI:
        return self._work_row

    # Insert work property properties here



