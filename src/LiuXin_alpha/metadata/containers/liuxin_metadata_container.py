
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

        self._expressions = []

    @property
    def work_row(self) -> RowAPI:
        return self._work_row

    # Insert work property properties here

    @property
    def expressions(self) -> list[RowAPI]:
        """
        A list of all the expressions associated with the work row.

        :return:
        """
        return self._expressions

    def add_expression(self, expression: RowAPI) -> None:
        """
        Add an expression to the work row.

        :param expression:
        :return:
        """
        assert expression is not None and isinstance(expression, RowAPI) and expression not in self._expressions

        self._expressions.append(expression)

    def remove_expression(self, expression: RowAPI) -> None:
        """
        Remove an expression from the store.

        :return:
        """
        # Todo: Remove the expression from the expressions list without changing the order



