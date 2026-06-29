
"""
View methods for the driver wrapper.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.row_api import ViewRowAPI


# Todo: Need to actually type this mess
class DriverWrapperViewMixin:
    """
    View methods for the driver wrapper.
    """

    def get_views(self, force_refresh: bool = False):
        """
        Return the known view names.

        Some drivers expose tables+views from `get_tables()`.
        Filter by `get_relation_type()` here so higher-level schema introspection can ask
        for views explicitly without depending on backend-specific list APIs.

        :param force_refresh: Passed to get_tables
        :return:
        """
        names = self.get_tables(force_refresh=force_refresh)
        return [
            name
            for name in names
            if self.get_relation_type(name) == "view"
        ]

    def is_view(self, name: str) -> bool:
        """
        Return True iff `name` exists and is a SQLite view.

        :param name:
        :return:
        """
        return self.get_relation_type(name) == "view"

    def get_view_column_headings(self, view: str) -> list[str]:
        """
        Gets the column headings for a table in the database.

        :param view:
        :return column_headings: An index of column headings in the order they appear on the database
        """
        return self.driver.direct_get_view_column_headings(view)

    # Todo: Need a method to get the name of all the views for a database
    def get_view_row_from_id(self, view: str, row_id: int) -> "ViewRowAPI":
        """
        Returns a row from a view of the database.

        :param view:
        :param row_id:
        :return:
        """
        return self.driver.direct_get_view_row_dict_from_id(view, row_id)
