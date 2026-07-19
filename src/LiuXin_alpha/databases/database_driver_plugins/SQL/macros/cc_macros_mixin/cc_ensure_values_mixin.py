
"""
Macros to ensure values in custom column tables.
"""

from __future__ import annotations

import json

from typing import Any, TYPE_CHECKING

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from LiuXin_alpha.utils.language_tools import plural_singular_mapper

from LiuXin_alpha.errors import DatabaseDriverError

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI




class CustomColumnsEnsureValueMacrosMixin:
    """
    Mixin for management (creation, update and deletion) of custom columns themselves.
    """

    db: "DatabaseAPI"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN MACROS
    def ensure_custom_column_value(self, cc_table: str, value: Any) -> Any:
        """
        Add a value to a custom column - values are assumed to not already exist.

        :return:
        """
        # Todo: Hopefully? Making a value column method would be good here.
        cc_val_column = self.db.driver_wrapper.get_display_column(cc_table)
        cc_id_column = self.db.driver_wrapper.get_id_column(cc_table)

        # Todo: The custom columns table have a different structure - account for it
        try:
            insert_stmt = "INSERT INTO {0} ({1}) VALUES (?);".format(cc_table, cc_val_column)
            self.db.driver_wrapper.execute(insert_stmt, (value,))
        except DatabaseDriverError:
            pass

        search_stmt = "SELECT {0} FROM {1} WHERE {2} = ? ORDER BY {0};".format(cc_id_column, cc_table, cc_val_column)
        return self.db.driver_wrapper.get(search_stmt, (value,))[0][0]
