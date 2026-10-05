
"""
Maintain the fixed rating scale expected by the database facade.

The rating helper writes missing or incorrect entries; it is normally invoked during bootstrap when repair_bootstrap_rows is enabled.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.utils.logging import default_log

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI




class DatabaseRatingMixin:
    """
    Repair rating IDs 1 through 11 to represent half-step values from zero to five.

    Example:
        During writable bootstrap, db.check_rating_table() ensures the standard eleven rating entries exist.
    """

    # Todo: This might also want to be an internal method
    def check_rating_table(self: "DatabaseAPI") -> None:
        """
        Insert missing standard ratings and synchronize incorrect numeric values.

        ID i stores (i - 1) / 2. Missing rows are inserted through the wrapper using string IDs/values; existing values are compared as floats and repaired through Row.sync. Nonnumeric existing values can raise during conversion. Earlier repairs are not rolled back as a group.

        Example:
            For an open writable database, db.check_rating_table() restores rating ID 1 to 0.0 and ID 11 to 5.0.


        :return: None; ratings outside IDs 1 through 11 are left untouched.
        :raises ValueError: An existing rating cannot be converted to float.
        :raises TypeError: An existing rating value does not support float conversion.
        """
        for i in range(1, 12):
            rating = six_unicode(i - 1)
            rating_id = six_unicode(i)
            rating_row = self.get_row_from_id("ratings", rating_id)

            if rating_row is None:
                new_row_dict = {
                    "rating_id": rating_id,
                    "rating": six_unicode(float(rating) / 2.0),
                }
                self.driver_wrapper.add_row(new_row_dict)

            else:

                if float(rating_row["rating"]) != float(rating) / 2.0:
                    err_str = "Rating row malformed - correcting"
                    default_log.log_variables(
                        err_str,
                        "INFO",
                        ("rating", rating),
                        ('rating_row["rating"]', six_unicode(rating_row["rating"])),
                    )
                    rating_row["rating"] = float(rating) / 2.0
                    rating_row.sync()

        # rating_11_row = self.get_row_from_id("ratings", 11)
        # if rating_11_row is not None:
        #     self.delete(rating_11_row)
