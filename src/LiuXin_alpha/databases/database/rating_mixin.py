
"""
Methods to handle interacting with
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
    Mixin class which provides specialized methods for dealing with the ratings table.
    """

    # Todo: This might also want to be an internal method
    def check_rating_table(self: "DatabaseAPI") -> None:
        """
        Checks that there is a valid ratings table.

        It should have 11 entries - each should be an integer from 0-10. Check that these exist. Do nothing if they do,
        error should if they do, but not in the expected form and insert them if they do not.
        :return:
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
