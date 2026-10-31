
"""
Maintain the fixed rating scale expected by the database facade.

The rating helper writes missing or incorrect entries; it is normally invoked during
bootstrap when repair_bootstrap_rows is enabled.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI




class DatabaseRatingMixin:
    """
    Repair rating IDs 1 through 11 to represent half-step values from zero to five.

    Example:
        During writable bootstrap, db.check_rating_table() ensures the standard eleven
        rating entries exist.
    """

    # Todo: This might also want to be an internal method
    def check_rating_table(self: "DatabaseAPI") -> None:
        """
        Insert missing standard ratings and synchronize incorrect numeric values.

        ID i stores (i - 1) / 2. Macro writes keep explicit IDs and backend identity
        allocation consistent. All repairs share a transaction, including when
        called inside an existing unit. Nonnumeric existing values can raise
        during conversion and roll back earlier repairs.

        Example:
            For an open writable database, db.check_rating_table() restores rating ID 1
            to 0.0 and ID 11 to 5.0.


        :return: None; ratings outside IDs 1 through 11 are left untouched.
        :raises ValueError: An existing rating cannot be converted to float.
        :raises TypeError: An existing rating value does not support float conversion.
        """
        with self.macros.transaction():
            for rating_id in range(1, 12):
                rating = (rating_id - 1) / 2.0
                rating_row = self.macros.get_row("ratings", rating_id)
                if rating_row is None:
                    self.macros.insert_row(
                        "ratings", {"rating_id": rating_id, "rating": rating}
                    )
                elif float(rating_row["rating"]) != rating:
                    err_str = "Rating row malformed - correcting"
                    default_log.log_variables(
                        err_str,
                        "INFO",
                        ("rating", rating),
                        ('rating_row["rating"]', str(rating_row["rating"])),
                    )
                    self.macros.update_row("ratings", rating_id, {"rating": rating})
